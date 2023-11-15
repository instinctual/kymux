use crate::error::*;
use crate::{
    Connection, ConnectionDriver, RecvStream, RecvStreamDriver, SendStream, SendStreamDriver,
};

use std::cell::RefCell;
use std::num::ParseIntError;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

impl From<web_sys::WebTransport> for Connection {
    fn from(value: web_sys::WebTransport) -> Self {
        Self::new(WebTransportJSConnectionDriver::wrap(value))
    }
}

pub struct WebTransportJSOptions {
    pub congestion_control: WebTransportJSCongestionControl,
    pub require_unreliable: bool,
    pub server_certificate_hashes: Vec<WebTransportJSHash>,
}

impl WebTransportJSOptions {
    fn to_web_sys(&self) -> web_sys::WebTransportOptions {
        let mut options = web_sys::WebTransportOptions::new();
        options.require_unreliable(self.require_unreliable);
        options.congestion_control(self.congestion_control.to_web_sys());

        let hashes = js_sys::Array::new();
        for hash in &self.server_certificate_hashes {
            hashes.push(&hash.to_web_sys());
        }

        options.server_certificate_hashes(&hashes);

        options
    }
}

impl Default for WebTransportJSOptions {
    fn default() -> Self {
        Self {
            congestion_control: WebTransportJSCongestionControl::Default,
            require_unreliable: false,
            server_certificate_hashes: Vec::new(),
        }
    }
}

pub enum WebTransportJSCongestionControl {
    Default,
    Throughput,
    LowLatency,
}

impl WebTransportJSCongestionControl {
    fn to_web_sys(&self) -> web_sys::WebTransportCongestionControl {
        match &self {
            Self::Default => web_sys::WebTransportCongestionControl::Default,
            Self::Throughput => web_sys::WebTransportCongestionControl::Throughput,
            Self::LowLatency => web_sys::WebTransportCongestionControl::LowLatency,
        }
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
#[error("decode hex error")]
pub enum DecodeHexError {
    OddLength,
    ParseInt(#[from] ParseIntError),
}

pub struct WebTransportJSHash {
    algorithm: String,
    value: Vec<u8>,
}

impl WebTransportJSHash {
    pub fn new(algorithm: impl Into<String>, value: Vec<u8>) -> Self {
        Self {
            algorithm: algorithm.into(),
            value,
        }
    }

    pub fn new_from_hex(algorithm: impl Into<String>, hash: &str) -> Result<Self, DecodeHexError> {
        let hash = Self::hex_to_bytes(hash)?;
        let this = Self::new(algorithm, hash);
        Ok(this)
    }

    pub fn new_sha256_from_hex(hash: &str) -> Result<Self, DecodeHexError> {
        Self::new_from_hex("sha-256", hash)
    }

    fn hex_to_bytes(s: &str) -> Result<Vec<u8>, DecodeHexError> {
        if s.len() % 2 == 0 {
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.into()))
                .collect()
        } else {
            Err(DecodeHexError::OddLength)
        }
    }

    fn to_web_sys(&self) -> web_sys::WebTransportHash {
        let array = js_sys::Uint8Array::from(self.value.as_ref());

        let mut obj = web_sys::WebTransportHash::new();
        obj.algorithm(&self.algorithm);
        obj.value(&array);
        obj
    }
}

#[derive(Debug)]
pub(crate) struct WebTransportJSConnectionDriver {
    web_transport: web_sys::WebTransport,
    uni_streams_reader: RefCell<Option<web_sys::ReadableStreamDefaultReader>>,
    bi_streams_reader: RefCell<Option<web_sys::ReadableStreamDefaultReader>>,
    datagrams_reader: RefCell<Option<web_sys::ReadableStreamDefaultReader>>,
    datagrams_writer: RefCell<Option<web_sys::WritableStreamDefaultWriter>>,
}

impl WebTransportJSConnectionDriver {
    fn wrap(web_transport: web_sys::WebTransport) -> Self {
        Self {
            web_transport,
            uni_streams_reader: RefCell::new(None),
            bi_streams_reader: RefCell::new(None),
            datagrams_reader: RefCell::new(None),
            datagrams_writer: RefCell::new(None),
        }
    }

    pub async fn connect(
        url: &str,
        options: &WebTransportJSOptions,
    ) -> Result<Connection, ConnectionError> {
        let options = options.to_web_sys();
        let web_transport = web_sys::WebTransport::new_with_options(url, &options)?;
        JsFuture::from(web_transport.ready()).await?;
        Ok(web_transport.into())
    }
}

#[async_trait(?Send)]
impl ConnectionDriver for WebTransportJSConnectionDriver {
    async fn open_uni(&self) -> Result<SendStream, ConnectionError> {
        let promise = self.web_transport.create_unidirectional_stream();
        let send_stream = JsFuture::from(promise)
            .await?
            .dyn_into::<web_sys::WritableStream>()?;
        let send_driver = WebTransportJSSendStreamDriver::new(send_stream);
        let send = SendStream::new(send_driver);
        Ok(send)
    }

    async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let promise = self.web_transport.create_bidirectional_stream();
        let bi_stream = JsFuture::from(promise)
            .await?
            .dyn_into::<web_sys::WebTransportBidirectionalStream>()?;
        let send_driver = WebTransportJSSendStreamDriver::new(bi_stream.writable().into());
        let recv_driver = WebTransportJSRecvStreamDriver::new(bi_stream.readable().into());
        let send = SendStream::new(send_driver);
        let recv = RecvStream::new(recv_driver);
        Ok((send, recv))
    }

    async fn accept_uni(&self) -> Result<RecvStream, ConnectionError> {
        let mut reader = self.uni_streams_reader.borrow_mut();
        if reader.is_none() {
            let uni_streams_reader = self
                .web_transport
                .incoming_unidirectional_streams()
                .get_reader()
                .dyn_into::<web_sys::ReadableStreamDefaultReader>()
                .map_err(|err| ConnectionError(format!("Unexpected reader type {err:?}")))?;
            *reader = Some(uni_streams_reader);
        }

        let promise = reader.as_ref().expect("Not initialized").read();
        let obj = JsFuture::from(promise).await?;
        let done = js_sys::Reflect::get(&obj, &JsValue::from("done"))?
            .as_bool()
            .unwrap_or(false);
        if done {
            return Err(ConnectionError("No more unistreams".to_string()));
        }

        let recv_stream = js_sys::Reflect::get(&obj, &JsValue::from("value"))?
            .dyn_into::<web_sys::ReadableStream>()?;
        let recv_driver = WebTransportJSRecvStreamDriver::new(recv_stream);
        let recv = RecvStream::new(recv_driver);
        Ok(recv)
    }

    async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let mut reader = self.bi_streams_reader.borrow_mut();
        if reader.is_none() {
            let bi_streams_reader = self
                .web_transport
                .incoming_bidirectional_streams()
                .get_reader()
                .dyn_into::<web_sys::ReadableStreamDefaultReader>()
                .map_err(|err| ConnectionError(format!("Unexpected reader type {err:?}")))?;
            *reader = Some(bi_streams_reader);
        }

        let promise = reader.as_ref().expect("Not initialized").read();
        let obj = JsFuture::from(promise).await?;
        let done = js_sys::Reflect::get(&obj, &JsValue::from("done"))?
            .as_bool()
            .unwrap_or(false);
        if done {
            return Err(ConnectionError("No more bistreams".to_string()));
        }

        let bi_stream = js_sys::Reflect::get(&obj, &JsValue::from("value"))?
            .dyn_into::<web_sys::WebTransportBidirectionalStream>()?;
        let send_driver = WebTransportJSSendStreamDriver::new(bi_stream.writable().into());
        let recv_driver = WebTransportJSRecvStreamDriver::new(bi_stream.readable().into());
        let send = SendStream::new(send_driver);
        let recv = RecvStream::new(recv_driver);
        Ok((send, recv))
    }

    async fn read_datagram(&self) -> Result<Bytes, ConnectionError> {
        let mut reader = self.datagrams_reader.borrow_mut();
        if reader.is_none() {
            let datagrams_reader = self
                .web_transport
                .datagrams()
                .readable()
                .get_reader()
                .dyn_into::<web_sys::ReadableStreamDefaultReader>()
                .expect("Invalid readable stream");
            *reader = Some(datagrams_reader);
        }

        let promise = reader.as_ref().expect("Not initialized").read();
        let obj = JsFuture::from(promise).await?;
        let done = js_sys::Reflect::get(&obj, &JsValue::from("done"))?
            .as_bool()
            .unwrap_or(false);
        if done {
            return Err(ConnectionError("Could not read datagrams".to_string()));
        }

        let vec = js_sys::Reflect::get(&obj, &JsValue::from("value"))?
            .dyn_into::<js_sys::Uint8Array>()?
            .to_vec();
        Ok(vec.into())
    }

    async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError> {
        let mut writer = self.datagrams_writer.borrow_mut();
        if writer.is_none() {
            let datagrams_writer = self.web_transport.datagrams().writable().get_writer()?;
            *writer = Some(datagrams_writer);
        }

        let typed_array = js_sys::Uint8Array::from(&data[..]);
        let data_js_value = JsValue::from(typed_array);

        let promise = writer
            .as_ref()
            .expect("Not initialized")
            .write_with_chunk(&data_js_value);
        JsFuture::from(promise).await?;

        Ok(())
    }

    async fn closed(&self) -> Result<(), ConnectionError> {
        let promise = self.web_transport.closed();
        let _ = JsFuture::from(promise).await?;
        Ok(())
    }

    fn close(&self, error_code: u32, reason: &str) {
        self.web_transport.close_with_close_info(
            web_sys::WebTransportCloseInfo::new()
                .close_code(error_code)
                .reason(reason),
        );
    }

    fn max_datagram_size(&self) -> Option<usize> {
        let max = self.web_transport.datagrams().max_datagram_size();
        Some(max as usize)
    }
}

#[derive(Debug)]
struct WebTransportJSSendStreamDriver {
    writer: web_sys::WritableStreamDefaultWriter,
}

impl WebTransportJSSendStreamDriver {
    fn new(send: web_sys::WritableStream) -> Self {
        let writer = send.get_writer().expect("Invalid writable stream");
        Self { writer }
    }
}

#[async_trait(?Send)]
impl SendStreamDriver for WebTransportJSSendStreamDriver {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError> {
        // The JavaScript API necessarily writes the whole buffer
        self.write_all(buf).await?;
        Ok(buf.len())
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), WriteError> {
        let typed_array = js_sys::Uint8Array::from(buf);
        let data_js_value = JsValue::from(typed_array);
        let promise = self.writer.write_with_chunk(&data_js_value);
        JsFuture::from(promise).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), WriteError> {
        let promise = self.writer.close();
        JsFuture::from(promise).await?;
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<(), UnknownStreamError> {
        let promise = self.writer.abort();
        JsFuture::from(promise).await?;
        Ok(())
    }
}

#[derive(Debug)]
struct WebTransportJSRecvStreamDriver {
    reader: web_sys::ReadableStreamByobReader,
}

impl WebTransportJSRecvStreamDriver {
    fn new(recv: web_sys::ReadableStream) -> Self {
        let reader = recv
            .get_reader_with_options(
                web_sys::ReadableStreamGetReaderOptions::new()
                    .mode(web_sys::ReadableStreamReaderMode::Byob),
            )
            .dyn_into::<web_sys::ReadableStreamByobReader>()
            .expect("Invalid readable stream");
        Self { reader }
    }
}

#[async_trait(?Send)]
impl RecvStreamDriver for WebTransportJSRecvStreamDriver {
    async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError> {
        let typed_array = js_sys::Uint8Array::new(&JsValue::from(buf.len()));

        let promise = self.reader.read_with_array_buffer_view(&typed_array);
        let obj = JsFuture::from(promise).await?;
        let done = js_sys::Reflect::get(&obj, &JsValue::from("done"))?
            .as_bool()
            .unwrap_or(false);
        if done {
            return Ok(None); // EOS
        }

        let array = js_sys::Reflect::get(&obj, &JsValue::from("value"))?
            .dyn_into::<js_sys::Uint8Array>()
            .expect("Unexpected read value");

        let len = array.byte_length() as usize;
        array.copy_to(&mut buf[..len]);

        Ok(Some(len))
    }
}

impl From<JsValue> for ConnectionError {
    fn from(value: JsValue) -> Self {
        Self(format!("{value:?}"))
    }
}

impl From<JsValue> for ReadError {
    fn from(value: JsValue) -> Self {
        Self(format!("{value:?}"))
    }
}

impl From<JsValue> for ReadExactError {
    fn from(value: JsValue) -> Self {
        Self::ReadError(value.into())
    }
}

impl From<JsValue> for WriteError {
    fn from(value: JsValue) -> Self {
        Self(format!("{value:?}"))
    }
}

impl From<JsValue> for SendDatagramError {
    fn from(value: JsValue) -> Self {
        Self(format!("{value:?}"))
    }
}

impl From<JsValue> for UnknownStreamError {
    fn from(_: JsValue) -> Self {
        Self
    }
}

// For convenience, also provide the conversion from these errors to JsValue
// (containing the error message). They could not yield the original JsValue,
// which is not stored in the kyproto errors).
//
// This implementation could not be done by the client, since neither JsValue
// nor kyproto errors are defined in the client crate.

macro_rules! impl_from_jsvalue {
    ($t:ty) => {
        impl From<$t> for wasm_bindgen::JsValue {
            fn from(value: $t) -> Self {
                Self::from(&value.to_string())
            }
        }
    };
}

impl_from_jsvalue!(ConnectionError);
impl_from_jsvalue!(SendDatagramError);
impl_from_jsvalue!(ReadError);
impl_from_jsvalue!(ReadExactError);
impl_from_jsvalue!(WriteError);
impl_from_jsvalue!(UnknownStreamError);
