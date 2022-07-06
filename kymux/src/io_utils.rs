use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::Result;

pub(crate) type SizeType = usize;
pub(crate) const SIZE_LEN: usize = std::mem::size_of::<SizeType>();

pub(crate) async fn read_buf<T>(rx: &mut T) -> Result<Vec<u8>>
where
    T: AsyncReadExt + Unpin,
{
    let mut raw_len: [u8; SIZE_LEN] = [0; SIZE_LEN];
    rx.read_exact(&mut raw_len).await?;

    let len = SizeType::from_be_bytes(raw_len);
    let mut buf: Vec<u8> = vec![0; len];
    rx.read_exact(&mut buf[..]).await?;

    Ok(buf)
}

pub(crate) async fn read_msg<M, T>(rx: &mut T) -> Result<M>
where
    M: DeserializeOwned,
    T: AsyncReadExt + Unpin,
{
    let buf = read_buf(rx).await?;
    Ok(rmp_serde::from_slice(&buf)?)
}

pub(crate) async fn write_buf<T>(tx: &mut T, e: Vec<u8>) -> Result<()>
where
    T: AsyncWriteExt + Unpin,
{
    let len: SizeType = e.len();

    tx.write_all(&len.to_be_bytes()).await?;
    tx.write_all(&e).await?;

    Ok(())
}

pub(crate) async fn write_msg<M, T>(tx: &mut T, m: M) -> Result<()>
where
    M: Serialize,
    T: AsyncWriteExt + Unpin,
{
    let buf = rmp_serde::to_vec(&m)?;
    write_buf(tx, buf).await
}
