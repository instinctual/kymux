use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamOwner {
    Local,
    Peer,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum StreamType {
    Video,
    Audio,
    Input,
}

pub(crate) fn stream_id_to_u64(id: quinn::StreamId) -> u64 {
    let id: quinn::VarInt = id.into();
    id.into()
}
