use crate::protocol::Protocol;
use crate::router::KyChannel;
use crate::{EndpointDesc, Result, StreamOwner};

use async_trait::async_trait;
use tokio::net::TcpStream;

mod forwarder;
mod receiver;
mod sender;

/**
 * This protocol, tailored for video streaming, transmits config packets
 * (SPS/PPS) over a QUIC stream, and media data over QUIC datagrams.
 *
 * The implementation is composed of 3 parts:
 *  - the sender
 *  - the receiver
 *  - the forwarder
 *
 * The _sender_ receives kypackets from a client over TCP. It sends to the
 * kymux receiver the initial codec packet and all config packets (SPS/PPS)
 * over a single QUIC stream, in sequence. It generates RaptorQ packets from
 * media packets to add forward error correction, and send them over datagrams
 * having size max_datagram_size() (exposed by quinn) including some additional
 * headers.
 *
 * The _receiver_ listens for 3 events:
 *  - accept a QUIC uni-stream
 *  - receive kypackets over a previously accepted QUIC uni-stream
 *  - receive datagrams
 * and expose them in a single queue (MPSC) to be consumed by the forwarder.
 *
 * The _forwarder_ aims to re-assemble datagrams using a RaptorQ decoder and
 * reorder kypackets, and send them to the receiving client. Since datagrams
 * may be reordered or lost, it bufferizes when necessary to wait some time for
 * missing packets, but after some timeout it considers a packet as lost, and
 * continue sending the following ones.
 *
 *
 * ## Protocol and implementation details
 *
 * ### Sender
 *
 * The client (connected to Kymux) produces ky-packets and sends them over TCP
 * to Kymux. The specific format of ky-packets is not important here:
 * [`Packet::read()`] is used to parse them into [`Packet`]s.
 *
 * The Kymux sender receives 3 types of packets from the client:
 *  - `Packet::Codec(packet)`: a codec packet (it indicates the codec used), it
 *     is assumed that a codec packet is sent exactly once, as the first packet
 *     of the stream. No more codec packets should be sent.
 *  - `Packet::Media(packet)` if `packet.is_config`: a config packet (SPS/PPS).
 *  - `Packet::Media(packet)` if `!packet.is_config`: a media packet.
 *
 * It initially opens a single unidirectional QUIC stream to the Kymux
 * receiver. It sends the initial codec packet and config packets over this
 * QUIC stream in sequence, prefixed with a small header containing sequence
 * numbers (described later). On the wire, each packet has the following format:
 *
 *  - kypacket_seq: 32 bits
 *  - group_seq: 32 bits
 *  - kypacket: full kypacket as is
 *
 * The sequence numbers are set to 0 for codec packets, since they are
 * meaningless here.
 *
 * Media packets (where `is_config` is false) are used to generate RaptorQ
 * packets (source and repair packets, to add forward error correction) and
 * sent in QUIC datagrams:
 *
 * ```notrust
 *                    +-------------------------+ +----+ +------------------+
 *      video packets |            P0           | | P1 | |       P2         |
 *                    +-------------------------+ +----+ +------------------+
 *          | RaptorQ |                         | |    | |                  |
 *          v encoder v                         v v    v v                  v
 *                    +----+ +----+ +----+ +----+ +----+ +----+ +----+ +----+
 *     QUIC datagrams |P0D0| |P0D1| |P0D2| |P0D3| |P1D0| |P2D0| |P2D1| |P2D2|
 *                    +----+ +----+ +----+ +----+ +----+ +----+ +----+ +----+
 *                    +----+ +----+ +----+        +----+ +----+ +----+ +----+
 *                    |P0D4| |P0D5| |P0D6|        |P1D1| |P2D3| |P2D4| |P2D5|
 *                    +----+ +----+ +----+        +----+ +----+ +----+ +----+
 *                                                +----+
 *                                                |P0D2|
 *                                                +----+
 * ```
 *
 * The number of additional repair symbols is a percentage (currently 30%) of
 * the number of source symbols required for a single kypacket (with a minimum
 * of at least a certain number of repair packets, currently 2):
 *
 * ```notrust
 *  ----------------+-------------------------------------------------------
 *   source symbols |  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17 18
 *  ----------------+-------------------------------------------------------
 *   repair symbols |  2  2  2  2  2  2  3  3  3  3  4  4  4  5  5  5  6  6
 *  ----------------+-------------------------------------------------------
 *    total symbols |  3  4  5  6  7  8 10 11 12 13 15 16 17 19 20 21 23 24
 *  ----------------+-------------------------------------------------------
 * ```
 *
 * Here is the format of the QUIC datagram payload:
 *
 *  - endpoint id: 64 bits (necessary for routing)
 *  - kypacket_seq: 32 bits
 *  - group_seq: 32 bits
 *  - RaptorQ Object Transmission Information: 96 bits (RFC6330 §3.3)
 *  - RaptorQ Payload ID: 32 bits (RFC6330 §3.2)
 *  - kypacket segment: chunk of kypacket "as is" (the first one includes
 *                      the kypacket header)
 *
 *
 * ### Sequence numbers
 *
 * The `kypacket_seq` sequence number is incremented for each kypacket (config
 * packet or not), and wraps around (see [Sequencer] documentation to know how
 * overflow is handled by the receiver side). It allows to detect missing
 * packets and reorder them.
 *
 * `group_seq` is an additional sequence number incremented on each config
 * packet. Its purpose is to group packets in relation to a config packet. A
 * media packet is only meaningful after its previous config packet (it may
 * reference SPS/PPS). Concretely, this allows to ignore packets associated to
 * a config packet which is not received yet.
 *
 * For datagrams, an additional 31-bit number identifies the datagram number
 * for a given `kypacket_seq`. For illustration, here are the values if a
 * kypacket 14 is split into 3 datagrams:
 *
 *  1. kypacket_seq=14 datagram_number=0 end=false
 *  2. kypacket_seq=14 datagram_number=1 end=false
 *  3. kypacket_seq=14 datagram_number=2 end=true
 *
 * This information is sufficient for the receiver to reassemble datagrams into
 * complete packets if possible, or to detect that datagrams are missing.
 *
 *
 * ### Receiver
 *
 * The receiver merely aggregates events from different sources (uni-stream
 * opening, kypackets over QUIC streams, QUIC datagrams) into a single queue to
 * be consumed by the forwarder.
 *
 * It is implemented separated from the forwarder for two reasons:
 *  - semantically, the receiver handles the transport layer between Kymux
 *    peers, while the forwarder implements the policy to forward packets to the
 *    client;
 *  - this avoids to add even more complexity to the forwarder.
 *
 *
 * ### Forwarder
 *
 * The forwarder receives stream packets, datagram messages, and can be waken
 * up on a timeout. Its role is to send complete kypackets in order (but some
 * packets may be missing) to the connected client.
 *
 * The received packets are stored in a way which facilitates processing and
 * decision to send data to the client. QUIC datagrams pass through an
 * additional layer to be reordered and re-assembled.
 *
 * ```notrust
 *          QUIC stream         QUIC datagrams
 *              |                     |
 *              |                     v
 *              |           +------------------+
 *              |           | DatagramSegments |
 *              |           +------------------+
 *              |                     |
 *              | StreamPacket        | DatagramPacket
 *              v                     v
 *           +---------------------------+
 *           |       PendingGroups       |
 *           +---------------------------+
 * ```
 *
 * This first layer is responsible to re-assemble kypackets from RaptorQ
 * packets. Concretely, a struct `DatagramSegments` feeds a RaptorQ decoder
 * for each kypacket, until the full original packet is recovered:
 *
 *
 * ```notrust
 *                    DatagramSegments
 *                    +----+ +----+ +----+ +----+
 *     kypacket 37:   | D0 | | D5 | | D2 | | D3 | // a sufficient set of RaptorQ
 *                    +----+ +----+ +----+ +----+ // encoding symbols
 *                    |                         |
 *                    v     RaptorQ decoder     v
 *                    +-------------------------+
 *                    | Original packet decoded |
 *                    +-------------------------+
 *                           +----+ +----+
 *     kypacket 39:    None  | D1 | | D2 |  // insufficient to decode kypacket
 *                           +----+ +----+
 *
 *                    +----+
 *     kypacket 42:   | D0 |  // insufficient to decode kypacket
 *                    +----+
 * ```
 *
 * Once complete, this layer produces a full kypacket (`DatagramPacket`), via
 * the method [`DatagramSegments::assemble()`].
 *
 * The packets received from the QUIC stream or reassembled from QUIC datagrams
 * are stored in a [`PendingGroup`] associated with their `group_seq`. This
 * struct embeds:
 *   - the config packet for this group (if already received)
 *   - the pending kypackets already re-assembled from datagrams
 *   - the pending datagram segments (parts of datagram packets not
 *     re-assembled yet)
 *
 * Here is a schema for illustrating the config packet and the kypackets
 * reassembled from datagrams (the pending datagram segments are described
 * above) associated to groups:
 *
 * ```notrust
 *             config packet    pending
 *               (SPS/PPS)      media packets
 *              +---------+           +---+       +---+ +---+             +---+
 *     group 0: | CFG P00 |      ___  |P01|  ___  |P03| |P04|  ___   ___  |P07|
 *              +---------+           +---+       +---+ +---+             +---+
 *                              +---+       +---+       +---+
 *     group 1:  _________      |P09|  ___  |P11|  ___  |P13|
 *                              +---+       +---+       +---+
 *
 *     group 2:  ...
 * ```
 *
 * ### Processing
 *
 * When a new event occurs (new kypacket over QUIC stream, new datagram or
 * deadline reached), the receiver analyzes the current state to decide to send
 * new packets to the client.
 *
 * Here is an overview of the strategy to find the next packet to send (see
 * [`Forwarder::next_packet()`]).
 *
 * Firstly, the obvious case, if the packet with the expected next
 * `kypacket_seq` is available (whether it's a config packet or not), it is
 * sent immediately.
 *
 * The interesting case is when a packet is available, but not the next one (it
 * has a higher `kypacket_seq`), meaning that the packets to send before are not
 * available yet. Clearly, we want to bufferize a bit to compensate for packet
 * reordering, but we don't want to wait indefinitely because the packet may be
 * lost.
 *
 * Also, it makes sense to "drop" (i.e. not wait for) "lost" packets only if
 * the next available packet may contribute to a frame. For example, if the
 * receiver only knows the next config packet but has no media packet depending
 * on it, it should not send it immediately, so that if it receives missing
 * packets from a previous group beforehand, it has a chance to transmit them
 * to the client instead of dropping them.
 *
 * The obvious solution is to send the next available packet after some
 * arbitrary timeout ([`Forwarder::MAX_BUFFERING`] currently set to 50ms). The
 * less obvious problem is to decide when to start this timeout (i.e. from
 * which starting point should the deadline be computed).
 *
 * We could consider starting the timeout when this available packet is
 * received, but this is a poor choice: the packets may be received out of
 * order, and some more recent packets may have already been received. With
 * this strategy, receiving an additional packet may delay the time the
 * receiver would forward data to the client, which is undesirable.
 *
 * Therefore, the strategy implemented in this protocol is to use the
 * assemble() date of the oldest re-assembled datagram packet. In other words,
 * for each kypacket, the receiver stores the reception date of the last
 * datagram that completed it; over all re-assembled kypackets not sent (or
 * dropped) yet, it considers the minimum (it received a full kypacket at this
 * date, so the next kypacket was expected to be received at least as early),
 * and adds a buffering delay (20ms). This is the new deadline to send the next
 * packet (that may be immediately).
 *
 * There is no need to timestamp config packets (i.e. QUIC stream packets): as
 * mentioned earlier, a config packet will never be sent alone on a timeout
 * basis (at least one datagram packet from the same group must be available).
 *
 * Note: Currently, the implementation does not take keyframes into account. It
 * may be interesting to add yet another sequence level (gop_seq) to send media
 * packets only after the GOP keyframe is sent. It will depend if we use an
 * intra-refresh strategy or if we send keyframes often.
 */

pub(crate) struct UnreliableFecProtocol {
    desc: EndpointDesc,
}

impl UnreliableFecProtocol {
    #[allow(dead_code)]
    pub(crate) fn new(desc: EndpointDesc) -> Self {
        Self { desc }
    }
}

#[async_trait]
impl Protocol for UnreliableFecProtocol {
    async fn forward(&mut self, ky_channel: KyChannel, client: TcpStream) -> Result<()> {
        let (client_rx, client_tx) = client.into_split();
        let (ky_channel_rx, ky_channel_tx) = ky_channel.into_split();

        if self.desc.owner == StreamOwner::Local {
            let sender = sender::Sender::new();
            sender.send_video(ky_channel_tx, client_rx).await
        } else {
            let receiver = receiver::Receiver::new();
            receiver.recv_video(ky_channel_rx, client_tx).await
        }
    }
}
