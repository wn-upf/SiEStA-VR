use crate::lib::alvr_stream_socket::{try_again, BufferedReceiver, ConResult, SocketReader, ToCon};
use anyhow::Result;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    marker::PhantomData,
    mem,
    // sync::{
    //     Arc, Mutex,
    // },
    time::{Duration, Instant},
};

use std::net::IpAddr;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ControlPacketType {
    // Example fields; replace these with the actual data you want to send
    pub data: Vec<u8>,
}

use crossbeam::channel::{unbounded, Sender};
// This corresponds to the length of the payload
const FRAMED_PREFIX_LENGTH: usize = mem::size_of::<u32>();

fn framed_send<S: Serialize>(
    sender: &mut Sender<Vec<u8>>,
    buffer: &mut Vec<u8>,
    packet: &S,
) -> Result<()> {
    let serialized_size = bincode::serialized_size(&packet)? as usize;
    let packet_size = serialized_size + FRAMED_PREFIX_LENGTH;

    if buffer.len() < packet_size {
        buffer.resize(packet_size, 0);
    }

    buffer[0..FRAMED_PREFIX_LENGTH].copy_from_slice(&(serialized_size as u32).to_be_bytes());
    bincode::serialize_into(&mut buffer[FRAMED_PREFIX_LENGTH..packet_size], &packet)?;

    sender.send(buffer.to_vec())?;

    Ok(())
}

pub fn framed_recv<R: DeserializeOwned>(
    receiver: &mut BufferedReceiver<Vec<u8>>,
    buffer: &mut Vec<u8>,
    timeout: Duration,
) -> ConResult<R> {
    let deadline = Instant::now() + timeout;

    loop {
        let count = receiver.recv(buffer).unwrap();
        if count >= FRAMED_PREFIX_LENGTH {
            let packet_length = FRAMED_PREFIX_LENGTH
                + u32::from_be_bytes(buffer[0..FRAMED_PREFIX_LENGTH].try_into().unwrap()) as usize;
            if count >= packet_length {
                let packet =
                    bincode::deserialize(&buffer[FRAMED_PREFIX_LENGTH..packet_length]).to_con()?;
                return Ok(packet);
            }
        } else if Instant::now() > deadline {
            return try_again();
        }
    }
}

pub fn framed_recv_vec<R: serde::de::DeserializeOwned>(buffer: &[u8]) -> Result<R, bincode::Error> {
    // println!("BUFLEN = {} / {}", buffer.len(), FRAMED_PREFIX_LENGTH);
    if buffer.len() < FRAMED_PREFIX_LENGTH {
        return Err(Box::new(bincode::ErrorKind::SizeLimit));
    }

    let packet_length = FRAMED_PREFIX_LENGTH
        + u32::from_be_bytes(buffer[0..FRAMED_PREFIX_LENGTH].try_into().unwrap()) as usize;
    if buffer.len() < packet_length {
        return Err(Box::new(bincode::ErrorKind::SizeLimit));
    }

    bincode::deserialize(&buffer[FRAMED_PREFIX_LENGTH..packet_length])
}

pub struct ControlSocketSender<T> {
    inner: Sender<Vec<u8>>,
    buffer: Vec<u8>,
    _phantom: PhantomData<T>,
}

impl<S: Serialize> ControlSocketSender<S> {
    pub fn send(&mut self, packet: &S) -> Result<()> {
        framed_send(&mut self.inner, &mut self.buffer, packet)
    }
}
#[allow(unused)]
#[derive(Clone)]
pub struct ControlSocketReceiver<R: DeserializeOwned> {
    inner: BufferedReceiver<Vec<u8>>,
    buffer: Vec<u8>,
    _phantom: PhantomData<R>,
}
#[allow(unused)]
impl<R: DeserializeOwned> ControlSocketReceiver<R> {
    pub fn recv(&mut self, timeout: Duration) -> ConResult<R> {
        framed_recv(&mut self.inner, &mut self.buffer, timeout)
    }
}
#[derive(Clone)]
pub struct ProtoControlSocket {
    sender: Sender<Vec<u8>>,
    receiver: BufferedReceiver<Vec<u8>>,
}
#[allow(unused)]
impl ProtoControlSocket {
    pub fn connect(timeout: Duration) -> ConResult<(Self, IpAddr)> {
        let (sender, receiver) = unbounded();
        let receiver = BufferedReceiver::new(receiver);

        Ok((
            ProtoControlSocket { sender, receiver },
            "0.0.0.0".parse().unwrap(), // Placeholder for now
        ))
    }

    pub fn send<S: Serialize>(&mut self, packet: &S) -> Result<()> {
        framed_send(&mut self.sender, &mut vec![], packet)
    }

    pub fn recv<R: DeserializeOwned>(&mut self, timeout: Duration) -> ConResult<R> {
        framed_recv(&mut self.receiver, &mut vec![], timeout)
    }

    pub fn split<S: Serialize, R: DeserializeOwned>(
        self,
    ) -> Result<(ControlSocketSender<S>, ControlSocketReceiver<R>)> {
        Ok((
            ControlSocketSender {
                inner: self.sender,
                buffer: vec![],
                _phantom: PhantomData,
            },
            ControlSocketReceiver::<R> {
                inner: self.receiver,
                buffer: vec![],
                _phantom: PhantomData,
            },
        ))
    }
}
