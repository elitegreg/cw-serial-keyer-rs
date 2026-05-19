use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::task::JoinHandle;

use crate::{Result, SerialChannel};

/// Handle for a Tokio TCP-to-serial byte proxy.
///
/// The server accepts one TCP client at a time. Bytes read from the TCP client
/// are written through a [`SerialChannel`], and bytes read from that channel are
/// forwarded to the TCP client.
pub struct SerialTcpServer {
    addr: SocketAddr,
    task: Option<JoinHandle<()>>,
}

impl SerialTcpServer {
    pub(crate) async fn start(addr: impl ToSocketAddrs, channel: SerialChannel) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let task = tokio::spawn(run_server(listener, channel));
        Ok(Self {
            addr,
            task: Some(task),
        })
    }

    /// The socket address the server is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stop the server task.
    pub async fn close(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for SerialTcpServer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn run_server(listener: TcpListener, channel: SerialChannel) {
    loop {
        let Ok((stream, _peer)) = listener.accept().await else {
            break;
        };
        let client_channel = channel.resubscribe();
        let _ = proxy_client(stream, client_channel).await;
    }
}

async fn proxy_client(stream: TcpStream, mut channel: SerialChannel) -> Result<()> {
    let (mut tcp_read, mut tcp_write) = stream.into_split();
    let mut tcp_buf = [0_u8; 4096];

    loop {
        tokio::select! {
            n = tcp_read.read(&mut tcp_buf) => {
                let n = n?;
                if n == 0 {
                    break;
                }
                channel.write(&tcp_buf[..n]).await?;
            }
            serial = channel.read() => {
                tcp_write.write_all(&serial?).await?;
            }
        }
    }

    Ok(())
}
