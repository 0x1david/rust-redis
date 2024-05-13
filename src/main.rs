mod client;
mod parser;
mod store;
use anyhow::{bail, Result};
use clap::Parser;
use client::{ClientRole, RedisClient};
use core::net::SocketAddr;
use log::{debug, info, warn};
use parser::RedisProtocolParser;
use std::{io::Cursor, sync::Arc};
use tokio::{
    io::{split, AsyncReadExt, ReadHalf, WriteHalf},
    net::{TcpListener, TcpStream},
    select,
    sync::Mutex,
};

use crate::{parser::Value, store::RedisType};
static PSYNC_IGNORE: [u8; 1024] = [36, 56, 56, 13, 10, 82, 69, 68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192, 64, 250, 5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250, 8, 117, 115, 101, 100, 45, 109, 101, 109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101, 192, 0, 255, 240, 110, 59, 254, 192, 255, 90, 162, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = 6379)]
    port: u16,

    #[clap(long, num_args = 1)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = Args::parse();
    let address = format!("127.0.0.1:{}", args.port);
    info!("Booting server at: {}", &address);

    let listener = TcpListener::bind(address).await.unwrap();
    info!("Binding listener was successful");

    let client = RedisClient::setup_client(args.replicaof).await;
    let client = Arc::new(client);

    loop {
        info!("Listening for connections...");
        let client_clone = client.clone();
        let mut buf = [0; 1024];

        match &client.role {
            ClientRole::Master {..} => {
                let (stream, addr) = listener.accept().await.unwrap();
                    let (mut read, write) = split(stream);
                    let write = Arc::new(write.into());

                    info!("Accepted new connection: {}", addr);
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(write, &mut read, addr, client_clone).await {
                            warn!("Failed to handle connection {}", e)
                        }
                    });
            },
            ClientRole::Slave {
               master_stream_r,
               ..
            } => {
                let mut lock = master_stream_r.lock().await;
                select! {
                    Ok((stream, addr)) = listener.accept() => {
                            let (mut read, write) = split(stream);
                            let write = Arc::new(write.into());
                            info!("Accepted new connection: {}", addr);
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(write, &mut read, addr, client_clone).await {
                                    warn!("Failed to handle connection {}", e)
                                }
                            });
                        }
                    Ok(read_bytes) = lock.read(&mut buf) => {
                    if read_bytes == 0 {
                        debug!("[HANDLE_CONNECTION] - Read zero bytes, returning");
                        return
                    }
                    println!("{:?}", &buf);
                    if buf == PSYNC_IGNORE {
                        println!("Ignoring RDB COMMAND");
                        continue
                    }
                    let mut received_data = Cursor::new(std::str::from_utf8(&buf[..read_bytes]).expect("Should never be wrong format."));
                    let _ = handle_propagation_from_master(&mut received_data, client_clone).await;

                    }
                }
            }
        }
        // 2. Select Statement

        // let mut master_lock = master_stream_r
        //     .as_ref()
        //     .expect("Slave should be set.")
        //     .lock()
        //     .await;

        // select! {
        //     read_result = stream_read.read(&mut buf) => {
        //         read_bytes = read_result?;
        //         received_data = Cursor::new(std::str::from_utf8(&buf[..read_bytes])?);
        //     }

        //     read_result = master_lock.read(&mut buf_master) => {
        //         read_bytes = read_result?;
        //         received_data = Cursor::new(std::str::from_utf8(&buf_master[..read_bytes])?);
        //     }
        // }
    }
}

async fn handle_propagation_from_master(data: &mut Cursor<&str>, client: Arc<RedisClient>) -> Result<()> {
    let payloads = RedisProtocolParser::parse(data)?;
    for payload in payloads {
        let (command, contents) = payload.retrieve_content()?;
        debug!(
            "[HANDLE_CONNECTION] - Retrieved master propagation command: {:?}, contents: {:?}",
            command, contents
        );

        if command.is_some() {
            let (key, value, arg, arg_value) = match contents {
                Value::Array(x) => (
                    x[0].to_string(),
                    RedisType::String(x[1].to_string()),
                    x.get(2).cloned(),
                    x.get(3).cloned(),
                ),
                _ => bail!("Cant store data in given format."),
            };
            let _ = client.process_set(key, value, arg, arg_value).await?;
        } else {
            bail!("Handling inputs without commands is not supported.")
        };
    };
    Ok(())

}

async fn handle_connection(
    stream_write: Arc<Mutex<WriteHalf<TcpStream>>>,
    stream_read: &mut ReadHalf<TcpStream>,
    addr: SocketAddr,
    client: Arc<RedisClient>,
) -> Result<()> {
    debug!("[HANDLE_CONNECTION] - START");
    let mut buf = [0; 1024];
    let mut received_data: Cursor<&str>;
    let mut read_bytes: usize;

    loop {
        read_bytes = stream_read.read(&mut buf).await?;
        if read_bytes == 0 {
            debug!("[HANDLE_CONNECTION] - Read zero bytes, returning");
            return Ok(());
        }

        received_data = Cursor::new(std::str::from_utf8(&buf[..read_bytes])?);

        let payloads = RedisProtocolParser::parse(&mut received_data)?;
        let payload_len = payloads.len() - 1;

        for (index, payload) in payloads.into_iter().enumerate() {
            let last = index == payload_len;
            let (command, contents) = payload.retrieve_content()?;
            debug!(
                "[HANDLE_CONNECTION] - Retrieved command: {:?}, contents: {:?}",
                command, contents
            );

            if let Some(command) = command {
                client
                    .process_command(command, contents, stream_write.clone(), &addr, last)
                    .await?;
            } else {
                bail!("Handling inputs without commands is not supported.")
            }

        }
        debug!("[HANDLE_CONNECTION] - NEXT LOOP");
    }
}
