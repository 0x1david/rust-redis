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


        // 1. select statement
        match client.role {
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
                ..
            } => {
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
                        // Err(e) => {
                        //     warn!("error: {}", e);
                        // }
                    _ = handle_master_propagation(client.clone()) => {

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

async fn handle_master_propagation(client: Arc<RedisClient>) -> Result<()> {
    Ok(())
}

async fn handle_connection(
    stream_write: Arc<Mutex<WriteHalf<TcpStream>>>,
    stream_read: &mut ReadHalf<TcpStream>,
    addr: SocketAddr,
    // Cloning the Redis client needs to be dealt with. Should work with Arc<Mutex<RedisClient>>
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

        let payload = RedisProtocolParser::parse(&mut received_data)?;

        let (command, contents) = payload.retrieve_content()?;
        debug!(
            "[HANDLE_CONNECTION] - Retrieved command: {:?}, contents: {:?}",
            command, contents
        );

        if let Some(command) = command {
            client
                .process_command(command, contents, stream_write.clone(), &addr)
                .await?;
        } else {
            bail!("Handling inputs without commands is not supported.")
        }
        debug!("[HANDLE_CONNECTION] - NEXT LOOP");
    }
}
