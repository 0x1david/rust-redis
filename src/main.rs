mod client;
mod parser;
mod store;
use anyhow::{bail, Result};
use clap::Parser;
use client::{ClientRole, RedisClient};
use core::net::SocketAddr;
use parser::RedisProtocolParser;
use std::{collections::HashMap, io::Cursor, sync::Arc};
use tokio::{io::{AsyncReadExt, split, ReadHalf, WriteHalf}, net::{TcpListener, TcpStream}, sync::Mutex, select};
use log::{info, warn, debug};

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = 6379)]
    port: u16,

    #[clap(long, num_args = 2)]
    replicaof: Option<Vec<String>>,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    debug!("[MAIN] - At Main Start");
    let args = Arc::new(Args::parse());
    let address = format!("127.0.0.1:{}", args.port);
    info!("Booting server at: {}", &address);
    let client = if let Some(address) = &args.replicaof {
        let address = address.join(":").replace("localhost", "127.0.0.1");
        debug!("[MAIN] - Operating as a replica of {}", address);
        let mut client = RedisClient::new(ClientRole::Slave {
            master_stream_w: None,
            master_stream_r: None,
            master_address: address,
            master_id: "?".to_string(),
            master_offset: -1,
        });
        client.handshake().await.unwrap();
        client
    } else {
        debug!("[MAIN] - Operating as master");
        RedisClient::new(ClientRole::Master {
            slave_connections: Arc::new(Mutex::new(HashMap::new())),
            replication_id: "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2".to_string(),
            replication_offset: 0,
        })
    };
    let listener = TcpListener::bind(address).await.unwrap();
    info!("Binding listener was successful");

    loop {
        info!("Listening for connections...");
        debug!("{}", debug_slave_connections(&client, "MAIN-START_OF_LOOP"));
        match listener.accept().await {
            Ok((stream, addr)) => {
                let (read, write) = split(stream);
                let mut read = read;
                let write = Arc::new(write.into());
                info!("Accepted new connection: {}", addr);
                debug!("{}", debug_slave_connections(&client, "MAIN-PRE_CLONING"));
                let client_clone = client.clone();
                debug!("{}", debug_slave_connections(&client, "MAIN-PRE_HANDLECONNECTION"));
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(write, &mut read, addr, client_clone).await {
                        warn!("Failed to handle connection {}", e)
                    }
                });
                debug!("{}", debug_slave_connections(&client, "MAIN-POST_HANDLE_CONNECTION"));
            }
            Err(e) => {
                warn!("error: {}", e);
            }
        }
    }
}

async fn handle_connection(
    stream_write: Arc<Mutex<WriteHalf<TcpStream>>>,
    stream_read: &mut ReadHalf<TcpStream>,
    addr: SocketAddr,
    mut client: RedisClient,
) -> Result<()> {
    debug!("[HANDLE_CONNECTION] - START");
    let mut buf = [0; 1024];
    let mut buf_master = [0; 1024];
    let mut received_data: Cursor<&str>;
    let mut read_bytes: usize;
    let mut reply: bool;

    loop {
        debug!("[HANDLE_CONNECTION] - MUTEX - Stream locked.");
        debug!("{}", debug_slave_connections(&client, "HANDLE_CONNECTION"));
            match client.role {
                ClientRole::Master { .. } => {
                    read_bytes = stream_read.read(&mut buf).await?;
                    received_data = Cursor::new(std::str::from_utf8(&buf[..read_bytes])?);
                    reply = true;
                }
                ClientRole::Slave { ref master_stream_r, .. } => {
                    let mut master_lock = master_stream_r
                        .as_ref()
                        .expect("Slave should be set.")
                        .lock().await;
                    select! {
                        read_result = stream_read.read(&mut buf) => {
                            read_bytes = read_result?;
                            received_data = Cursor::new(std::str::from_utf8(&buf[..read_bytes])?);
                            reply = true;
                        }
                           
                        read_result = master_lock.read(&mut buf_master) => {
                            read_bytes = read_result?;
                            received_data = Cursor::new(std::str::from_utf8(&buf_master[..read_bytes])?);
                            reply = false;
                        }
                    }
                }
            }

        debug!("[HANDLE_CONNECTION] - MUTEX - Read Stream unlocked: {:?}.", stream_read);
        debug!("{}", debug_slave_connections(&client, "HANDLE_CONNECTION"));
        if read_bytes == 0 {
            debug!("[HANDLE_CONNECTION] - Read zero bytes, returning");
            return Ok(());
        }

        debug!("[HANDLE_CONNECTION]parsing payload {:?}", received_data);
        let payload = RedisProtocolParser::parse(&mut received_data)?;

        debug!("[HANDLE_CONNECTION] - Calling retrieve content on payload.");
        let (command, contents) = payload.retrieve_content()?;

        debug!("[HANDLE_CONNECTION] - Retrieved command: {:?}, contents: {:?}", command, contents);
        if let Some(command) = command {
            debug!("[HANDLE_CONNECTION] - Calling proccess command.");
            client
                .process_command(command, contents, stream_write.clone(), &addr, reply)
                .await?;
        } else {
            bail!("Handling inputs without commands is not supported.")
        }
        debug!("{}", debug_slave_connections(&client, "HANDLE_CONNECTION"));
        debug!("[HANDLE_CONNECTION] - NEXT LOOP");
    };
}

fn debug_slave_connections(client: &RedisClient, func_name: &str) -> String {
        match client.role {
            ClientRole::Master { ref slave_connections, .. } => {
                format!("[{}] - Slave connections status: {:?}", func_name ,slave_connections)
            }
            _ => {
                format!("[{}] - is slave -> doing nothing.", func_name)
            }
        }
}
