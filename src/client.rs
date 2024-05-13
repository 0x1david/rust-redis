use crate::parser::{Command, Payload, PayloadVec, RedisEncodable, Value, DELIMITER};
use crate::store::redis_type::Stream;
use crate::store::{KeyValueStore, RedisType};
use anyhow::{bail, Context, Result};
use hex_literal::hex;
use log::{debug, warn};
use std::collections::HashMap;
use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};

static DEFAULT_ID: [u8;88] = hex!("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");

#[derive(Clone)]
pub(crate) struct RedisClient {
    store: Arc<RwLock<KeyValueStore>>,
    pub role: ClientRole,
}

impl RedisClient {
    pub async fn setup_client(replicaof: Option<String>) -> Self {
        if let Some(address) = replicaof {
            let address = address.replace(' ', ":").replace("localhost", "127.0.0.1");
            println!("Setting up client on address: {}", address);
            // let address = address.join(":").replace("localhost", "127.0.0.1");
            let (r, w) = RedisClient::handshake(&address).await.unwrap();

            Self {
                store: Arc::new(RwLock::new(KeyValueStore::new())),
                role: ClientRole::Slave {
                    master_stream_w: Arc::new(Mutex::new(w)),
                    master_stream_r: Arc::new(Mutex::new(r)),
                    master_id: "?".to_string(),
                    master_address: address,
                    master_offset: -1,
                },
            }
        } else {
            Self {
                store: Arc::new(RwLock::new(KeyValueStore::new())),
                role: ClientRole::new_master(),
            }
        }
    }

    pub(crate) async fn process_command(
        &self,
        command: Command,
        contents: Value,
        stream: ClientWrite,
        addr: &SocketAddr,
        reply: bool
    ) -> Result<()> {
        debug!("[PROCESS_COMMAND] - START");
        let response = match command {
            Command::Echo => {
                debug!("[PROCESS_COMMAND] - Processing 'Echo' Command");
                let value = match contents {
                    Value::String(s) => s,
                    Value::Array(x) => PayloadVec(x).redis_encode(),
                    Value::Empty => "".to_string(),
                };
                value.to_string()
            }
            Command::Ping => {
                debug!("[PROCESS_COMMAND] - Processing 'Ping' Command");
                Payload::SimpleString("PONG".to_string()).redis_encode()
            }
            Command::Get => {
                debug!("[PROCESS_COMMAND] - Processing 'Get' Command");
                let value = match contents {
                    Value::String(s) => s,
                    Value::Array(x) => x[0].to_string(),
                    _ => bail!("unimplemented"),
                };
                self.store.write().await.get(&value)
            }
            Command::Set => {
                debug!("[PROCESS_COMMAND] - Processing 'Set' Command");
                let (key, value, arg, arg_value) = match contents {
                    Value::Array(x) => (
                        x[0].to_string(),
                        RedisType::String(x[1].to_string()),
                        x.get(2).cloned(),
                        x.get(3).cloned(),
                    ),
                    _ => bail!("Cant store data in given format."),
                };
                match &self.role {
                    ClientRole::Master {
                        slave_connections, ..
                    } => {
                        debug!(
                            "[PROCESS_COMMAND] - Slave connections status: {:?}.",
                            slave_connections
                        );
                        debug!("[PROCESS_COMMAND] - Processing 'Set' as Master.");
                        let payload =
                            Payload::build_bulk_string_array(vec!["SET", &key, value.as_inner()])
                                .redis_encode();
                        debug!("[PROCESS_COMMAND] - Encoded payload: {:?}.", payload);

                        debug!("[PROCESS_COMMAND] - Propagating payload to slaves.");
                        self.propagate(payload.as_bytes()).await?;
                        debug!("[PROCESS_COMMAND] - Processing set locally.");
                        self.process_set(key, value, arg, arg_value).await?
                    }
                    ClientRole::Slave { .. } => {
                        debug!("[PROCESS_COMMAND] - Processing 'Set' locally as a Slave.");
                        self.process_set(key, value, arg, arg_value).await?
                    }
                }
            }
            Command::Type => {
                debug!("[PROCESS_COMMAND] - Processing 'Type' Command");
                let value = match contents {
                    Value::String(s) => s,
                    Value::Array(x) => x[0].to_string(),
                    _ => bail!("unimplemented"),
                };
                self.store.read().await.get_type(&value)
            }
            Command::XAdd => {
                debug!("[PROCESS_COMMAND] - Processing 'XAdd' Command");
                match contents {
                    Value::Array(x) => {
                        let (key, value) = x.split_at(2);
                        let stream_key = key[0].to_string();
                        let entry_id = key[1].to_string();
                        let value =
                            RedisType::Stream(Stream::new(&entry_id, &mut value[1..].to_vec()));
                        self.store.write().await.set(&stream_key, value, None)?;
                        Payload::BulkString(entry_id).redis_encode()
                    }
                    _ => bail!("Incorrect input type."),
                }
            }
            Command::Info => {
                debug!("[PROCESS_COMMAND] - Processing 'Info' Command");
                let value = match contents {
                    Value::String(s) => s,
                    Value::Array(x) => x[0].to_string(),
                    _ => bail!("unimplemented"),
                };
                match value.as_str() {
                    "replication" => Payload::BulkString(self.role.to_string()).redis_encode(),
                    _ => bail!("Unimplemented"),
                }
            }
            Command::ReplConf => {
                debug!("[PROCESS_COMMAND] - Processing 'ReplConf' Command");
                match &self.role {
                    ClientRole::Master { .. } => {}
                    _ => unimplemented!(),
                }
                Payload::SimpleString("OK".to_string()).redis_encode()
            }
            Command::PSync => {
                let mut lock = stream.lock().await;
                lock.write_all(self.role.psync().as_bytes()).await?;

                let byte_vec = get_empty_rdb();
                lock.write_all(&byte_vec).await?;

                let new_stream = stream.clone();
                match &self.role {
                    ClientRole::Slave {
                        ..
                    } => {
                        debug!("[PROCESS_COMMAND] - As Slave.");
                        debug!(
                            "[PROCESS_COMMAND] - Setting master stream to {:?}.",
                            new_stream
                        );
                        // *master_stream = new_stream;
                        debug!("Idk what i was supposed to do here")
                    }
                    ClientRole::Master {
                        slave_connections, ..
                    } => {
                        debug!("[PROCESS_COMMAND] - As Master.");
                        debug!("[PROCESS_COMMAND] - Adding stream {:?} to slave connections with key: '{}'.", new_stream, addr);
                        slave_connections
                            .lock()
                            .await
                            .insert(addr.to_string(), stream.clone());
                    }
                }
                debug!("[PROCESS_COMMAND] - Finished processing command.");
                String::default()
            }
        };

        debug!("[PROCESS_COMMAND] - Writing response to stream.");
        if reply {
            stream.lock().await.write_all(response.as_bytes()).await?;
        }
        debug!("[PROCESS_COMMAND] - END.");

        Ok(())
    }

    pub async fn handshake(addr: &str) -> Result<(ReadHalf<TcpStream>, WriteHalf<TcpStream>)> {
        debug!("[HANDSHAKE] - START.");
        let payload = Payload::build_bulk_string_array(vec!["ping"]).redis_encode();
        let psync = ClientRole::init_psync();

        debug!("[HANDSHAKE] - Creating messages.");
        let messages = [
            payload.as_bytes(),
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n".as_bytes(),
            "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".as_bytes(),
            psync.as_bytes(),
        ];
        debug!("[HANDSHAKE] - Establishing Stream.");
        let (mut r, mut w) = Self::connect_to_master(addr).await?;

        let mut buf = vec![0; 1024];
        for msg in messages {
            w.write_all(msg).await?;
            let _ = r.read(&mut buf).await?;
        }

        debug!("[HANDSHAKE] - END.");
        Ok((r, w))
    }

    async fn connect_to_master(
        master_address: &str,
    ) -> Result<(ReadHalf<TcpStream>, WriteHalf<TcpStream>)> {
        debug!("[CONNECT_TO_MASTER] - START..");
        let timeout_duration = tokio::time::Duration::from_secs(2);
        let connect_future = TcpStream::connect(master_address);
        match tokio::time::timeout(timeout_duration, connect_future).await {
            Ok(Ok(stream)) => {
                debug!("[CONNECT_TO_MASTER] - Successfully Connected..");
                debug!("[CONNECT_TO_MASTER] - END..");
                let (r, w) = tokio::io::split(stream);
                Ok((r, w))
            }
            Ok(Err(e)) => {
                warn!(
                    "[CONNECT_TO_MASTER] - Failed to connect to {}: {}.",
                    master_address, e
                );
                warn!("[CONNECT_TO_MASTER] - Terminating..");
                bail!(e)
            }
            Err(e) => {
                warn!(
                    "[CONNECT_TO_MASTER] - Connection attempt to {} timed out: {}.",
                    master_address, e
                );
                warn!("[CONNECT_TO_MASTER] - Terminating..");
                bail!(e)
            }
        }
    }

    pub async fn propagate(&self, message: &[u8]) -> Result<()> {
        debug!("[PROPAGATE] - START");
        let res = match &self.role {
            ClientRole::Master {
                slave_connections, ..
            } => {
                let connections = slave_connections.lock().await;

                let futures: Vec<_> = connections
                    .iter()
                    .map(|(_, stream)| async move {
                        let mut stream = stream.lock().await;
                        debug!("[PROPAGATE] - Writing to Stream.");
                        stream.write_all(message).await
                    })
                    .collect();

                debug!("[PROPAGATE] - Joining and awaiting all connection propagations.");
                futures::future::join_all(futures).await;
                Ok(())
            }
            ClientRole::Slave { .. } => {
                debug!("[PROPAGATE] - Slave currently can't propagate.");
                debug!("[PROPAGATE] - Terminating..");
                bail!("Slave currently can't propagate")
            }
        };
        debug!("[PROPAGATE] - END");
        res
    }
    pub async fn process_set(
        &self,
        key: String,
        value: RedisType,
        arg: Option<Payload>,
        arg_value: Option<Payload>,
    ) -> Result<String> {
        if let Some(arg) = arg {
            let arg_value = arg_value
                .context("Missing arg specifier")?
                .to_string()
                .parse::<i64>()
                .context("Incorrect arg type expected an integer.")?;
            match arg.to_string().to_lowercase().as_str() {
                "px" => self
                    .store
                    .write()
                    .await
                    .set(&key.to_string(), value, Some(arg_value)),
                _ => bail!("unimplemented arg."),
            }
        } else {
            self.store.write().await.set(&key.to_string(), value, None)
        }
    }
}

type ClientWrite = Arc<Mutex<WriteHalf<TcpStream>>>;

#[derive(Clone)]
pub enum ClientRole {
    Master {
        replication_id: String,
        replication_offset: usize,
        slave_connections: Arc<Mutex<HashMap<String, ClientWrite>>>,
    },
    Slave {
        master_stream_w: ClientWrite,
        master_stream_r: Arc<Mutex<ReadHalf<TcpStream>>>,
        master_address: String,
        master_id: String,
        master_offset: i32,
    },
}

impl ClientRole {
    pub fn new_master() -> Self {
        Self::Master {
            slave_connections: Arc::new(Mutex::new(HashMap::new())),
            replication_id: String::from_utf8_lossy(&DEFAULT_ID).to_string(),
            replication_offset: 0,
        }
    }
    pub fn init_psync() -> String {
        debug!("[PSYNC] - Creating psync payload.");
        Payload::build_bulk_string_array(vec!["PSYNC", "?", "-1"]).redis_encode()
    }

    pub fn psync(&self) -> String {
        match self {
            Self::Master {
                replication_id,
                replication_offset,
                ..
            } => Payload::SimpleString(format!(
                "FULLRESYNC {} {}",
                replication_id, replication_offset
            ))
            .redis_encode(),
            Self::Slave { .. } => panic!("Slave can only initialize psync, not reply to it"),
        }
    }
}

fn get_empty_rdb() -> Vec<u8> {
    let empty_rdb_data = DEFAULT_ID;

    let mut arr = vec![];
    let len = empty_rdb_data.len();
    let rdb_length = format!("${}{}", len, DELIMITER);

    arr.extend_from_slice(rdb_length.as_bytes());
    arr.extend_from_slice(&empty_rdb_data);
    println!("DEBUG EMPTY RDB: {}", String::from_utf8_lossy(&arr));
    arr
}

impl Display for ClientRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Master {
                replication_offset,
                replication_id,
                ..
            } => write!(
                f,
                "role:master\nmaster_replid:{}\nmaster_repl_offset:{}",
                replication_id, replication_offset
            ),
            Self::Slave { .. } => write!(f, "role:slave"),
        }
    }
}
