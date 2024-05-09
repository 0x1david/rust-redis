use crate::parser::{Command, Payload, PayloadVec, RedisEncodable, Value, DELIMITER};
use crate::store::redis_type::Stream;
use crate::store::{KeyValueStore, RedisType};
use anyhow::{bail, Context, Result};
use hex_literal::hex;
use std::collections::HashMap;
use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::sleep;
use tokio::io::{AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use log::{debug, warn};

type Replica = Arc<Mutex<WriteHalf<TcpStream>>>;

#[derive(Clone)]
pub(crate) struct RedisClient {
    store: KeyValueStore,
    pub role: ClientRole,
}

impl RedisClient {
    pub(crate) fn new(role: ClientRole) -> Self {
        Self {
            store: KeyValueStore::new(),
            role,
        }
    }

    pub(crate) async fn process_command(
        &mut self,
        command: Command,
        contents: Value,
        stream: Replica,
        addr: &SocketAddr,
        should_reply: bool,
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
                self.store.get(&value).await
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
                    ClientRole::Master { slave_connections, .. } => {
                        debug!("[PROCESS_COMMAND] - Slave connections status: {:?}.", slave_connections);
                        debug!("[PROCESS_COMMAND] - Processing 'Set' as Master.");
                        let payload =
                            Payload::build_bulk_string_array(vec!["SET", &key, &value.as_inner()])
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
                self.store.get_type(&value).await
            }
            Command::XAdd => {
                debug!("[PROCESS_COMMAND] - Processing 'XAdd' Command");
                match contents {
                    Value::Array(x) => {
                        let (key, value) = x.split_at(2);
                        let stream_key = key[0].to_string();
                        let entry_id = key[1].to_string();
                        let value = RedisType::Stream(Stream::new(&entry_id, &mut value[1..].to_vec()));
                        self.store.set(&stream_key, value, None).await?;
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
                match &mut self.role {
                    ClientRole::Master { .. } => {}
                    _ => unimplemented!(),
                }
                Payload::SimpleString("OK".to_string()).redis_encode()
            }
            Command::PSync => {
                debug!("[PROCESS_COMMAND] - Processing 'PSync' Command");
                debug!("[PROCESS_COMMAND] - MUTEX - Locking stream: {:?}.", stream);
                let mut lock = stream.lock().await;
                debug!("[PROCESS_COMMAND] - Writing PSync to Stream .");
                lock.write_all(self.role.psync().as_bytes()).await?;
                let byte_vec = get_empty_rdb();
                debug!("[PROCESS_COMMAND] - Writing RDB File to Stream .");
                lock.write_all(&byte_vec).await?;
                debug!("[PROCESS_COMMAND] - Writing RDB File to Stream .");
                debug!("[PROCESS_COMMAND] - Cloning Stream.");
                let new_stream = stream.clone();
                match &mut self.role {
                    ClientRole::Slave { master_stream_w: master_stream, .. } => {
                        debug!("[PROCESS_COMMAND] - As Slave.");
                        debug!("[PROCESS_COMMAND] - Setting master stream to {:?}.", new_stream);
                        *master_stream = Some(new_stream);
                    }
                    ClientRole::Master {
                        slave_connections, ..
                    } => {
                        debug!("[PROCESS_COMMAND] - As Master.");
                        debug!("[PROCESS_COMMAND] - Adding stream {:?} to slave connections with key: '{}'.", new_stream, addr);
                        debug!("[PROCESS_COMMAND] - MUTEX - Locking Slave Connections: {:?}.", slave_connections);
                        slave_connections
                        .lock()
                        .await
                        .insert(addr.to_string(), stream.clone());
                        debug!("[PROCESS_COMMAND] - MUTEX - Unlocking Slave Connections: {:?}.", slave_connections);
                    }
                }
                String::default()
            }
        };
        debug!("[PROCESS_COMMAND] - Finished processing command.");
        if should_reply {
            debug!("[PROCESS_COMMAND] - MUTEX - Locking stream: {:?}.", stream);
            debug!("[PROCESS_COMMAND] - Writing response to stream.");
            stream.lock().await.write_all(response.as_bytes()).await?;
            debug!("[PROCESS_COMMAND] - MUTEX - Unlocking stream: {:?}", stream);
            debug!("[PROCESS_COMMAND] - END.");
        }
        Ok(())
    }

    pub async fn handshake(&mut self) -> Result<()> {
        debug!("[HANDSHAKE] - START.");
        let payload = Payload::build_bulk_string_array(vec!["ping"]).redis_encode();
        let psync = self.role.psync();

        debug!("[HANDSHAKE] - Creating messages.");
        let messages = [
            payload.as_bytes(),
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n".as_bytes(),
            "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".as_bytes(),
            psync.as_bytes(),
        ];
        debug!("[HANDSHAKE] - Establishing Stream.");
        self.establish_stream().await?;

        debug!("[HANDSHAKE] - Sending Messages.");
        for msg in messages {
            self.send_message(msg).await?;
        }
        debug!("[HANDSHAKE] - END.");
        Ok(())
    }

    pub async fn establish_stream(&mut self) -> Result<()> {
        debug!("[ESTABLISH_STREAM] - START..");
        match &mut self.role {
            ClientRole::Slave {
                master_address,
                master_stream_w,
                master_stream_r,
                ..
            } => {
                if master_stream_w.is_some() {
                    warn!("[ESTABLISH_STREAM] - Slave already has a master stream..");
                    warn!("[ESTABLISH_STREAM] - Terminating..");
                    bail!("Stream already established");
                }

                debug!("[ESTABLISH_STREAM] - Connecting to master..");
                let (r, w) = Self::connect_to_master(master_address).await?;
                *master_stream_r = Some(Arc::new(Mutex::new(r)));
                *master_stream_w = Some(Arc::new(Mutex::new(w)));
                debug!("[ESTABLISH_STREAM] - Master stream saved..");
                debug!("[ESTABLISH_STREAM] - END..");
                Ok(())
            }
            ClientRole::Master { .. } => {
                warn!("[ESTABLISH_STREAM] - Master trying to establish stream..");
                warn!("[ESTABLISH_STREAM] - Terminating..");
                bail!("Master doesn't establish streams.")
            }
        }
    }

    async fn connect_to_master(master_address: &str) -> Result<(ReadHalf<TcpStream>, WriteHalf<TcpStream>)> {
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
                warn!("[CONNECT_TO_MASTER] - Failed to connect to {}: {}.", master_address, e);
                warn!("[CONNECT_TO_MASTER] - Terminating..");
                bail!(e)
            }
            Err(e) => {
                warn!("[CONNECT_TO_MASTER] - Connection attempt to {} timed out: {}.", master_address, e);
                warn!("[CONNECT_TO_MASTER] - Terminating..");
                bail!(e)
            }
        }
    }
    pub async fn send_message(&self, message: &[u8]) -> Result<()> {
        debug!("[SEND_MESSAGE] - START..");
        match &self.role {
            ClientRole::Slave { master_stream_w: master_stream, .. } => match master_stream {
                Some(stream) => {
                    debug!("[SEND_MESSAGE] - As Slave.");
                    let stream = stream.clone();
                    debug!("[SEND_MESSAGE] - MUTEX - Locking Stream: {:?}", stream);
                    let mut lock = stream.lock().await;
                    debug!("[SEND_MESSAGE] - Writing Message to Stream.");
                    lock.write_all(message).await?;

                    // Sleeping is a cheap out instead of awaiting a response from the
                    // counter-party which wouldrequire passing through the read_stream all the way
                    // here from main or a different change in design, which I'm currently very lazy to do.
                    // If I do a refactor later on this is one of the main POIs.
                    sleep(std::time::Duration::new(0, 100_000_000));
        
                    // let mut response = vec![0; 1024];
                    // let resp = Ok(lock.read(&mut response).await?);
                    debug!("[SEND_MESSAGE] - MUTEX - Unlocking Stream: {:?}", stream);
                    debug!("[SEND_MESSAGE] - END.");
                    // resp
                    Ok(())
                }
                None => bail!("Conection not initiated."),
            },
            ClientRole::Master { .. } => {
                debug!("[SEND_MESSAGE] - Can't send message as master."); debug!("[SEND_MESSAGE] - Terminating..");
                bail!("Master role has no master connection")
            }
        }
    }
    pub async fn propagate(&self, message: &[u8]) -> Result<()> {
        debug!("[PROPAGATE] - START");
        let res = match &self.role {
            ClientRole::Master {
                slave_connections, ..
            } => {
                debug!("[PROPAGATE] - MUTEX - Locking Slave Connections: {:?}", slave_connections);
                let connections = slave_connections.lock().await;

                let futures: Vec<_> = connections
                    .iter()
                    .map(|(addr, stream)| async move {
                        debug!("[PROPAGATE] - MUTEX - Locking Stream of {}, Mutex: {:?}", addr, stream);
                        let mut stream = stream.lock().await;
                        debug!("[PROPAGATE] - Writing to Stream.");
                        stream.write_all(message).await
                    })
                    .collect();

                debug!("[PROPAGATE] - Joining and awaiting all connection propagations.");
                futures::future::join_all(futures).await;
                debug!("[PROPAGATE] - MUTEX - Unlocking Stream");
                debug!("[PROPAGATE] - MUTEX - Unlocking Slave Connection: {:?}", slave_connections);
                Ok(())
            }
            ClientRole::Slave { .. } => {
                debug!("[PROPAGATE] - Slave currently can't propagate.");
                debug!("[PROPAGATE] - Terminating..");
                bail!("Slave currently can't propagate")
            },
        };
        debug!("[PROPAGATE] - END");
        res
    }
    pub async fn process_set(
        &mut self,
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
                "px" => {
                    self.store
                        .set(&key.to_string(), value, Some(arg_value))
                        .await
                }
                _ => bail!("unimplemented arg."),
            }
        } else {
            self.store.set(&key.to_string(), value, None).await
        }
    }
}

#[derive(Clone)]
pub enum ClientRole {
    Master {
        replication_id: String,
        replication_offset: usize,
        slave_connections: Arc<Mutex<HashMap<String, Replica>>>,
    },
    Slave {
        master_stream_w: Option<Arc<Mutex<WriteHalf<TcpStream>>>>,
        master_stream_r: Option<Arc<Mutex<ReadHalf<TcpStream>>>>,
        master_address: String,
        master_id: String,
        master_offset: i32,
    },
}

impl ClientRole {
    pub fn psync(&self) -> String {
        debug!("[PSYNC] - Creating psync payload.");
        match self {
            Self::Slave {
                master_id,
                master_offset,
                ..
            } => Payload::build_bulk_string_array(vec![
                "PSYNC",
                master_id,
                &master_offset.to_string(),
            ])
            .redis_encode(),
            Self::Master {
                replication_id,
                replication_offset,
                ..
            } => Payload::SimpleString(format!(
                "FULLRESYNC {} {}",
                replication_id, replication_offset
            ))
            .redis_encode(),
        }
    }
}

fn get_empty_rdb() -> Vec<u8> {
    let empty_rdb_data = hex!("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_handshake_slave_timeout() {
        let mut client = RedisClient {
            store: KeyValueStore::new(),
            role: ClientRole::Slave {
                master_stream_w: None,
                master_stream_r: None,
                master_address: "127.0.0.1:9999".to_string(),
                master_id: "?".to_string(),
                master_offset: -1,
            },
        };

        assert!(client.handshake().await.is_err());
    }

    #[tokio::test]
    async fn test_handshake_master_role() {
        let mut client = RedisClient {
            store: KeyValueStore::new(),
            role: ClientRole::Master {
                slave_connections: Arc::new(Mutex::new(HashMap::new())),
                replication_id: "some_id".to_string(),
                replication_offset: 0,
            },
        };

        assert!(client.handshake().await.is_ok());
    }
}
