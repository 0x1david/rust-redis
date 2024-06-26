# IN PROGRESS 

## Miniature Redis Implementation in Rust

### Overview

This project is a miniature implementation of a Redis-like database server written entirely in Rust. The implementation focuses on compatibility with the Redis protocol, enabling basic interactions typical of Redis operations but within a simplified scope. This project serves as a learning tool to understand the workings of key-value stores and the intricacies of the Redis protocol.

### Current Features

- **Redis Protocol Parsing**: The server can parse and understand commands formatted according to the Redis Serialization Protocol (RESP). This includes decoding different data types like simple strings, bulk strings, arrays, and handling basic commands.

- **Basic Command Support**: Implements a subset of common Redis commands:
  - `GET`: Retrieve the value associated with a key.
  - `SET`: Set the specified value for a key, storing the value in the database.
  - `PING`: Check the connection to the server; responds with "PONG".
  - `ECHO`: Return a copy of the given string to the client.
  - `INFO`: Obtain detailed information and statistics about the server in a human-readable format.
  - `TYPE`: Determine the type of a specific key (e.g., string, list, set).
  - `REPLICAOF`: Configure the server to be a replica of another specified server.
  - `REPLCONF`: Used during replication to acknowledge the processing of commands from the master.
  - `PSYNC`: Used to synchronize data between master and replica servers, initiating partial or full resynchronization as needed.

- **Replication**: Basic replication features are supported, allowing this server to act as a slave that can replicate data from a designated master server. This is pivotal for scenarios where data backup or read scalability is needed.

### In Progress Features

- **Reverse Replication**: Enhancing the replication feature to allow a slave to also serve as a master to other replicas. This feature is in progress and aims to support more complex replication topologies.

- **Additional Commands and Features**: Plans to support more commands and features typical of a full Redis implementation, such as handling lists, sets, sorted sets, and advanced key management features.

### How to Use

To get started with this Redis server:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/0x1david/rust-redis
   cd rust-redis
   ```
2. **Build the project**:
   ```bash
   cargo build --release
   ```
3. **Run the server**:
   ```bash
   ./target/release/rust-redis
   ```
4. **Connect to the server using a Redis Client**:
   ```bash
   redis-cli -p 6379
   ```
