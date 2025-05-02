use nexus_extensibility::extensibility::{LogLevel, Logger};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use smol::io::{AsyncReadExt, AsyncWriteExt};
use smol::net::TcpStream;
use std::io::Result;
use std::time::{Duration, Instant};

trait JsonRpcMessage : Serialize {}

#[derive(Serialize, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    method: String,
    params: Option<Value>,
    id: Option<u64>,
}

impl JsonRpcMessage for JsonRpcRequest {}

#[derive(Serialize, Deserialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    result: Option<Value>,
    error: Option<JsonRpcError>,
    id: Option<u64>,
}

impl JsonRpcMessage for JsonRpcResponse {}

#[derive(Serialize, Deserialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

struct MyLogger {
    comm_stream: TcpStream,
}

impl Logger for MyLogger {
    fn log(&self, log_level: LogLevel, message: &str) {
        let notification = json!({
            "jsonrpc": "2.0",
            "method": "log",
            "params": [log_level, message]
        });


        // smol::spawn(async move {
        //     let mut writer = writer.lock().unwrap();
        //     send_to_server(&notification, &mut *writer).await.unwrap();
        // }).detach();
    }
}

/// A remote communicator.
pub struct RemoteCommunicator {
    comm_stream: TcpStream,
    data_stream: TcpStream,
    watchdog_timer: Instant,
    logger: MyLogger,
}

impl RemoteCommunicator {
    /// Initializes a new instance of the RemoteCommunicator.
    pub fn new(
        comm_stream: TcpStream,
        data_stream: TcpStream,
    ) -> Self {
        let logger = MyLogger { 
            comm_stream: comm_stream.clone()
        };

        Self {
            comm_stream,
            data_stream,
            watchdog_timer: Instant::now(),
            logger,
        }
    }

    /// Returns the duration since the last communication.
    pub fn last_communication(&self) -> Duration {
        Instant::now().duration_since(self.watchdog_timer)
    }

    /// Starts the remoting operation.
    pub async fn run(&mut self) -> Result<()> {
        loop {
            let size = self.read_size().await?;
            let mut buffer = vec![0; size as usize];

            self.comm_stream.read_exact(&mut buffer).await?;

            let request = serde_json::from_slice::<JsonRpcRequest>(&buffer)?;
            let response = self.process_invocation(request).await;

            send_to_server(response, &mut self.comm_stream).await?;
        }
        // todo!("Timeout for stream reading!!"); // https://doc.rust-lang.org/std/net/struct.TcpStream.html#method.write_timeout
    }

    async fn process_invocation(
        &self,
        request: JsonRpcRequest,
    ) -> JsonRpcResponse {
        let method_name = request.method.as_str();
        let params = request.params.unwrap_or_default();

        match method_name {
            "initialize" => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                result: Some(json!(1)), // API version
                error: None,
                id: request.id,
            },
            _ => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                result: None,
                error: Some(JsonRpcError {
                    code: -1,
                    message: format!("Unknown method '{}'", method_name),
                }),
                id: request.id,
            },
        }
    }

    async fn read_size(&mut self) -> Result<u32> {
        let mut size_buffer = [0u8; 4];

        self.comm_stream
            .read_exact(&mut size_buffer).await?;

        Ok(u32::from_be_bytes(size_buffer))
    }
}

async fn send_to_server(message: impl JsonRpcMessage, writer: &mut TcpStream) -> Result<()> {
    let encoded = serde_json::to_vec(&message)?;
    let size = (encoded.len() as u32).to_be_bytes();

    writer.write_all(&size).await?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;

    Ok(())
}