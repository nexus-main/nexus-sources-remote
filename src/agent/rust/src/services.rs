use log::{debug, info};
use nexus_remoting::RemoteCommunicator;
use smol::io::AsyncReadExt;
use smol::net::{TcpListener, TcpStream};
use smol::{spawn, Task, Timer};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

struct TcpClientPair {
    comm_stream: Option<TcpStream>,
    data_stream: Option<TcpStream>,
    remote_communicator: Option<RemoteCommunicator>,
    watchdog_timer: Instant,
    task: Option<Task<()>>
}

impl TcpClientPair {
    fn new() -> Self {
        Self {
            comm_stream: None,
            data_stream: None,
            remote_communicator: None,
            watchdog_timer: Instant::now(),
            task: None
        }
    }
}

pub struct AgentService {
    extension_hive: ExtensionHive,
    tcp_client_pairs: Arc<Mutex<HashMap<Uuid, TcpClientPair>>>,
    json_rpc_listen_address: String,
    json_rpc_listen_port: u16,
    client_timeout: Duration
}

impl AgentService {
    pub fn new(
        extension_hive: ExtensionHive,
        json_rpc_listen_address: String,
        json_rpc_listen_port: u16,
    ) -> Self {
        Self {
            extension_hive,
            tcp_client_pairs: Arc::new(Mutex::new(HashMap::new())),
            json_rpc_listen_address,
            json_rpc_listen_port,
            client_timeout: Duration::from_secs(60)
        }
    }

    pub async fn accept_clients(&self) -> smol::io::Result<()> {
        let tcp_client_pairs = self.tcp_client_pairs.clone();
        let client_timeout = self.client_timeout;

        // Spawn a task to detect and remove inactive clients
        spawn(async move {
            loop {
                Timer::after(Duration::from_secs(600)).await;

                let mut pairs = tcp_client_pairs.lock().unwrap();
                let now = Instant::now();

                pairs.retain(|_, pair| {
                    let watchdog_timer_elapsed = now.duration_since(pair.watchdog_timer);

                    let is_dead = 
                        (pair.comm_stream.is_none() || pair.data_stream.is_none()) && watchdog_timer_elapsed >= client_timeout ||
                        pair.remote_communicator.as_ref().is_some_and(|x| x.last_communication() >= client_timeout);

                    if is_dead {
                        if let Some(task) = pair.task.take() {
                            task.cancel();
                        }
                    }

                    !is_dead
                });
            }
        }).detach();

        // Start the TCP server
        let listener = TcpListener::bind((&self.json_rpc_listen_address[..], self.json_rpc_listen_port)).await?;

        info!(
            "Listening for JSON-RPC communication on {}:{}",
            self.json_rpc_listen_address, self.json_rpc_listen_port
        );

        loop {
            let (stream, _) = listener.accept().await?;
            let tcp_client_pairs = self.tcp_client_pairs.clone();

            smol::spawn(async move {
                if let Err(e) = self.handle_client(stream, tcp_client_pairs).await {
                    eprintln!("Error handling client: {}", e);
                }
            })
            .detach();
        }
    }

    async fn handle_client(
        &self,
        stream: TcpStream,
        tcp_client_pairs: Arc<Mutex<HashMap<Uuid, TcpClientPair>>>,
    ) -> smol::io::Result<()> {
        let mut reader = stream.clone();
        let writer = stream;

        // Get connection id
        let mut buffer = vec![0u8; 36];

        reader.read_exact(&mut buffer).await?;

        let id_string = String::from_utf8(buffer).unwrap();
        let id = Uuid::parse_str(&id_string).unwrap();

        // Read connection type
        let mut buffer = vec![0u8; 4];

        reader.read_exact(&mut buffer).await?;

        let type_string = String::from_utf8(buffer).unwrap();

        debug!("Accept TCP client with connection ID {id_string} and communication type {type_string}");

        let mut pairs = tcp_client_pairs.lock().unwrap();
        let pair = pairs.entry(id).or_insert_with(TcpClientPair::new);

        match type_string.as_str() {
            "comm" => {
                pair.comm_stream = Some(writer);
            }
            "data" => {
                pair.data_stream = Some(writer);
            }
            _ => {
                // todo!("Close the socket!! (writer.close())")
                return Err(smol::io::Error::new(
                    smol::io::ErrorKind::InvalidData,
                    "Unknown connection type",
                ));
            }
        }

        if pair.comm_stream.is_some() && pair.data_stream.is_some() && pair.remote_communicator.is_none() {
            debug!("Accept remoting client with connection ID {id}");

            let comm_stream = pair.comm_stream.take().unwrap();
            let data_stream = pair.data_stream.take().unwrap();
            let get_data_source_type = |type_name: &str| self.extension_hive.get_extension_type(type_name);

            pair.remote_communicator = Some(RemoteCommunicator::new(comm_stream, data_stream, get_data_source_type));
            pair.remote_communicator.as_ref().unwrap().run().await?;
        }

        Ok(())
    }
}