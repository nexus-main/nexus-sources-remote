mod options;
mod services;

use env_logger::{Builder, Env};
use io::Result;
use options::Options;
use smol::io;

fn main() -> Result<()> {

    let env = Env::new().filter_or("RUST_LOG", "info");
    Builder::from_env(env).init();

    let json_rpc_listen_address = Options::json_rpc_listen_address();
    let json_rpc_listen_port = Options::json_rpc_listen_port();

    // let agent_service = AgentService {
    //     extension_hive,
    //     package_service,
    //     json_rpc_listen_address,
    //     json_rpc_listen_port
    // }

    smol::block_on(async {


        // agent_service.load_packages().await?;
        // agent_service.accept_clients().await?;

        Ok(())
    })
}