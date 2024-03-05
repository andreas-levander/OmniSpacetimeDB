pub mod datastore;
pub mod durability;
pub mod node;

use std::collections::HashMap;
use std::error::Error;
use omnipaxos::messages::Message;
use omnipaxos::util::NodeId;
use tokio::sync::mpsc;
use crate::durability::omnipaxos_durability::{OmniPaxosDurability, Transaction};
use tokio::sync::{Mutex as AsyncMutex};
use std::sync::{Arc, Mutex};
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio::task::JoinHandle;
use crate::node::{Node, NodeRunner};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    // just for testing
    spawn_nodes();
    loop {}
    Ok(())
}

const SERVERS: [NodeId; 3] = [1, 2, 3];
#[allow(clippy::type_complexity)]
fn initialise_channels() -> (
    HashMap<NodeId, mpsc::Sender<Message<Transaction>>>,
    HashMap<NodeId, AsyncMutex<mpsc::Receiver<Message<Transaction>>>>,
) {
    let mut senders: HashMap<NodeId, mpsc::Sender<Message<Transaction>>> = HashMap::new();
    let mut receivers: HashMap<NodeId, AsyncMutex<mpsc::Receiver<Message<Transaction>>>> = HashMap::new();
    for s in SERVERS {
        let (tx, mut rx) = mpsc::channel::<Message<Transaction>>(10);
        senders.insert(s, tx);
        receivers.insert(s, AsyncMutex::new(rx));
    }
    (senders, receivers)
}



fn spawn_nodes() {
    let mut nodes = HashMap::new();
    let (sender_channels, mut receiver_channels) = initialise_channels();
    let senders = Arc::new(sender_channels);
    let receivers = Arc::new(receiver_channels);
    for pid in SERVERS {

        let server_config = ServerConfig {
            pid,
            election_tick_timeout: 5,
            ..Default::default()
        };
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: Vec::from(SERVERS),
            ..Default::default()
        };
        let op_config = OmniPaxosConfig {
            server_config,
            cluster_config,
        };
        let omni_paxos = op_config
            .build(MemoryStorage::default())
            .expect("failed to build OmniPaxos");

        let node = Arc::new(Mutex::new(Node::new(
            pid,
            OmniPaxosDurability{omni_paxos},
        )));

        let mut runner = NodeRunner{
            node: node.clone(),
            senders: senders.clone(),
            receivers: receivers.clone(),
            node_id: pid,
        };

        let handle = tokio::task::spawn(async move {
            runner.run().await;

        });
        nodes.insert(pid, (node, handle));
    }
}


