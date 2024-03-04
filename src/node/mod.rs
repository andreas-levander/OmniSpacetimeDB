use crate::datastore::error::DatastoreError;
use crate::datastore::example_datastore::{ExampleDatastore, Tx};
use crate::datastore::tx_data::TxResult;
use crate::datastore::*;
use crate::durability::omnipaxos_durability::OmniPaxosDurability;
use crate::durability::{DurabilityLayer, DurabilityLevel};
use omnipaxos::messages::*;
use omnipaxos::util::NodeId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use omnipaxos::macros::Entry;
use omnipaxos::messages::sequence_paxos::PaxosMessage;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::time;
use tokio::time::{Duration};
use tokio::sync::{Mutex as AsyncMutex};

#[derive(Clone, Debug)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Entry)]
pub enum KVCommand {
    Put(KeyValue),
    Delete(String),
    Get(String),
}


pub struct NodeRunner {
    pub node: Arc<Mutex<Node>>,
    senders: Arc<HashMap<NodeId, mpsc::Sender<Message<KVCommand>>>>,
    receivers: Arc<HashMap<NodeId, AsyncMutex<mpsc::Receiver<Message<KVCommand>>>>>,
    node_id : NodeId
    // TODO Messaging and running
}

impl NodeRunner {
    async fn send_outgoing_msgs(&mut self) {
        let msgs = self.node.lock().unwrap().omni_durability.omni_paxos.outgoing_messages();
        for msg in msgs {
            println!("Send message {:?}", msg);
            let recipient = msg.get_receiver();
            let channel = self.senders.get(&recipient).unwrap();
            channel.clone().send(msg).await.expect("Failed to send message to channel");
        }
    }

    async fn process_incoming_msgs(&mut self) {
        let mut rx = self.receivers.get(&self.node_id).unwrap().lock().await;
        while let Some(message) = rx.recv().await {
            println!("GOT = {:?}", message);
        }
    }

    async fn handle_decided_entries(&mut self) {
        let mut n = self.node.lock().unwrap();
        if n.omni_durability.get_durable_tx_offset() > n.last_decided_index {
            let _ = n.advance_replicated_durability_offset();
            n.last_decided_index = n.omni_durability.get_durable_tx_offset();
        }
    }

    pub async fn run(&mut self) {
        println!("Running node: {}", self.node_id);
        // setting up functions to run every interval
        let mut msg_interval = time::interval(Duration::from_millis(1));
        let mut tick_interval = time::interval(Duration::from_millis(10));
        loop {
            tokio::select! {
                biased;
                _ = msg_interval.tick() => {
                    self.process_incoming_msgs().await;
                    self.send_outgoing_msgs().await;
                    self.handle_decided_entries().await;
                },
                _ = tick_interval.tick() => {
                    self.node.lock().unwrap().omni_durability.omni_paxos.tick();
                },
                else => (),
            }
        }
    }
}

pub struct Node {
    node_id: NodeId, // Unique identifier for the node
    omni_durability: OmniPaxosDurability,
    datastore: ExampleDatastore,
    last_decided_index: TxOffset
                     // TODO Datastore and OmniPaxosDurability

}

impl Node {
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability) -> Self {
        Node {
            node_id,
            omni_durability,
            datastore: ExampleDatastore::new(),
            last_decided_index: TxOffset(0)
        }

    }

    /// update who is the current leader. If a follower becomes the leader,
    /// it needs to apply any unapplied txns to its datastore.
    /// If a node loses leadership, it needs to rollback the txns committed in
    /// memory that have not been replicated yet.
    pub fn update_leader(&mut self) {
        todo!()
    }

    /// Apply the transactions that have been decided in OmniPaxos to the Datastore.
    /// We need to be careful with which nodes should do this according to desired
    /// behavior in the Datastore as defined by the application.
    fn apply_replicated_txns(&mut self) {
        todo!()
    }

    pub fn begin_tx(&self, durability_level: DurabilityLevel, ) -> <ExampleDatastore as Datastore<String, String>>::Tx {
        let tx = self.datastore.begin_tx(durability_level);
        todo!()
    }

    pub fn release_tx(&self, tx: <ExampleDatastore as Datastore<String, String>>::Tx) {
        self.datastore.release_tx(tx);
        todo!()
    }

    /// Begins a mutable transaction. Only the leader is allowed to do so.
    pub fn begin_mut_tx(
        &self,
    ) -> Result<<ExampleDatastore as Datastore<String, String>>::MutTx, DatastoreError> {
        todo!()
    }

    /// Commits a mutable transaction. Only the leader is allowed to do so.
    pub fn commit_mut_tx(
        &mut self,
        tx: <ExampleDatastore as Datastore<String, String>>::MutTx,
    ) -> Result<TxResult, DatastoreError> {
        todo!()
    }

    fn advance_replicated_durability_offset(&self, ) -> Result<(), crate::datastore::error::DatastoreError> {
        self.datastore.advance_replicated_durability_offset(self.omni_durability.get_durable_tx_offset())
    }
}

/// Your test cases should spawn up multiple nodes in tokio and cover the following:
/// 1. Find the leader and commit a transaction. Show that the transaction is really *chosen* (according to our definition in Paxos) among the nodes.
/// 2. Find the leader and commit a transaction. Kill the leader and show that another node will be elected and that the replicated state is still correct.
/// 3. Find the leader and commit a transaction. Disconnect the leader from the other nodes and continue to commit transactions before the OmniPaxos election timeout.
/// Verify that the transaction was first committed in memory but later rolled back.
/// 4. Simulate the 3 partial connectivity scenarios from the OmniPaxos liveness lecture. Does the system recover? *NOTE* for this test you may need to modify the messaging logic.
///
/// A few helper functions to help structure your tests have been defined that you are welcome to use.
#[cfg(test)]
mod tests {
    use crate::node::*;
    use omnipaxos::messages::Message;
    use omnipaxos::util::NodeId;
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::sync::{Arc, Mutex};
    use std::thread::sleep;
    use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
    use omnipaxos_storage::memory_storage::MemoryStorage;
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    const SERVERS: [NodeId; 3] = [1, 2, 3];

    #[allow(clippy::type_complexity)]
    fn initialise_channels() -> (
        HashMap<NodeId, mpsc::Sender<Message<KVCommand>>>,
        HashMap<NodeId, AsyncMutex<mpsc::Receiver<Message<KVCommand>>>>,
    ) {
        let mut senders: HashMap<NodeId, mpsc::Sender<Message<KVCommand>>> = HashMap::new();
        let mut receivers: HashMap<NodeId, AsyncMutex<mpsc::Receiver<Message<KVCommand>>>> = HashMap::new();
        for s in SERVERS {
            let (tx, mut rx) = mpsc::channel::<Message<KVCommand>>(10);
            senders.insert(s, tx);
            receivers.insert(s, AsyncMutex::new(rx));
        }
        (senders, receivers)
    }

    fn create_runtime() -> Runtime {
        Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    }

    fn spawn_nodes() -> HashMap<NodeId, (Arc<Mutex<Node>>, JoinHandle<()>)> {
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

            let handle = tokio::spawn(async move {
                runner.run().await;

            });
            nodes.insert(pid, (node, handle));
        }
        nodes
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn basic_test() {
        let nodes = spawn_nodes();
        println!("test: basic test running");
        for (key, value) in &nodes {
            println!("{}:", key);
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(nodes.len(), 3);
    }
}
