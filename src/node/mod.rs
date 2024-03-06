use crate::datastore::error::DatastoreError;
use crate::datastore::example_datastore::{ExampleDatastore, MutTx, Tx};
use crate::datastore::tx_data::TxResult;
use crate::datastore::*;
use crate::durability::omnipaxos_durability::OmniPaxosDurability;
use crate::durability::{DurabilityLayer, DurabilityLevel};
use crate::durability::omnipaxos_durability::Transaction;
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
    pub senders: Arc<HashMap<NodeId, mpsc::Sender<Message<Transaction>>>>,
    pub receivers: Arc<HashMap<NodeId, AsyncMutex<mpsc::Receiver<Message<Transaction>>>>>,
    pub node_id : NodeId
}

impl NodeRunner {
    async fn send_outgoing_msgs(&mut self) {
        //println!("{} sending outgoing messages", self.node_id);
        let msgs = self.node.lock().unwrap().omni_durability.omni_paxos.outgoing_messages();
        for msg in msgs {
            //println!("Send message {:?}", msg);
            let recipient = msg.get_receiver();
            let channel = self.senders.get(&recipient).unwrap();
            channel.clone().send(msg).await.expect("Failed to send message to channel");
        }
    }

    async fn process_incoming_msgs(&mut self) {
        //println!("{} processing incomming messages", self.node_id);
        let mut rx = self.receivers.get(&self.node_id).unwrap().lock().await;

        while let Ok(message) = rx.try_recv() {
            //println!("Id: {} GOT = {:?}", self.node_id, message);
            self.node.lock().unwrap().omni_durability.omni_paxos.handle_incoming(message);
        }

    }

    async fn handle_decided_entries(&mut self) {


        let mut n = self.node.lock().unwrap();

        if n.leader.is_some() && n.omni_durability.get_durable_tx_offset().0 != 0 && (n.last_decided_index.is_none() || n.omni_durability.get_durable_tx_offset() > n.last_decided_index.unwrap()) {
            println!("{} - handling decided entries paxos durable offset: {:?}, last decided: {:?}", self.node_id, n.omni_durability.get_durable_tx_offset(), n.last_decided_index);
            // if we are a leader
            if n.leader.unwrap() == n.node_id {
                let _ = n.advance_replicated_durability_offset();
                n.last_decided_index = Some(n.omni_durability.get_durable_tx_offset());
            }
            // we are a follower
            else {
                n.apply_replicated_txns();
            }
        }
    }

    async fn check_leader_changes(&mut self) {
        //println!("{} checking leader changes", self.node_id);
        let mut n = self.node.lock().unwrap();

        // if there is a leader
        if let Some(new_leader) = n.omni_durability.omni_paxos.get_current_leader() {
            n.update_leader(new_leader);
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
                    self.check_leader_changes().await;
                    self.process_incoming_msgs().await;
                    self.send_outgoing_msgs().await;
                    self.handle_decided_entries().await;
                },
                _ = tick_interval.tick() => {
                    //println!("omnipaxos tick");
                    self.node.lock().unwrap().omni_durability.omni_paxos.tick();
                },
                else => (),
            }
        }
    }
}

pub struct Node {
    node_id: NodeId, // Unique identifier for the node
    pub omni_durability: OmniPaxosDurability,
    datastore: ExampleDatastore,
    last_decided_index: Option<TxOffset>,
    leader: Option<NodeId>
}

impl Node {
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability) -> Self {
        Node {
            node_id,
            omni_durability,
            datastore: ExampleDatastore::new(),
            last_decided_index: None,
            leader: None
        }

    }

    /// update who is the current leader. If a follower becomes the leader,
    /// it needs to apply any unapplied txns to its datastore.
    /// If a node loses leadership, it needs to rollback the txns committed in
    /// memory that have not been replicated yet.
    pub fn update_leader(&mut self, new_leader: NodeId) {
        match self.leader {
            Some(current_leader) => {

                // if there is a new leader
                if (current_leader != new_leader) {
                    // if we were the leader
                    if (current_leader == self.node_id) {
                        self.datastore.rollback_to_replicated_durability_offset().expect("failed to roll back")

                    }
                    // if we become the leader
                    else if (new_leader == self.node_id) {
                        self.apply_replicated_txns()

                    }
                    // we are a follower changing leader
                    self.leader = Some(new_leader);
                    println!("{} swapping leader to: {}", self.node_id, new_leader);

                }
            }
            None => self.leader = Some(new_leader)
        }
    }

    /// Apply the transactions that have been decided in OmniPaxos to the Datastore.
    /// We need to be careful with which nodes should do this according to desired
    /// behavior in the Datastore as defined by the application.
    fn apply_replicated_txns(&mut self) {
        let mut next_offset = None;
        if self.last_decided_index.is_none(){
            next_offset = Some(TxOffset(0));
        }
        else {
            next_offset = Some(self.last_decided_index.unwrap());
        }
        println!("{} - applying replicated transactions from offset: {:?}", self.node_id, next_offset);
        let mut to_apply = self.omni_durability.iter_starting_from_offset(next_offset.unwrap());
        let mut changes = false;
        while let Some(transaction) = to_apply.next() {
            println!("{} - replaying transaction: {:?}", self.node_id, transaction);
            self.datastore.replay_transaction(&transaction.1).expect("Failed to replay transaction");
            changes = true;

        };
        if changes {
            self.last_decided_index = Some(self.omni_durability.get_durable_tx_offset());
        }

    }

    pub fn begin_tx(&self, durability_level: DurabilityLevel, ) -> <ExampleDatastore as Datastore<String, String>>::Tx {
        self.datastore.begin_tx(durability_level)
    }

    pub fn release_tx(&self, tx: <ExampleDatastore as Datastore<String, String>>::Tx) {
        self.datastore.release_tx(tx);
    }

    /// Begins a mutable transaction. Only the leader is allowed to do so.
    pub fn begin_mut_tx(
        &self,
    ) -> Result<<ExampleDatastore as Datastore<String, String>>::MutTx, DatastoreError> {
        Ok(self.datastore.begin_mut_tx())
    }

    /// Commits a mutable transaction. Only the leader is allowed to do so.
    pub fn commit_mut_tx(
        &mut self,
        tx: <ExampleDatastore as Datastore<String, String>>::MutTx,
    ) -> Result<TxResult, DatastoreError> {
        let result = self.datastore.commit_mut_tx(tx).expect("Datastore error");
        self.omni_durability.append_tx(result.tx_offset.clone(), result.tx_data.clone());
        Ok(result)
    }

    fn advance_replicated_durability_offset(&self, ) -> Result<(), crate::datastore::error::DatastoreError> {
        let omni_paxos_durable_offset = TxOffset(self.omni_durability.get_durable_tx_offset().0 - 1);
        println!("{} - advancing replicated durability offset to: {:?}", self.node_id, omni_paxos_durable_offset);
        self.datastore.advance_replicated_durability_offset(omni_paxos_durable_offset)
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
        HashMap<NodeId, mpsc::Sender<Message<Transaction>>>,
        HashMap<NodeId, AsyncMutex<mpsc::Receiver<Message<Transaction>>>>,
    ) {
        let mut senders: HashMap<NodeId, mpsc::Sender<Message<Transaction>>> = HashMap::new();
        let mut receivers: HashMap<NodeId, AsyncMutex<mpsc::Receiver<Message<Transaction>>>> = HashMap::new();
        for s in SERVERS {
            let (tx, mut rx) = mpsc::channel::<Message<Transaction>>(100);
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

    fn get_leader(nodes: &HashMap<NodeId, (Arc<Mutex<Node>>, JoinHandle<()>)>) -> NodeId {
        let first = nodes.get(&1).unwrap();
        let node_1 = (*first).0.lock().unwrap();
        let leader = node_1.omni_durability.omni_paxos.get_current_leader().expect("Leader not found");
        leader
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn basic_test() {
        let nodes = spawn_nodes();
        println!("test: basic test running");
        for (key, value) in &nodes {
            println!("{}:", key);
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(nodes.len(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn replication_works() {
        let nodes = spawn_nodes();

        // wait for a leader to be elected
        tokio::time::sleep(Duration::from_secs(2)).await;
        {
            let leader = get_leader(&nodes);
            println!("leader: {}", leader);

            let leader_arc = nodes.get(&leader).unwrap();
            let mut leader_node = (*leader_arc).0.lock().unwrap();
            let mut t1 = leader_node.begin_mut_tx().unwrap();
            t1.set("test".to_string(), "asd".to_string());
            leader_node.commit_mut_tx(t1).expect("Failure commiting transaction");
            // transaction should be in leader memory
            let t2 = leader_node.begin_tx(DurabilityLevel::Memory);
            assert_eq!(t2.get(&"test".to_string()), Some("asd".to_string()));
            leader_node.release_tx(t2);

        }
        // wait for value to be decided
        tokio::time::sleep(Duration::from_secs(5)).await;

        // value now decided and in replicated storage in leader
        {
            let leader = get_leader(&nodes);
            println!("leader: {}", leader);

            let leader_arc = nodes.get(&leader).unwrap();
            let mut leader_node = (*leader_arc).0.lock().unwrap();
            let t2 = leader_node.begin_tx(DurabilityLevel::Replicated);
            assert_eq!(t2.get(&"test".to_string()), Some("asd".to_string()));
            leader_node.release_tx(t2);
        }

        // value now decided and replicated in all nodes
        {
            for (k, v) in &nodes {
                let node = (*v).0.lock().unwrap();
                let tx = node.begin_tx(DurabilityLevel::Replicated);
                assert_eq!(tx.get(&"test".to_string()), Some("asd".to_string()));
                node.release_tx(tx);
            }
        }

    }
}
