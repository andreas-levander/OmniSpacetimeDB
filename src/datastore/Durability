use super::*;

/// OmniPaxosDurability is an OmniPaxos node that provides the replicated
/// implementation of the DurabilityLayer trait required by the Datastore.
pub struct OmniPaxosDurability {
    node_id: u64, // Unique identifier for each node
    nodes: Vec<Sender<Message>>, // Senders for communicating with other nodes
    transaction_log: Arc<Mutex<TransactionLog>>, // Shared transaction log
    durable_tx_offset: Arc<Mutex<Option<TxOffset>>>, // Durable transaction offset
}

impl OmniPaxosDurability {
    // Constructor
    pub fn new(node_id: u64, nodes: Vec<Sender<Message>>) -> Self {
        OmniPaxosDurability {
            node_id,
            nodes,
            transaction_log: Arc::new(Mutex::new(Vec::new())),
            durable_tx_offset: Arc::new(Mutex::new(None)),
        }
    }

    // Method for starting the durability layer
    pub async fn start(mut self) {
        // Spawn async tasks for handling messages
        let mut receiver = self.nodes[self.node_id as usize].subscribe();
        while let Some(message) = receiver.recv().await {
            match message {
                Message::Prepare(ballot_number, _) => {
                    self.handle_prepare(ballot_number).await;
                }
                Message::Promise(ballot_number, _) => {
                    self.handle_promise(ballot_number).await;
                }
                Message::Accept(ballot_number, _) => {
                    self.handle_accept(ballot_number).await;
                }
                Message::Learn(log) => {
                    self.handle_learn(log).await;
                }
            }
        }
    }

    // Method for handling prepare phase
    async fn handle_prepare(&mut self, ballot_number: u64) {
        let current_ballot_number = self.durable_tx_offset.lock().unwrap().map(|offset| offset.0).unwrap_or(0);
        
        if ballot_number > current_ballot_number {
            let promise_message = Message::Promise(ballot_number, self.transaction_log.lock().unwrap().clone());
            self.nodes[self.node_id as usize].send(promise_message).await.unwrap();
        }
    }

    // Method for handling promise phase
    async fn handle_promise(&mut self, ballot_number: u64) {
        let current_ballot_number = self.durable_tx_offset.lock().unwrap().map(|offset| offset.0).unwrap_or(0);
        
        if ballot_number > current_ballot_number {
            let accept_message = Message::Accept(ballot_number, self.transaction_log.lock().unwrap().clone());
            self.nodes[self.node_id as usize].send(accept_message).await.unwrap();
        }
    }

    // Method for handling accept phase
    async fn handle_accept(&mut self, ballot_number: u64) {
        let current_ballot_number = self.durable_tx_offset.lock().unwrap().map(|offset| offset.0).unwrap_or(0);
        
        if ballot_number >= current_ballot_number {
            let learn_message = Message::Learn(self.transaction_log.lock().unwrap().clone());
            self.nodes[self.node_id as usize].send(learn_message).await.unwrap();
        }
    }

    // Method for handling learn phase
    async fn handle_learn(&mut self, log: TransactionLog) {
        *self.transaction_log.lock().unwrap() = log;
    }
}

impl DurabilityLayer for OmniPaxosDurability {
    // Method for iterating over transactions
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        // Placeholder implementation: return empty iterator
        Box::new(std::iter::empty())
    }

    // Method for iterating over transactions starting from a specific offset
    fn iter_starting_from_offset(
        &self,
        offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        // Placeholder implementation: return empty iterator
        Box::new(std::iter::empty())
    }

    // Method for appending a transaction to the log
    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        // Placeholder implementation: append transaction to the log
        let mut log = self.transaction_log.lock().unwrap();
        log.push(format!("{}: {:?}", tx_offset.0, tx_data));
    }

    // Method for retrieving the durable transaction offset
    fn get_durable_tx_offset(&self) -> TxOffset {
        // Placeholder implementation: return the current durable transaction offset
        let offset = self.durable_tx_offset.lock().unwrap().unwrap_or(TxOffset(0));
        offset
    }
}
