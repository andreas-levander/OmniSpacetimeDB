use super::*;

/// OmniPaxosDurability is an OmniPaxos node that provides the replicated
/// implementation of the DurabilityLayer trait required by the Datastore.
pub struct OmniPaxosDurability {
    omni_paxos: Arc<Mutex<OmniPaxos<KVCommand, MemoryStorage<KVCommand>>>>,
}

impl OmniPaxosDurability {
    /// Create a new instance of OmniPaxosDurability
    pub fn new() -> Self {
        // Initialize OmniPaxos instance with MemoryStorage
        let omni_paxos = OmniPaxos::new(MemoryStorage::new());
        Self {
            omni_paxos: Arc::new(Mutex::new(omni_paxos)),
        }
    }
}

impl DurabilityLayer for OmniPaxosDurability {
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        // Acquire lock and read entries from OmniPaxos storage
        let omni_paxos = self.omni_paxos.lock().expect("Mutex was poisoned");
        let entries = omni_paxos.read_entries(..).unwrap_or_else(Vec::new);
        
        // Convert entries into iterator of (TxOffset, TxData) tuples
        Box::new(entries.into_iter().map(|entry| (entry.offset as TxOffset, entry.data)))
    }

    fn iter_starting_from_offset(
        &self,
        offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        // Acquire lock and read entries starting from given offset
        let omni_paxos = self.omni_paxos.lock().expect("Mutex was poisoned");
        let entries = omni_paxos.read_entries(offset..).unwrap_or_else(Vec::new);
        
        // Convert entries into iterator of (TxOffset, TxData) tuples
        Box::new(entries.into_iter().map(|entry| (entry.offset as TxOffset, entry.data)))
    }

    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        // Acquire lock and append entry to OmniPaxos storage
        let mut omni_paxos = self.omni_paxos.lock().expect("Mutex was poisoned");
        omni_paxos.append_entry((tx_offset, tx_data));
    }

    fn get_durable_tx_offset(&self) -> TxOffset {
        // Acquire lock and get last applied offset from OmniPaxos
        let omni_paxos = self.omni_paxos.lock().expect("Mutex was poisoned");
        omni_paxos.get_last_applied() as TxOffset
    }
}
