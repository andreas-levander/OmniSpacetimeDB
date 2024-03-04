use omnipaxos::OmniPaxos;
use super::*;

use omnipaxos_storage::memory_storage::MemoryStorage;
use crate::node::KVCommand;


/// OmniPaxosDurability is a OmniPaxos node that should provide the replicated
/// implementation of the DurabilityLayer trait required by the Datastore.
pub struct OmniPaxosDurability {
    // TODO
    pub omni_paxos: OmniPaxos<KVCommand, MemoryStorage<KVCommand>>
}

impl DurabilityLayer for OmniPaxosDurability {
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        let it = self.omni_paxos.read_entries(..).iter();
        todo!()
    }

    fn iter_starting_from_offset(
        &self,
        offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        todo!()
    }

    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        todo!()
    }

    fn get_durable_tx_offset(&self) -> TxOffset {
        todo!()
    }
}
