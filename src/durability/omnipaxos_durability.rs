use omnipaxos::macros::Entry;
use omnipaxos::OmniPaxos;
use omnipaxos::util::LogEntry::Decided;
use super::*;

use omnipaxos_storage::memory_storage::MemoryStorage;


/// Every transaction in the omnipaxos sequencepaxos log, aka all mutable transactions (TxResult from datastore)
#[derive(Debug, Clone, Entry)]
pub struct Transaction {
    pub offset: TxOffset,
    pub data: TxData
}
/// OmniPaxosDurability is a OmniPaxos node that should provide the replicated
/// implementation of the DurabilityLayer trait required by the Datastore.
pub struct OmniPaxosDurability {
    // TODO
    pub omni_paxos: OmniPaxos<Transaction, MemoryStorage<Transaction>>
}

impl DurabilityLayer for OmniPaxosDurability {
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        let mut it = self.omni_paxos.read_entries(..).unwrap_or_default();
        let entries = it.into_iter().map(|entry| {
            match entry {
                Decided(transaction) => (transaction.offset, transaction.data),
                _ => (),
            }
        }).collect();
        entries
    }

    fn iter_starting_from_offset(
        &self,
        offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        self.omni_paxos.read_entries(offset..)
            .unwrap_or_default()
            .into_iter()
            .map(|entry| {
                match entry {
                    Decided(transaction) => (transaction.offset, transaction.data),
                    _ => (),
                }
            }).collect()
    }

    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        self.omni_paxos.append(
            Transaction{
                offset: tx_offset,
                data: tx_data
            }).expect("Failed to append transaction in omnipaxos");
    }

    fn get_durable_tx_offset(&self) -> TxOffset {
        TxOffset(self.omni_paxos.get_decided_idx())
    }
}
