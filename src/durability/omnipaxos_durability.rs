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
        let filtered: Vec<(TxOffset, TxData)> = self.omni_paxos.read_entries(..)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry|
                match entry {
                    Decided(transaction) => Some((transaction.offset, transaction.data)),
                    _ => None,
                }
            ).collect();
        Box::new(filtered.into_iter())
    }

    fn iter_starting_from_offset(
        &self,
        offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        let filtered: Vec<(TxOffset, TxData)> = self.omni_paxos.read_entries(offset.0..)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry|
                match entry {
                    Decided(transaction) => Some((transaction.offset, transaction.data)),
                    _ => None,
                }
            ).collect();
        println!("{:?} - filtered entries from offset", self.omni_paxos.read_decided_suffix(offset.0));
        println!("{:?} - filtered entries all", self.omni_paxos.read_decided_suffix(0));
        Box::new(filtered.into_iter())
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
