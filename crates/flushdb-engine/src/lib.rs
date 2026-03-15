pub mod arena;
pub mod memtable;
pub mod memtable_list;
pub mod skiplist;
pub mod sstable;
mod range_tombstone;

pub use memtable::{DedupSet, Memtable, MemtableConfig};
pub use memtable_list::MemtableList;
pub use range_tombstone::{RangeTombstone, RangeTombstoneIndex};
pub use skiplist::{SkipListIntoIterator, SkipListIterator, SkipNode};

#[cfg(test)]
mod send_assertions {
    use super::*;

    fn _assert_send<T: Send>() {}

    fn _assert_types_are_send() {
        _assert_send::<Memtable>();
        _assert_send::<MemtableList>();
        _assert_send::<SkipNode>();
        _assert_send::<RangeTombstone>();
        _assert_send::<RangeTombstoneIndex>();
        _assert_send::<DedupSet>();
    }
}
