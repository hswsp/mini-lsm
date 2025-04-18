// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{ops::Bound, sync::Arc};

use crate::{
    iterators::StorageIterator,
    lsm_storage::{LsmStorageInner, MiniLsm},
    mem_table::MemTable,
};

impl LsmStorageInner {
    pub fn dump_structure(&self) {
        let snapshot = self.state.read();
        if !snapshot.l0_sstables.is_empty() {
            println!(
                "L0 ({}): {:?}",
                snapshot.l0_sstables.len(),
                snapshot.l0_sstables,
            );
        }
        for (level, files) in &snapshot.levels {
            println!("L{level} ({}): {:?}", files.len(), files);
        }
    }

    /// Print debug information about memtables and their WALs
    pub fn dump_memtables(&self) {
        let state = self.state.read();
        println!("------------- print memtables: -----------");

        // Helper function to print memtable contents
        let print_memtable = |memtable: &Arc<MemTable>, is_active: bool| {
            if is_active && memtable.is_empty() {
                return;
            }

            println!(
                "  - {} WAL {}:",
                if is_active { "Active" } else { "Immutable" },
                memtable.id()
            );

            let mut iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
            let mut count = 0;
            while iter.is_valid() {
                count += 1;
                println!(
                    "    {:?} -> {:?}",
                    String::from_utf8_lossy(iter.key().into_inner()),
                    String::from_utf8_lossy(iter.value())
                );
                let _ = iter.next();
            }
            println!("    Total {} key-value pairs", count);
        };
        // Print active memtable
        print_memtable(&state.memtable, true);

        // Print immutable memtables
        for memtable in &state.imm_memtables {
            print_memtable(memtable, false);
        }

        println!(
            "Total {} memtables ",
            state.imm_memtables.len() + (!state.memtable.is_empty() as usize)
        );
    }
}

impl MiniLsm {
    pub fn dump_structure(&self) {
        self.inner.dump_structure()
    }
}
