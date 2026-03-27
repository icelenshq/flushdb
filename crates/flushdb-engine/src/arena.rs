const DEFAULT_BLOCK_SIZE: usize = 1_048_576; // 1 MB

#[derive(Debug, Clone, Copy)]
pub struct ArenaSlice {
    block: usize,
    offset: usize,
    len: usize,
}

impl ArenaSlice {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

pub struct Arena {
    blocks: Vec<Vec<u8>>,
    current_offset: usize,
    total_allocated: usize,
    block_size: usize,
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

impl Arena {
    pub fn new() -> Self {
        Self {
            blocks: vec![vec![0u8; DEFAULT_BLOCK_SIZE]],
            current_offset: 0,
            total_allocated: 0,
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }

    pub fn with_block_size(block_size: usize) -> Self {
        assert!(block_size > 0, "block_size must be greater than 0");
        Self {
            blocks: vec![vec![0u8; block_size]],
            current_offset: 0,
            total_allocated: 0,
            block_size,
        }
    }

    pub fn allocate(&mut self, size: usize) -> ArenaSlice {
        if self.current_offset + size <= self.current_block_capacity() {
            let block = self.blocks.len() - 1;
            let offset = self.current_offset;
            self.current_offset += size;
            self.total_allocated += size;
            ArenaSlice {
                block,
                offset,
                len: size,
            }
        } else {
            let new_block_size = self.block_size.max(size);
            self.blocks.push(vec![0u8; new_block_size]);
            self.current_offset = size;
            self.total_allocated += size;
            let block = self.blocks.len() - 1;
            ArenaSlice {
                block,
                offset: 0,
                len: size,
            }
        }
    }

    pub fn write(&mut self, slice: &ArenaSlice, data: &[u8]) {
        assert!(
            data.len() <= slice.len,
            "data length {} exceeds allocated slice length {}",
            data.len(),
            slice.len
        );
        self.blocks[slice.block][slice.offset..slice.offset + data.len()].copy_from_slice(data);
    }

    pub fn read(&self, slice: &ArenaSlice) -> &[u8] {
        &self.blocks[slice.block][slice.offset..slice.offset + slice.len]
    }

    pub fn total_allocated(&self) -> usize {
        self.total_allocated
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub fn reset(&mut self) {
        self.blocks.clear();
        self.blocks.push(vec![0u8; self.block_size]);
        self.current_offset = 0;
        self.total_allocated = 0;
    }

    fn current_block_capacity(&self) -> usize {
        self.blocks.last().map_or(0, |b| b.len())
    }
}
