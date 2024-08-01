use std::io;
use std::io::ErrorKind;
use crate::cursor::Cursor;
use crate::data_consts::{PAGE_SIZE, ROW_SIZE};
use crate::Row;

enum NodeType {
    NodeInternal,
    NodeLeaf,
}


/// Common Node Header Layout
const NODE_TYPE_SIZE: usize = std::mem::size_of::<u8>();
const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_SIZE: usize = std::mem::size_of::<u8>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = std::mem::size_of::<u32>();
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;


pub struct LeafNode<'a> {
    pub node: &'a mut [u8],
}

impl<'a> LeafNode<'a> {
    /// Leaf Node Header Layout
    const LEAF_NODE_NUM_CELL_SIZE: usize = std::mem::size_of::<u32>();
    const LEAF_NODE_NUM_CELL_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
    const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + Self::LEAF_NODE_NUM_CELL_SIZE;


    /// Leaf Node Body Layout
    const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
    const LEAF_NODE_KEY_OFFSET: usize = 0;
    const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
    const LEAF_NODE_VALUE_OFFSET: usize = Self::LEAF_NODE_KEY_OFFSET + Self::LEAF_NODE_KEY_SIZE;
    const LEAF_NODE_CELL_SIZE: usize = Self::LEAF_NODE_KEY_SIZE + Self::LEAF_NODE_VALUE_SIZE;
    const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - Self::LEAF_NODE_HEADER_SIZE;
    pub const LEAF_NODE_MAX_CELLS: usize = Self::LEAF_NODE_SPACE_FOR_CELLS / Self::LEAF_NODE_CELL_SIZE;


    pub fn new(node: &'a mut [u8]) -> Self {
        LeafNode { node }
    }


    pub fn leaf_node_num_cells(node: &mut [u8]) -> &mut u32 {
        unsafe {
            &mut *(node.as_ptr().add(Self::LEAF_NODE_NUM_CELL_OFFSET) as *mut u32)
        }
    }


    pub fn leaf_node_num_cells_safe(node: &mut [u8]) -> u32 {
        let num_cells_bytes = &node[Self::LEAF_NODE_NUM_CELL_OFFSET..Self::LEAF_NODE_NUM_CELL_OFFSET + Self::LEAF_NODE_NUM_CELL_SIZE];
        u32::from_le_bytes(num_cells_bytes.try_into().unwrap())
    }


    pub fn leaf_node_cell(node: &mut [u8], cell_num: usize) -> &mut [u8] {
        let offset = Self::LEAF_NODE_HEADER_SIZE + cell_num * Self::LEAF_NODE_CELL_SIZE;
        &mut node[offset..offset + Self::LEAF_NODE_CELL_SIZE]
    }


    pub fn leaf_node_key(node: &mut [u8], cell_num: usize) -> &mut [u8] {
        &mut Self::leaf_node_cell(node, cell_num)[..Self::LEAF_NODE_KEY_SIZE]
    }


    pub fn leaf_node_value(node: &mut [u8], cell_num: usize) -> &mut [u8] {
        &mut Self::leaf_node_cell(node, cell_num)
            [Self::LEAF_NODE_VALUE_OFFSET..Self::LEAF_NODE_VALUE_OFFSET + Self::LEAF_NODE_VALUE_SIZE]
    }


    pub fn initialize_leaf_node(node: &mut [u8]) {
        *Self::leaf_node_num_cells(node) = 0
    }


    pub fn leaf_node_insert(cursor: &mut Cursor, key: u32, value: &Row) -> io::Result<()> {
        let node = cursor.table.pager.get_page(cursor.page_num)?;

        let num_cells = *Self::leaf_node_num_cells(node);
        if num_cells as usize >= Self::LEAF_NODE_MAX_CELLS {
            // Node full
            return Err(io::Error::new(ErrorKind::Other, "Need to implement splitting a leaf node."));
        }

        if cursor.cell_num < num_cells as usize {
            // Make room for new cell
            for i in (cursor.cell_num..num_cells as usize).rev() {
                let (left, right) = node.split_at_mut(Self::LEAF_NODE_HEADER_SIZE + i * Self::LEAF_NODE_CELL_SIZE);
                let dest = &mut right[..Self::LEAF_NODE_CELL_SIZE];
                let src = &left[left.len() - Self::LEAF_NODE_CELL_SIZE..];
                dest.copy_from_slice(src);
            }
        }

        *Self::leaf_node_num_cells(node) += 1;
        Self::leaf_node_key(node, cursor.cell_num).copy_from_slice(&key.to_le_bytes());
        value.serialize_row_unsafe(Self::leaf_node_value(node, cursor.cell_num));

        Ok(())
    }


    pub fn print_leaf_node(node: &mut [u8]) {
        let num_cells = *Self::leaf_node_num_cells(node);
        println!("leaf (size {})", num_cells);
        for i in 0..num_cells as usize {
            let key = Self::leaf_node_key(node, i);
            let key_value = u32::from_le_bytes(key.try_into().expect("Incorrect key length"));
            println!("  - {} : {}", i, key_value);
        }
    }
}