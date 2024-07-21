use crate::data_consts::{PAGE_SIZE, ROW_SIZE};

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


/// Leaf Node Header Layout
const LEAF_NODE_NUM_CELL_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_NUM_CELL_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELL_SIZE;


/// Leaf Node Body Layout
const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_KEY_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_VALUE_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;


#[repr(C)]
pub struct LeafNode {

}

impl LeafNode {
    pub fn leaf_node_num_cells(node: *mut u8) -> *mut u32 {
        unsafe { node.add(LEAF_NODE_NUM_CELL_OFFSET) as *mut u32 }
    }


    pub fn leaf_node_cell(node: *mut u8, cell_num: usize) -> *mut u32 {
        unsafe {
            node.add(LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE) as *mut u32
        }
    }

    pub fn leaf_node_key(node: *mut u8, cell_num: usize) -> *mut u32 {
        unsafe {
            Self::leaf_node_cell(node, cell_num)
        }
    }


    pub fn leaf_node_value(node: *mut u8, cell_num: usize) -> *mut u32 {
        unsafe {
            Self::leaf_node_cell(node, cell_num).add(LEAF_NODE_KEY_SIZE)
        }
    }


    pub fn initialize_leaf_node(node: *mut u8) {
        unsafe {
            *Self::leaf_node_num_cells(node) = 0;
        }
    }
}