use std::process::exit;
use crate::leaf_node::{NodeType, LeafNode};
use crate::table::Table;

pub struct Cursor<'a> {
    pub table: &'a mut Table,
    pub page_num: usize,
    pub cell_num: usize,
    pub end_of_table: bool
}


impl<'a> Cursor<'a> {
    pub fn table_start(table: &'a mut Table) -> Self {
        let page_num = table.root_page_num;
        let cell_num = 0;

        let root_node = table.pager.get_page(page_num).expect("Failed to retrieve page");
        let num_cells = *LeafNode::leaf_node_num_cells(root_node);
        let end_of_table = num_cells == 0;

        Cursor {
            table,
            page_num,
            cell_num,
            end_of_table
        }
    }


    pub fn table_end(table: &'a mut Table) -> Self {
        let page_num = table.root_page_num;
        let root_node = table.pager.get_page(page_num).expect("Failed to retrieve page");
        let num_cells = *LeafNode::leaf_node_num_cells(root_node) as usize;

        Cursor {
            table,
            page_num,
            cell_num: num_cells,
            end_of_table: true
        }
    }


    pub fn table_find(table: &'a mut Table, key: u32) -> Self {
        let root_page_num = table.root_page_num;
        let root_node = table.pager.get_page(root_page_num).expect("Failed to retrieve page");

        if LeafNode::get_node_type(root_node) == NodeType::NodeLeaf {
            return Self::leaf_node_find(table, root_page_num, key)
        } else {
            println!("Need to implement searching an internal node");
            exit(1);
        }
    }


    pub fn leaf_node_find(table: &'a mut Table, page_num: usize, key: u32) -> Self {
        let node = table.pager.get_page(page_num).expect("Failed to retrieve page");
        let num_cells = LeafNode::leaf_node_num_cells(node);

        let mut min_index = 0;
        let mut max_index = *num_cells as usize;

        while min_index != max_index {
            let index = min_index + (max_index - min_index) / 2;
            let key_at_index = u32::from_le_bytes(LeafNode::leaf_node_key(node, index).try_into().unwrap());

            if key == key_at_index {
                return Cursor {
                    table,
                    page_num,
                    cell_num: index,
                    end_of_table: false
                }
            } else if key < key_at_index {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }

        Cursor {
            table,
            page_num,
            cell_num: min_index,
            end_of_table: true,
        }
    }


    pub fn table_find_position(table: &'a mut Table, key: u32) -> (usize, usize) {
        let root_page_num = table.root_page_num;
        let root_node = table.pager.get_page(root_page_num).expect("Failed to retrieve page");

        if LeafNode::get_node_type(root_node) == NodeType::NodeLeaf {
            return Self::leaf_node_find_position(table, root_page_num, key);
        } else {
            println!("Need to implement searching an internal node");
            exit(1);
        }
    }


    pub fn leaf_node_find_position(table: &'a mut Table, page_num: usize, key: u32) -> (usize, usize) {
        let node = table.pager.get_page(page_num).expect("Failed to retrieve page");
        let num_cells = LeafNode::leaf_node_num_cells(node);

        let mut min_index = 0;
        let mut max_index = *num_cells as usize;

        while min_index != max_index {
            let index = min_index + (max_index - min_index) / 2;
            let key_at_index = u32::from_le_bytes(LeafNode::leaf_node_key(node, index).try_into().unwrap());

            if key == key_at_index {
                return (page_num, index);
            } else if key < key_at_index {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }

        (page_num, min_index)
    }


    pub fn cursor_value(&mut self) -> &mut [u8] {
        let page = self.table.pager.get_page(self.page_num).unwrap();
        LeafNode::leaf_node_value(page, self.cell_num)
    }


    pub fn cursor_advance(&mut self) {
        let node = self.table.pager.get_page(self.page_num).unwrap();
        self.cell_num += 1;

        if self.cell_num >= *LeafNode::leaf_node_num_cells(node) as usize {
            self.end_of_table = true;
        }
    }
}