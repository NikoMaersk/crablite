use crate::b_tree::LeafNode;
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