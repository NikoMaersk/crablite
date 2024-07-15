use crate::data_consts::{ROW_SIZE, ROWS_PER_PAGE};
use crate::table::Table;

pub struct Cursor<'a> {
    pub table: &'a mut Table,
    pub row_num: usize,
    pub end_of_table: bool
}


impl<'a> Cursor<'a> {
    pub fn table_start(table: &'a mut Table) -> Self {
        let end_of_table = table.num_rows == 0;
        Cursor {
            table,
            row_num: 0,
            end_of_table
        }
    }


    pub fn table_end(table: &'a mut Table) -> Self {
        let row_num = table.num_rows;
        Cursor {
            table,
            row_num,
            end_of_table: true
        }
    }


    pub fn cursor_advance(&mut self) {
        self.row_num += 1;
        if self.row_num >= self.table.num_rows {
            self.end_of_table = true;
        }
    }


    pub fn cursor_value(&mut self) -> &mut [u8] {
        let row_num = self.row_num;
        let page_num = row_num / ROWS_PER_PAGE;

        let page = self.table.pager.get_page(page_num).unwrap();

        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        &mut page[byte_offset..byte_offset + ROW_SIZE]
    }
}