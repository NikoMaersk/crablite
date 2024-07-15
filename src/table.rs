use std::{io, ptr};
use crate::cursor::Cursor;
use crate::pager::{Pager};
use crate::data_consts::*;


#[derive(Debug)]
#[repr(C)]
pub struct Row {
    pub id: u32,
    pub username: [u8; USERNAME_SIZE],
    pub email: [u8; EMAIL_SIZE],
}


impl Row {
    pub fn new(id: u32, username: &str, email: &str) -> Self {
        let mut row = Row {
            id,
            username: [0; 32],
            email: [0; 255]
        };

        let username_bytes = username.as_bytes();
        let email_bytes = email.as_bytes();

        row.username[..username.len()].copy_from_slice(username_bytes);
        row.email[..email.len()].copy_from_slice(email_bytes);
        row
    }


    pub fn serialize_row_unsafe(source: &Row, destination: &mut [u8]) {
        assert_eq!(destination.len(), EMAIL_OFFSET + EMAIL_SIZE);

        unsafe {
            let source_ptr = source as *const Row as *const u8;
            ptr::copy_nonoverlapping(
                source_ptr.add(ID_OFFSET),
                destination.as_mut_ptr().add(ID_OFFSET),
                ID_SIZE);
            ptr::copy_nonoverlapping(
                source_ptr.add(USERNAME_OFFSET),
                destination.as_mut_ptr().add(USERNAME_OFFSET),
                USERNAME_SIZE);
            ptr::copy_nonoverlapping(
                source_ptr.add(EMAIL_OFFSET),
                destination.as_mut_ptr().add(EMAIL_OFFSET),
                EMAIL_SIZE);
        }
    }


    pub fn serialize_row(source: &Row, destination: &mut [u8]) {
        let id_bytes = source.id.to_le_bytes();
        destination[ID_OFFSET..ID_OFFSET + ID_SIZE].copy_from_slice(&id_bytes);

        let username_length = source.username.len().min(USERNAME_SIZE);
        destination[USERNAME_OFFSET..USERNAME_OFFSET + username_length]
            .copy_from_slice(&source.username[..username_length]);

        for i in username_length..USERNAME_SIZE {
            destination[USERNAME_OFFSET + i] = 0;
        }

        let email_length = source.email.len().min(EMAIL_SIZE);
        destination[EMAIL_OFFSET..EMAIL_OFFSET + email_length]
            .copy_from_slice(&source.email[..email_length]);

        for i in email_length..EMAIL_SIZE {
            destination[EMAIL_OFFSET + i] = 0;
        }
    }


    fn deserialize_row(source: &[u8]) -> Self {
        let id = u32::from_le_bytes([source[0], source[1], source[2], source[3]]);
        let mut username = [0; USERNAME_SIZE];
        username.copy_from_slice(&source[ID_OFFSET + ID_SIZE..USERNAME_OFFSET + USERNAME_SIZE]);
        let mut email = [0; EMAIL_SIZE];
        email.copy_from_slice(&source[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE]);

        Row { id, username, email }
    }


    fn deserialize_row_existing_ref(source: &[u8], destination: &mut Row) {
        let (id_bytes, rest) = source.split_at(ID_SIZE);
        destination.id = u32::from_ne_bytes(id_bytes.try_into().unwrap());

        let (username_bytes, rest) = rest.split_at(USERNAME_SIZE);
        destination.username.copy_from_slice(username_bytes);

        let (email_bytes, _) = rest.split_at(EMAIL_SIZE);
        destination.email.copy_from_slice(email_bytes);
    }


    pub fn print_row(&self) {
        let username = String::from_utf8_lossy(&self.username);
        let email = String::from_utf8_lossy(&self.email);
        println!("({}, {}, {})", self.id, username.trim_end(), email.trim_end());
    }
}


impl Default for Row {
    fn default() -> Self {
        Row {
            id: 0,
            username: [0; USERNAME_SIZE],
            email: [0; EMAIL_SIZE],
        }
    }
}


pub enum ExecuteResult {
    ExecuteSuccess,
    ExecuteTableFull,
    ExecuteFailed,
}


pub struct Table {
    pub num_rows: usize,
    pub pager: Pager,
}


impl Table {
    pub fn db_open(filename: &str) -> io::Result<Self> {
        let pager = Pager::pager_open(filename)?;

        Ok(
        Table {
            num_rows: pager.file_length as usize / ROW_SIZE,
            pager
        })
    }


    pub fn db_close(&mut self) -> io::Result<()> {
        let pager = &mut self.pager;
        let num_full_pages = self.num_rows / ROWS_PER_PAGE;

        for i in 0..num_full_pages {
            if pager.pages[i].is_some() {
                pager.pager_flush(i, PAGE_SIZE)?;
            }
        }

        let num_additional_rows = self.num_rows % ROWS_PER_PAGE;
        if num_additional_rows > 0 {
            let page_num = num_full_pages;
            if pager.pages[page_num].is_some() {
                pager.pager_flush(page_num, num_additional_rows * ROW_SIZE)?;
            }
        }

        Ok(())
    }


    pub fn insert_row(&mut self, row_to_insert: &Row) -> ExecuteResult {
        if self.num_rows >= TABLE_MAX_ROWS {
            return ExecuteResult::ExecuteTableFull;
        }

        let mut cursor = Cursor::table_end(self);

        Row::serialize_row_unsafe(&row_to_insert, cursor.cursor_value());

        self.num_rows += 1;

        ExecuteResult::ExecuteSuccess
    }


    pub fn insert_row_str(&mut self, id: u32, username: &str, email: &str) -> ExecuteResult {
        if self.num_rows >= TABLE_MAX_ROWS {
            return ExecuteResult::ExecuteTableFull;
        }

        let username_bytes = username.as_bytes();
        let email_bytes = email.as_bytes();

        if username_bytes.len() > USERNAME_SIZE || email_bytes.len() > EMAIL_SIZE {
            return ExecuteResult::ExecuteFailed;
        }

        let row = Row::new(id, username, email);

        Row::serialize_row_unsafe(&row, self.row_slot(self.num_rows));

        self.num_rows += 1;

        ExecuteResult::ExecuteSuccess
    }


    pub fn print_all(&mut self) -> ExecuteResult {
        let mut row = Row::default();
        for i in 0..self.num_rows {
            Row::deserialize_row_existing_ref(self.row_slot(i), &mut row);
            Row::print_row(&row);
        }

        ExecuteResult::ExecuteSuccess
    }


    pub fn print_all_cursor(&mut self) -> ExecuteResult {
        let mut cursor = Cursor::table_start(self);
        let mut row = Row::default();

        while !&cursor.end_of_table {
            Row::deserialize_row_existing_ref(cursor.cursor_value(), &mut row);
            row.print_row();
            cursor.cursor_advance();
        }

        ExecuteResult::ExecuteSuccess
    }


    pub fn get_row(&self, row_num: usize) -> Option<Row> {
        if row_num >= self.num_rows {
            return None
        }

        let page_num = row_num / ROWS_PER_PAGE;
        let row_offset = row_num % ROWS_PER_PAGE;

        if let Some(page) = self.pager.pages.get(page_num).and_then(|page| page.as_ref()) {
            let row_start = row_offset * ROW_SIZE;
            let row_end = row_start + ROW_SIZE;

            return if row_end <= page.len() {
                let row_data = &page[row_start..row_end];
                Some(Row::deserialize_row(row_data))
            } else {
                None
            }
        }

        None
    }


    fn row_slot(&mut self, row_num: usize) -> &mut [u8] {
        let page_num = row_num / ROWS_PER_PAGE;
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        let page = self.pager.get_page(page_num).unwrap();

        &mut page[byte_offset..byte_offset + ROW_SIZE]
    }
}