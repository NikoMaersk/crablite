use std::ptr;
use crate::statement::Statement;


const ID_SIZE: usize = std::mem::size_of::<u32>();
const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;


#[derive(Debug)]
#[repr(C)]
pub struct Row {
    pub id: u32,
    pub username: [u8; USERNAME_SIZE],
    pub email: [u8; EMAIL_SIZE],
}


impl Row {
    fn new(id: u32, username: &[u8], email: &[u8]) -> Self {
        let mut row = Row {
            id,
            username: [0; 32],
            email: [0; 255]
        };

        row.username[..username.len()].copy_from_slice(username);
        row.email[..email.len()].copy_from_slice(email);
        row
    }


    pub fn serialize_row_unsafe(source: &Row, destination: &mut [u8]) {
        unsafe {
            let source_ptr = source as *const Row as *const u8;
            ptr::copy_nonoverlapping(source_ptr.add(ID_OFFSET), destination.as_mut_ptr().add(ID_OFFSET), ID_SIZE);
            ptr::copy_nonoverlapping(source_ptr.add(USERNAME_OFFSET), destination.as_mut_ptr().add(USERNAME_OFFSET), USERNAME_SIZE);
            ptr::copy_nonoverlapping(source_ptr.add(EMAIL_OFFSET), destination.as_mut_ptr().add(EMAIL_OFFSET), EMAIL_SIZE);
        }
    }


    pub fn serialize_row(source: &Row, destination: &mut [u8]) {
        let id_bytes = source.id.to_ne_bytes();
        destination[ID_OFFSET..ID_OFFSET + ID_SIZE].copy_from_slice(&id_bytes);

        destination[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE]
            .copy_from_slice(&source.username);

        destination[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE]
            .copy_from_slice(&source.email);
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
            username: [0; 32],
            email: [0; 255],
        }
    }
}


pub enum ExecuteResult {
    ExecuteSuccess,
    ExecuteTableFull,
    ExecuteFailed,
}


const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;


pub struct Table {
    pub num_rows: usize,
    pub pages: Vec<Option<Vec<u8>>>
}


impl Table {
    pub fn new() -> Self {
        let mut pages = Vec::with_capacity(TABLE_MAX_PAGES);
        pages.resize(TABLE_MAX_PAGES, None);
        Table {
            num_rows: 0,
            pages
        }
    }


    pub fn insert_row(&mut self, statement: &Statement) -> ExecuteResult {
        if self.num_rows >= TABLE_MAX_ROWS {
            return ExecuteResult::ExecuteTableFull;
        }

        Row::serialize_row(&statement.row_to_insert, self.row_slot(self.num_rows));

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


    pub fn get_row(&self, row_num: usize) -> Option<Row> {
        if row_num >= self.num_rows {
            return None
        }

        let page_num = row_num / ROWS_PER_PAGE;
        let row_offset = row_num % ROWS_PER_PAGE;

        if let Some(page) = self.pages.get(page_num).and_then(|page| page.as_ref()) {
            let row_start = row_offset * ROW_SIZE;
            let row_end = row_start + ROW_SIZE;

            return if row_end <= page.len() {
                let row_data = &page[row_start..row_end];
                Some(Row::deserialize_row(row_data))
            } else {
                None
            }
        }

        // if let Some(ref page) = self.pages[page_num] {
        //     let row_data = &page[row_offset..row_offset + ROW_SIZE];
        //     return Some(Row::deserialize_row(row_data));
        // }

        None
    }


    fn get_page(&mut self, page_num: usize) -> &mut Vec<u8> {
        self.pages[page_num].get_or_insert_with(|| vec![0; PAGE_SIZE])
    }


    fn row_slot(&mut self, row_num: usize) -> &mut [u8] {
        let page_num = row_num / ROWS_PER_PAGE;
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        let page = self.get_page(page_num);
        &mut page[byte_offset..byte_offset + ROW_SIZE]
    }
}