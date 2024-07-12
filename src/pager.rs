use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};

const TABLE_MAX_PAGES: usize = 100;
pub const PAGE_SIZE: usize = 4096;

pub struct Pager {
    pub file: File,
    pub file_length: u64,
    // pub pages: Vec<Option<Vec<u8>>>,
    // pub pages: [Option<Vec<u8>>; TABLE_MAX_PAGES]
    pub pages: Vec<Option<Box<[u8; PAGE_SIZE]>>>
}


impl Pager {
    pub fn pager_open(filename: &str) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)?;

        let file_length = file.metadata()?.len();

        // let pages = unsafe { std::mem::zeroed() };

        let mut pages = Vec::with_capacity(TABLE_MAX_PAGES);
        pages.resize(TABLE_MAX_PAGES, None);

        Ok(Pager {
            file,
            file_length,
            pages
        })
    }


    pub fn pager_flush(&mut self, page_num: usize, size: usize) -> io::Result<()> {
        if page_num >= TABLE_MAX_PAGES {
            return Err(io::Error::new(ErrorKind::InvalidInput, "page number out of bounds"));
        }

        if let Some(page) = &self.pages[page_num] {
            self.file.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))?;
            self.file.write_all(&page[..size])?;
        } else {
            return Err(io::Error::new(ErrorKind::Other, "Tried to flush null page"));
        }

        Ok(())
    }


    pub fn get_page(&mut self, page_num: usize) -> io::Result<&mut [u8; PAGE_SIZE]> {
        if page_num >= TABLE_MAX_PAGES {
            return Err(io::Error::new(ErrorKind::InvalidInput, "Page number out of bounds"));
        }

        if self.pages[page_num].is_none() {
            // Cache miss. Allocate memory and load from file.
            let mut page = Box::new([0u8; PAGE_SIZE]);
            let num_pages = (self.file_length / PAGE_SIZE as u64) as usize;

            // We might save a partial page at the end of the file
            if self.file_length % PAGE_SIZE as u64 != 0 {
                if page_num <= num_pages {
                    self.file.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))?;
                    let bytes_read = self.file.read(&mut page[..])?;
                    if bytes_read == 0 {
                        return Err(io::Error::new(ErrorKind::UnexpectedEof, "Reached end of file"));
                    }
                }
            }

            self.pages[page_num] = Some(page);
        }

        Ok(self.pages[page_num].as_mut().unwrap())
    }
}

