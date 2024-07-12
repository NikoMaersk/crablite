use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};

const TABLE_MAX_PAGES: usize = 100;
pub const PAGE_SIZE: usize = 4096;

pub struct Pager {
    pub file: File,
    pub file_length: u64,
    pub pages: [Option<Box<[u8; PAGE_SIZE]>>; TABLE_MAX_PAGES]
}


impl Pager {
    pub fn pager_open(filename: &str) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)?;

        let file_length = file.metadata()?.len();

        let pages = std::array::from_fn(|_| None);

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
            let partial_page = self.file_length % PAGE_SIZE as u64 != 0;
            if page_num < num_pages || (page_num == num_pages && partial_page) {
                self.file.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))?;
                let bytes_to_read = if page_num == num_pages && partial_page {
                    (self.file_length % PAGE_SIZE as u64) as usize
                } else {
                    PAGE_SIZE
                };
                self.file.read_exact(&mut page[..bytes_to_read])?;
            }

            self.pages[page_num] = Some(page);
        }

        Ok(self.pages[page_num].as_mut().unwrap())
    }
}

