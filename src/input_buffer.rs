use std::io::{self, Write};
use std::process::exit;


pub struct InputBuffer {
    pub buffer: String,
}


impl InputBuffer {
    pub fn new() -> InputBuffer {
        InputBuffer {
            buffer: String::new(),
        }
    }


    pub fn print_prompt() {
        print!("db > ");
        io::stdout().flush().expect("Failed to flush stdout")
    }


    pub fn read_input(&mut self) {
        self.buffer.clear();
        if io::stdin().read_line(&mut self.buffer).is_err() {
            println!("Error reading input");
            exit(1);
        }

        self.buffer = self.buffer.trim_end().to_string()
    }
}
