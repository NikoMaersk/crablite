use std::process::exit;
use crablite::InputBuffer;
use crablite::statement::{Statement, StatementType};
use crablite::table::{Row, Table, ExecuteResult, USERNAME_SIZE, EMAIL_SIZE};
use std::time::Instant;


enum MetaCommandResult {
    MetaCommandSuccess,
    MetaCommandUnrecognizedCommand
}


enum PrepareResult {
    PrepareSuccess,
    PrepareNegativeId,
    PrepareUnrecognizedStatement,
    PrepareSyntaxError,
    PrepareStringTooLong,
}


fn safe_implementation(statement: &Statement) {
    let mut dest: [u8; 291] = [0u8; 291];
    let start = Instant::now();
    for _ in 0..1_000_000 {
        Row::serialize_row(&statement.row_to_insert, &mut dest);
    }
    let duration = start.elapsed();
    println!("safe impl: {:?}", duration);
}

fn unsafe_implementation(statement: &Statement) {
    let mut dest: [u8; 291] = [0u8; 291];
    let start = Instant::now();
    for _ in 0..1_000_000 {
        Row::serialize_row_unsafe(&statement.row_to_insert, &mut dest);
    }
    let duration = start.elapsed();
    println!("unsafe impl: {:?}", duration);
}


fn main() {
    let mut table = Table::db_open("C:\\tmp\\crablite.db");
    let mut input_buffer = InputBuffer::new();

    loop {
        InputBuffer::print_prompt();
        input_buffer.read_input();

        if input_buffer.buffer.chars().nth(0) == Some('.') {
            match do_meta_command(&input_buffer) {
                MetaCommandResult::MetaCommandSuccess => continue,
                MetaCommandResult::MetaCommandUnrecognizedCommand => {
                    println!("Unrecognized command '{}'", input_buffer.buffer);
                    continue;
                }
            }
        }

        let mut statement = Statement::default();
        match prepare_statement(&input_buffer, &mut statement) {
            PrepareResult::PrepareSuccess => (),
            PrepareResult::PrepareNegativeId => {
                println!("ID must be positive.");
                continue;
            },
            PrepareResult::PrepareSyntaxError => {
                println!("Syntax error. Could not parse statement.");
                continue;
            },
            PrepareResult::PrepareUnrecognizedStatement => {
                println!("Unrecognized keyword at start of '{}'", input_buffer.buffer);
                continue;
            },
            PrepareResult::PrepareStringTooLong => {
                println!("String is too long.");
                continue;
            }
        }

        match execute_statement(&statement, &mut table) {
            ExecuteResult::ExecuteSuccess => println!("Executed."),
            ExecuteResult::ExecuteTableFull => println!("Error: Table full."),
            ExecuteResult::ExecuteFailed => println!("Error: No command given"),
        }
    }
}


fn do_meta_command(input_buffer: &InputBuffer) -> MetaCommandResult {
    if input_buffer.buffer.eq(".exit") {
        exit(0);
    } else {
        return MetaCommandResult::MetaCommandUnrecognizedCommand
    }
}


fn prepare_insert(input_buffer: &InputBuffer, statement: &mut Statement) -> PrepareResult {
    statement.statement_type = StatementType::StatementInsert;

    let mut line_split = input_buffer.buffer.split_whitespace();
    if let [Some(keyword), Some(id_str), Some(username), Some(email)]
        = std::array::from_fn(|_| line_split.next()) {
        if keyword != "insert" {
            return PrepareResult::PrepareSyntaxError;
        }

        if let Ok(id) = id_str.parse::<i32>() {
            if id < 0 {
                return PrepareResult::PrepareNegativeId;
            }

            let id = id as u32;
            let username_bytes = username.as_bytes();
            let email_bytes = email.as_bytes();

            if username_bytes.len() > USERNAME_SIZE || email_bytes.len() > EMAIL_SIZE {
                return  PrepareResult::PrepareSyntaxError;
            }

            statement.row_to_insert.id = id;
            statement.row_to_insert.username[..username_bytes.len()].copy_from_slice(username_bytes);
            statement.row_to_insert.email[..email_bytes.len()].copy_from_slice(email_bytes);

            return PrepareResult::PrepareSuccess
        }
    }

    PrepareResult::PrepareSyntaxError
}


fn prepare_statement(input_buffer: &InputBuffer, statement: &mut Statement) -> PrepareResult {
    let trimmed_input = input_buffer.buffer.trim();

    return if trimmed_input.len() > 6 && &trimmed_input[..6] == "insert" {
        prepare_insert(input_buffer, statement)
    } else if trimmed_input == "select" {
        statement.statement_type = StatementType::StatementSelect;
        PrepareResult::PrepareSuccess
    } else {
        PrepareResult::PrepareUnrecognizedStatement
    }
}


fn execute_insert(statement: &Statement, table: &mut Table) -> ExecuteResult {
    table.insert_row(&statement.row_to_insert)
}


fn execute_select(statement: &Statement, table: &mut Table) -> ExecuteResult {
    table.print_all()
}


fn execute_statement(statement: &Statement, table: &mut Table) -> ExecuteResult {
    return match statement.statement_type {
        StatementType::StatementInsert => table.insert_row(&statement.row_to_insert),
        StatementType::StatementSelect => table.print_all(),
        StatementType::None => ExecuteResult::ExecuteFailed
    }
}


// Deprecated prepare_statement
// fn prepare_statement(input_buffer: &InputBuffer, statement: &mut Statement) -> PrepareResult {
//     let trimmed_input = input_buffer.buffer.trim();
//
//     if trimmed_input.len() >= 6 && &trimmed_input[..6] == "insert" {
//         statement.statement_type = StatementType::StatementInsert;
//
//         return match scan_fmt!(&input_buffer.buffer, "insert {} {} {}", u32, String, String) {
//             Ok((id, username, email)) => {
//                 statement.row_to_insert.id = id;
//
//                 let username_bytes = username.as_bytes();
//                 if username_bytes.len() > 32 {
//                     return PrepareResult::PrepareUnrecognizedStatement;
//                 }
//
//                 statement.row_to_insert.username[..username_bytes.len()].copy_from_slice(username_bytes);
//
//                 let email_bytes = email.as_bytes();
//                 if email_bytes.len() > 255 {
//                     return PrepareResult::PrepareUnrecognizedStatement;
//                 }
//
//                 statement.row_to_insert.email[..email_bytes.len()].copy_from_slice(email_bytes);
//
//                 PrepareResult::PrepareSuccess
//             },
//             Err(_) => {
//                 PrepareResult::PrepareSyntaxError
//             }
//         }
//     }
//
//
//     if trimmed_input == "select" {
//         statement.statement_type = StatementType::StatementSelect;
//         return PrepareResult::PrepareSuccess;
//     }
//
//     PrepareResult::PrepareUnrecognizedStatement
// }
//
