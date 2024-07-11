use crate::Row;

pub enum StatementType {
    StatementInsert,
    StatementSelect,
    None,
}


pub struct Statement {
    pub statement_type: StatementType,
    pub row_to_insert: Row
}


impl Default for Statement {
    fn default() -> Self {
        Statement {
            statement_type: StatementType::None,
            row_to_insert: Row::default()
        }
    }
}