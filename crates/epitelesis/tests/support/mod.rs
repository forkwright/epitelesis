use std::num::ParseIntError;
use std::path::Path;

pub(crate) fn read_complete_pid(path: &Path) -> Result<Option<u32>, ParseIntError> {
    let Ok(record) = std::fs::read_to_string(path) else {
        return Ok(None);
    };
    let Some(record) = record.strip_suffix('\n') else {
        return Ok(None);
    };
    record.parse().map(Some)
}
