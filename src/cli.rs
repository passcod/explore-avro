use crate::avro_value::AvroValue;
use apache_avro::{types::Value, Reader};
use glob::glob;
use miette::{bail, miette, IntoDiagnostic, Result, WrapErr as _};
use std::fs::File;
use std::io::Seek;
use std::path::PathBuf;

pub(crate) type AvroData = Vec<Vec<AvroColumnarValue>>;

#[derive(Debug)]
pub(crate) struct AvroFile {
    file: File,
    path: PathBuf,
}

#[derive(Debug)]
pub(crate) struct CliService {
    files: Vec<AvroFile>,
}

#[derive(Debug, Clone)]
pub(crate) struct AvroColumnarValue {
    name: String,
    value: AvroValue,
}

impl AvroColumnarValue {
    pub fn from(name: String, value: AvroValue) -> Self {
        AvroColumnarValue { name, value }
    }
    
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn value(&self) -> &AvroValue {
        &self.value
    }
}

impl CliService {
    /// Creates an `Avro` as a union of all avros in the received paths
    ///
    /// # Arguments
    ///
    /// * `path` - A glob to match against Avro files to load
    pub fn from(path: String) -> Result<Self> {
        let mut paths: Vec<PathBuf> = Vec::new();
        for entry in glob(&path)
            .into_diagnostic()
            .wrap_err("Failed to read glob pattern")?
        {
            match entry {
                Ok(p) => paths.push(p),
                Err(e) => bail!("{:?}", e),
            }
        }

        if paths.len() == 0 {
            bail!("No files found");
        }

        let mut files: Vec<AvroFile> = Vec::new();
        for path in paths {
            let file = File::open(&path)
                .into_diagnostic()
                .wrap_err("Could not open file")?;
            files.push(AvroFile { file, path });
        }

        Ok(CliService { files })
    }

    /// Get all the names of the columns.
    /// Relies on the first record
    pub fn get_all_field_names(&mut self) -> Result<Vec<String>> {
        let first_file = &mut self.files[0];
        first_file
            .file
            .seek(std::io::SeekFrom::Start(0))
            .into_diagnostic()?;
        let mut reader = Reader::new(&first_file.file)
            .into_diagnostic()
            .wrap_err_with(|| format!("Could not read Avro file {}", first_file.path.display()))?;
        Ok(
            if let Ok(Value::Record(fields)) = reader.next().ok_or(miette!(
                "Avro must have at least one record row to infer schema"
            ))? {
                fields
                    .iter()
                    .map(|(f, _)| f.to_owned())
                    .collect::<Vec<String>>()
            } else {
                Vec::new()
            },
        )
    }

    /// Get all columns and values
    ///
    /// # Arguments
    /// * `fields_to_get` - Names of the columns to retrieve
    /// * `take` - Number of rows to take
    pub fn get_fields(
        &mut self,
        fields_to_get: &[String],
        take: Option<u32>,
    ) -> Result<Vec<Vec<AvroColumnarValue>>> {
        let mut extracted_fields = Vec::new();
        for file in &mut self.files {
            file.file
                .seek(std::io::SeekFrom::Start(0))
                .into_diagnostic()?;
            let reader = Reader::new(&file.file)
                .into_diagnostic()
                .wrap_err_with(|| format!("Could not read Avro file {}", file.path.display()))?;

            for (i, row) in reader.enumerate() {
                if extracted_fields.len() as u32 >= take.unwrap_or(u32::max_value()) {
                    break;
                }

                let row = row
                    .into_diagnostic()
                    .wrap_err_with(|| format!("Could not parse row {} from the Avro", i))?;
                if let Value::Record(fields) = row {
                    let mut extracted_fields_for_row = Vec::new();
                    for field_name in fields_to_get {
                        let field_value_to_insert = match fields
                            .iter()
                            .find(|(n, _)| n == field_name)
                        {
                            Some((field_name, field_value)) => {
                                let v = field_value.clone();
                                AvroColumnarValue::from(field_name.to_owned(), AvroValue::from(v))
                            }
                            None => AvroColumnarValue::from(field_name.to_owned(), AvroValue::na()),
                        };
                        extracted_fields_for_row.push(field_value_to_insert);
                    }
                    extracted_fields.push(extracted_fields_for_row);
                }
            }
        }

        Ok(extracted_fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_get_all_field_names() {
        println!("asdas");
        let path_to_test_avro = Path::new("./test_assets/bttf.avro")
            .to_str()
            .unwrap()
            .to_owned();
        let mut cli = CliService::from(path_to_test_avro).unwrap();
        let field_names = cli.get_all_field_names().unwrap();
        assert_eq!(field_names, vec!["firstName", "lastName", "age"]);
    }

    #[test]
    fn test_get_fields() {
        println!("asdas");
        let path_to_test_avro = Path::new("./test_assets/bttf.avro")
            .to_str()
            .unwrap()
            .to_owned();
        let _cli = CliService::from(path_to_test_avro).unwrap();
        // let field_names = cli.get_fields(vec!["firstName", "age"], None);
        // assert_eq!(field_names, vec!["firstName", "lastName", "age"]);
    }
}
