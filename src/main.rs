use std::io::Write;

use avro_value::AvroValue;
use clap::Parser;
use cli::{AvroColumnarValue, AvroData, CliService};
use miette::{bail, IntoDiagnostic as _, Result, WrapErr as _};
use prettytable::{color, Attr, Cell, Row, Table};
use regex::Regex;

mod avro_value;
mod cli;

/// A CLI for exploring [Apache Avro](https://avro.apache.org/) files.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
enum RavroArgs {
    /// Get fields from an Avro file
    Get {
        /// Files to process
        path: String,

        /// Names of the fields to get to get
        #[arg(short, long = "fields")]
        fields_to_get: Vec<String>,

        /// Regex to search. Only a row with a matching field will appear in the outputted table
        #[arg(short, long = "search")]
        search: Option<String>,

        /// Maximum number of records to show
        #[arg(short, long = "take")]
        take: Option<u32>,

        /// Output format.
        ///
        /// Omit for pretty table output, or specify: `csv`, `json`, `json-pretty`.
        #[arg(short = 'p', long = "format")]
        output_format: Option<String>,
    },
}

fn main() -> Result<()> {
    match RavroArgs::parse() {
        RavroArgs::Get {
            fields_to_get,
            path,
            search,
            take,
            output_format,
        } => {
            let mut avro = CliService::from(path)?;
            let fields_to_get = if fields_to_get.is_empty() {
                avro.get_all_field_names()?
            } else {
                fields_to_get
            };

            let data = avro.get_fields(&fields_to_get, take)?;

            match output_format {
                None => print_as_table(&fields_to_get, data, search)?,
                Some(format_option) => match format_option.as_ref() {
                    "csv" => print_as_csv(&fields_to_get, data)
                        .wrap_err("Could not print Avro as CSV")?,
                    "json" => print_as_json(&fields_to_get, data, false)
                        .wrap_err("Could not print Avro as JSON")?,
                    "json-pretty" => print_as_json(&fields_to_get, data, true)
                        .wrap_err("Could not print Avro as JSON")?,
                    _ => bail!("Output format not recognized"),
                },
            }
        }
    }

    Ok(())
}

fn print_as_table(field_names: &[String], data: AvroData, search: Option<String>) -> Result<()> {
    let mut table = Table::new();

    let search = match search {
        None => None,
        Some(re) => Some(Regex::new(&re).into_diagnostic()?),
    };

    let header_cells: Vec<Cell> = field_names
        .iter()
        .map(|f| {
            Cell::new(f)
                .with_style(Attr::Bold)
                .with_style(Attr::ForegroundColor(color::BLUE))
                .with_style(Attr::Underline(true))
        })
        .collect();
    table.add_row(Row::new(header_cells));

    let filtered_data: AvroData = data
        .into_iter()
        .filter(|r| {
            r.iter()
                .find(|v| match &search {
                    None => true,
                    Some(search) => search.is_match(&v.value().to_string()),
                })
                .is_some()
        })
        .collect();

    for fields_for_row in filtered_data {
        let row_cells: Vec<Cell> = fields_for_row
            .iter()
            .filter_map(|v: &AvroColumnarValue| {
                let value_str = v.value().to_string();
                let mut cell = Cell::new(&value_str);
                if let Some(search) = &search {
                    if search.is_match(&value_str) {
                        cell.style(Attr::Bold);
                        cell.style(Attr::ForegroundColor(color::GREEN));
                    }
                }

                match v.value() {
                    AvroValue::Na => cell.style(Attr::ForegroundColor(color::RED)),
                    _ => {}
                }

                Some(cell)
            })
            .collect();
        table.add_row(Row::new(row_cells));
    }

    table.printstd();
    Ok(())
}

fn print_as_csv(field_names: &[String], data: AvroData) -> Result<()> {
    let mut writer = csv::Writer::from_writer(std::io::stdout());

    // Headers
    writer.write_record(field_names).into_diagnostic()?;

    for row in data {
        writer
            .write_record(
                row.iter()
                    .map(|val: &AvroColumnarValue| val.value().to_string())
                    .collect::<Vec<String>>(),
            )
            .into_diagnostic()?;
    }

    writer.flush().into_diagnostic()?;
    Ok(())
}

fn print_as_json(field_filter: &[String], data: AvroData, pretty: bool) -> Result<()> {
    let mut stdout = std::io::stdout();
    for row in data {
        let obj = serde_json::Value::Object(
            row.iter()
                .filter(|val| field_filter.iter().any(|f| val.name() == f))
                .map(|val: &AvroColumnarValue| {
                    val.value().to_json().map(|v| (val.name().to_owned(), v))
                })
                .collect::<Result<serde_json::Map<String, serde_json::Value>>>()?,
        );

        if pretty {
            serde_json::to_writer_pretty(&mut stdout, &obj).into_diagnostic()?;
        } else {
            serde_json::to_writer(&mut stdout, &obj).into_diagnostic()?;
        }
        writeln!(&mut stdout, "").into_diagnostic()?;
    }
    Ok(())
}
