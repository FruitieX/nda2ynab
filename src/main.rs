use chrono::NaiveDateTime;
use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use itertools::Itertools;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct NdaRow {
    #[serde(rename = "Kirjauspäivä")]
    date: String,

    #[serde(rename = "Määrä")]
    amount: String,

    #[serde(rename = "Otsikko")]
    description: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct YnabRow {
    date: String,
    payee: String,
    memo: String,
    amount: String,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = Some(r"
This program can be used to convert from Nordea bank's CSV export format to
YNAB's CSV format.

Point it to the directory where your Nordea CSV files are stored (for example
your Downloads directory), and it will generate a YNAB CSV containing only new
transactions since the previous export.
"))]
struct Args {
    /// Path to directory containing exported csv files
    path: String,
}

#[derive(Debug)]
struct ParsedFileName {
    file_name: String,
    path: PathBuf,
    date: NaiveDateTime,
    iban: String,
}

#[derive(Debug)]
struct PrevFileNewestTransaction {
    transaction: NdaRow,
    repetitions: usize,
}

fn read_nda_csv(path: &Path) -> Result<Vec<NdaRow>, Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new().delimiter(b';').from_path(path)?;
    let rows: Vec<NdaRow> = rdr.deserialize().filter_map(|r| r.ok()).collect();
    Ok(rows)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let dir = fs::read_dir(args.path)?;
    let re = Regex::new(r".+ (FI\d{2} \d{4} \d{4} \d{4} \d{2}) - (.+)\.csv").unwrap();

    let mut matches: Vec<ParsedFileName> = dir
        .filter_map(|p| p.ok())
        .filter_map(|p| {
            let path = p.path();
            let file_name = path.file_name()?.to_str()?.to_string();
            let iban = re.captures(&file_name)?.get(1)?.as_str().to_string();
            let date_match = re.captures(&file_name)?.get(2)?.as_str();
            let date = NaiveDateTime::parse_from_str(date_match, "%Y.%m.%d %H.%M").ok()?;

            Some(ParsedFileName {
                file_name,
                path,
                date,
                iban,
            })
        })
        .collect();

    // Sort by parsed date
    matches.sort_by(|a, b| b.date.cmp(&a.date));

    // Select the most recent matching csv file
    let newest_file = matches.first().ok_or("Could not find any matching files")?;

    println!(
        "Using most recently exported file: {}",
        newest_file.file_name
    );

    // Try to find previous csv file with matching iban and read most recent transactions
    let prev_file = matches.iter().skip(1).find(|m| m.iban == newest_file.iban);
    let prev_file_trx = if let Some(prev_file) = prev_file {
        println!("Using previously exported file: {}", prev_file.file_name);

        let rows = read_nda_csv(&prev_file.path)?;

        let first_row = rows.first().ok_or(format!(
            "{} does not contain any valid rows",
            newest_file.file_name
        ))?;

        // Count how many rows identical to first_row exist
        let repetitions = rows.iter().filter(|r| r == &first_row).count();

        Some(PrevFileNewestTransaction {
            transaction: first_row.clone(),
            repetitions,
        })
    } else {
        println!("No previous export found, including all rows from the csv file");

        None
    };

    let newest_rows = read_nda_csv(&newest_file.path)?;

    // Remove all previously processed rows from newest_rows
    let first_previously_processed_index = if let (Some(prev_file), Some(prev_file_trx)) =
        (prev_file, prev_file_trx)
    {
        let positions_matches: Vec<usize> = newest_rows
            .iter()
            .positions(|r| r == &prev_file_trx.transaction)
            .collect();

        let match_count = positions_matches.len();

        if match_count < prev_file_trx.repetitions {
            return Err(format!("The most recent transaction in '{}' was found in '{}' {} time(s), which is fewer than the {} time(s) it appears in '{}'. Make sure the most recent export contains rows from the previous export.", prev_file.file_name, newest_file.file_name, match_count, prev_file_trx.repetitions, prev_file.file_name ).into());
        }

        positions_matches
            .into_iter()
            .rev()
            .nth(prev_file_trx.repetitions - 1)
    } else {
        None
    };

    let rows = if let Some(first_previously_processed_index) = first_previously_processed_index {
        newest_rows[0..first_previously_processed_index].to_vec()
    } else {
        newest_rows
    };

    let mut wtr = WriterBuilder::new().from_path("out.csv")?;

    let _: Result<Vec<_>, _> = rows
        .into_iter()
        .map(|r| YnabRow {
            date: r.date,
            payee: r.description,
            memo: "".to_string(),
            amount: r.amount,
        })
        .map(|r| wtr.serialize(r))
        .collect();

    wtr.flush()?;

    println!("Results written to out.csv.");

    Ok(())
}
