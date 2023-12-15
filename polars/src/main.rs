use std::fs::File;
use std::env;
use polars::prelude::*;
use std::time::SystemTime;

const PATH: &str = "data/full_address_data.parquet";

// incorrect feature name: https://github.com/pola-rs/polars/issues/11178
fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Query required");
        std::process::exit(1);
    }

    let q = args[1].clone().to_ascii_uppercase();

    let start = SystemTime::now();
    
    // let df = LazyFrame::scan_parquet(PATH, ScanArgsParquet::default())?
    //     .with_streaming(true)
    //     .filter(
    //         col("street").str().contains_literal(lit("EUCLID"))
    //         .and(col("city").str().contains_literal(lit("FULLERTON")))
    //     )
    //     .collect()?;



    let df = LazyFrame::scan_parquet(PATH, ScanArgsParquet::default())?
        .with_streaming(true)
        .filter(
            col("number").str().contains_literal(lit(q.clone()  ))
            .or(col("street").str().contains_literal(lit(q.clone())))
            .or(col("street2").str().contains_literal(lit(q.clone())))
            .or(col("city").str().contains_literal(lit(q.clone())))
        )
        .collect()?;
    
    // some of the numbers are stored as strings with spaces in them. We need to remove the spaces
    // let mut mdf = df.clone();
    // let mdf = mdf.apply("number", |s: &Series| {
    //     s.utf8()
    //         .unwrap()
    //         .into_iter()
    //         .map(|streets| {
    //             streets.map(|street| {
    //                 street.replace(" ", "")
    //                 .replace("-", "")
    //             })
    //         })
    //         .collect::<Utf8Chunked>()
    //         .into_series()
    // })?;

    // CsvWriter::new(File::create("data/full_address_data.csv")?)
    //     .include_header(true)
    //     .finish(&mut df.clone())?;

    JsonWriter::new(File::create("data/full_address_data.json")?)
        .finish(&mut df.clone())?;

    // println!("{:?}", df);
    println!("Done!");
    println!("took: {} ms", start.elapsed().unwrap().as_millis());
    Ok(())

}
