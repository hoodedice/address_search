use parquet::file::reader::{FileReader, SerializedFileReader};
use tokio::fs::File;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use futures::TryStreamExt;
use futures::stream::Stream;
use std::time::SystemTime;
use std::sync::Arc;

const PATH: &str = "full_address_data.parquet";

/**
 * @see: https://github.com/apache/arrow-rs/blob/master/parquet/examples/async_read_parquet.rs
 */
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), ()> {

  let file = File::open(PATH).await.unwrap_or_else(|err| {
      panic!("Parquet: Error while opening file: {}", err);
  });


  let pfile_reader = SerializedFileReader::new(std::fs::File::open(PATH).unwrap()).unwrap_or_else(|err| {
      panic!("Parquet: Error while reading file: {}", err);
  });
  
  let pfile_metadata = pfile_reader.metadata();
  let pschema = pfile_metadata.file_metadata().schema();

  println!("Parquet Schema:\n{:#?}", pschema);

  return Ok(());
}


async fn get_schema(file: File) -> Arc<Schema> {
  ParquetRecordBatchStreamBuilder::new(file)
      .await
      .unwrap_or_else(|err| {
          panic!("Parquet: Error while reading file: {}", err);
      })
      .schema()
      .clone()
}
