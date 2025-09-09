//! Generate test fixtures for faster test execution.
//!
//! This binary generates Parquet files from TPC-H data that can be
//! loaded quickly in tests instead of regenerating data each time.

use parquet::arrow::ArrowWriter;
use std::path::Path;
use term_guard::test_utils::{create_tpc_h_context, ScaleFactor};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating test fixtures...");

    let fixtures_dir = Path::new("fixtures");
    if !fixtures_dir.exists() {
        std::fs::create_dir_all(fixtures_dir)?;
    }

    // Generate fixtures for different scale factors
    for scale in [ScaleFactor::SF01, ScaleFactor::SF1] {
        println!("Generating fixtures for scale {scale:?}");
        let ctx = create_tpc_h_context(scale).await?;

        let scale_dir = fixtures_dir.join(format!("{scale:?}").to_lowercase());
        std::fs::create_dir_all(&scale_dir)?;

        // Save each table as Parquet
        for table_name in [
            "region", "nation", "customer", "orders", "lineitem", "supplier", "part", "partsupp",
        ] {
            let table = match ctx.table(table_name).await {
                Ok(t) => t,
                Err(_) => {
                    println!("  Skipping {table_name} (not found)");
                    continue;
                }
            };

            let output_path = scale_dir.join(format!("{table_name}.parquet"));
            println!("  Writing {table_name} to {output_path:?}");

            // Collect to DataFrame and write to Parquet
            let df = table.collect().await?;
            if !df.is_empty() {
                let file = std::fs::File::create(&output_path)?;
                let mut writer = ArrowWriter::try_new(file, df[0].schema(), None)?;

                for batch in df {
                    writer.write(&batch)?;
                }
                writer.close()?;
            }
        }
    }

    println!("Fixtures generated successfully!");
    Ok(())
}
