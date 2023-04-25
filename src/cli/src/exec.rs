use std::ascii;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion_cli::command::{Command, OutputFormat};
use datafusion_cli::helper::CliHelper;
use datafusion_cli::print_options::PrintOptions;
use rustyline::error::ReadlineError;
use rustyline::Editor;

// copied here from datafusion-cli-22.0.0/src/exec.rs so we can modify exec_and_print
pub async fn exec_from_files(
    files: Vec<String>,
    ctx: &mut SessionContext,
    print_options: &PrintOptions,
) {
    let files = files
        .into_iter()
        .map(|file_path| File::open(file_path).unwrap())
        .collect::<Vec<_>>();
    for file in files {
        let mut reader = BufReader::new(file);
        exec_from_lines(ctx, &mut reader, print_options).await;
    }
}

// copied here from datafusion-cli-22.0.0/src/exec.rs so we can modify exec_and_print
pub async fn exec_from_repl(
    ctx: &mut SessionContext,
    print_options: &mut PrintOptions,
) -> rustyline::Result<()> {
    let mut rl = Editor::<CliHelper>::new()?;
    rl.set_helper(Some(CliHelper::default()));
    rl.load_history(".history").ok();

    let mut print_options = print_options.clone();

    loop {
        match rl.readline("❯ ") {
            Ok(line) if line.starts_with('\\') => {
                rl.add_history_entry(line.trim_end());
                let command = line.split_whitespace().collect::<Vec<_>>().join(" ");
                if let Ok(cmd) = &command[1..].parse::<Command>() {
                    match cmd {
                        Command::Quit => break,
                        Command::OutputFormat(subcommand) => {
                            if let Some(subcommand) = subcommand {
                                if let Ok(command) = subcommand.parse::<OutputFormat>() {
                                    if let Err(e) = command.execute(&mut print_options).await {
                                        eprintln!("{e}")
                                    }
                                } else {
                                    eprintln!("'\\{}' is not a valid command", &line[1..]);
                                }
                            } else {
                                println!("Output format is {:?}.", print_options.format);
                            }
                        }
                        _ => {
                            if let Err(e) = cmd.execute(ctx, &mut print_options).await {
                                eprintln!("{e}")
                            }
                        }
                    }
                } else {
                    eprintln!("'\\{}' is not a valid command", &line[1..]);
                }
            }
            Ok(line) => {
                rl.add_history_entry(line.trim_end());
                match exec_and_print(ctx, &print_options, line).await {
                    Ok(_) => {}
                    Err(err) => eprintln!("{err}"),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("\\q");
                break;
            }
            Err(err) => {
                eprintln!("Unknown error happened {:?}", err);
                break;
            }
        }
    }

    Ok(())
}

// copied here from datafusion-cli-22.0.0/src/exec.rs so we can modify exec_and_print
pub async fn exec_from_lines(
    ctx: &mut SessionContext,
    reader: &mut BufReader<File>,
    print_options: &PrintOptions,
) {
    let mut query = "".to_owned();

    for line in reader.lines() {
        match line {
            Ok(line) if line.starts_with("--") => {
                continue;
            }
            Ok(line) => {
                let line = line.trim_end();
                query.push_str(line);
                if line.ends_with(';') {
                    match exec_and_print(ctx, print_options, query).await {
                        Ok(_) => {}
                        Err(err) => println!("{err}"),
                    }
                    query = "".to_owned();
                } else {
                    query.push('\n');
                }
            }
            _ => {
                break;
            }
        }
    }

    // run the left over query if the last statement doesn't contain ‘;’
    if !query.is_empty() {
        match exec_and_print(ctx, print_options, query).await {
            Ok(_) => {}
            Err(err) => println!("{err}"),
        }
    }
}

// copied here from datafusion-cli-22.0.0/src/exec.rs with modification to change byte printing
async fn exec_and_print(
    ctx: &mut SessionContext,
    print_options: &PrintOptions,
    sql: String,
) -> Result<(), DataFusionError> {
    let now = Instant::now();

    let plan = ctx.state().create_logical_plan(&sql).await?;
    let df = ctx.execute_logical_plan(plan).await?;

    let mut results = df.collect().await?;

    for batch in &mut results {
        let schema = batch.schema();
        let iter = batch.columns().iter().enumerate().map(|(i, column)| {
            match column.data_type() {
                DataType::Binary => (),
                _ => return (schema.field(i).name(), column.clone()),
            }

            let column: &BinaryArray = column.as_binary();

            // this field has a good string representation
            if schema.field(i).name() == "invocation_id" {
                return (
                    schema.field(i).name(),
                    Arc::new(StringArray::from_iter(column.into_iter().map(|bytes| {
                        bytes.map(|bytes| {
                            uuid::Uuid::from_slice(bytes)
                                .expect("invocation_id was not a uuid")
                                .to_string()
                        })
                    }))) as ArrayRef,
                );
            }

            // otherwise, just use escaped ascii to represent any byte sequence
            (
                schema.field(i).name(),
                Arc::new(StringArray::from_iter(column.iter().map(|bytes| {
                    Some(unsafe {
                        String::from_utf8_unchecked(
                            bytes?
                                .iter()
                                .flat_map(|byte| ascii::escape_default(*byte))
                                .collect(),
                        )
                    })
                }))) as ArrayRef,
            )
        });
        *batch = RecordBatch::try_from_iter(iter)?
    }

    print_options.print_batches(&results, now)?;

    Ok(())
}
