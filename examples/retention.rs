use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::arrow::{datatypes::DataType, record_batch::RecordBatch};
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_uba::create_retention_count;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context()?;

    ctx.table("event").await?;

    ctx.register_udaf(create_retention_count());

    let results = ctx
        .sql(
            "select distinct_id,retention_count(\
                case when event='add' and ds=20230101 then true else false end,\
                case when event='buy' and ds between 20230101 and 20230102 then true else false end,\
                ds-20230101 \
                ) as stats from event group by distinct_id",
        )
        .await?
        .collect()
        .await?;

    print_batches(&results)?;
    Ok(())
}

fn create_context() -> Result<SessionContext> {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::MemTable;
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("distinct_id", DataType::Int32, false),
        Field::new("event", DataType::Utf8, false),
        Field::new("ds", DataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["add", "add", "add"])),
            Arc::new(Int32Array::from(vec![20230101, 20230101, 20230101])),
        ],
    )?;

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["buy", "buy", "buy"])),
            Arc::new(Int32Array::from(vec![20230101, 20230101, 20230101])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("event", Arc::new(provider))?;
    Ok(ctx)
}
