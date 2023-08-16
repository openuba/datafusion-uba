use std::sync::Arc;

use datafusion::arrow::util::pretty::print_batches;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_uba::retention::{create_retention_count, create_retention_sum};
use datafusion_uba::test_util;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    ctx.register_parquet(
        "event",
        format!("{}/event.parquet", test_util::parquet_test_data()).as_str(),
        Default::default(),
    )
    .await
    .unwrap();

    ctx.register_udaf(create_retention_count());
    ctx.register_udaf(create_retention_sum());

    let df = ctx
        .sql(
            "select distinct_id,retention_count(\
                    case when xwhat='$startup' then true else false end,\
                    case when xwhat='$startup' then true else false end,\
                    20230107-20230101,\
                    ds-20230101 \
                    ) as stats \
                from event where ds between 20230101 and 20230107 and xwhat='$startup' \
                group by distinct_id order by distinct_id",
        )
        .await?;
    let results = df.clone().collect().await?;
    // print_batches(&results);

    let provider = MemTable::try_new(df.schema().clone().into(), vec![results])?;
    ctx.register_table("retention_count_result", Arc::new(provider))?;

    ctx.sql("select * from retention_count_result")
        .await
        .unwrap()
        .show_limit(10)
        .await
        .unwrap();

    let results = ctx
        .sql("select retention_sum(stats) from retention_count_result")
        .await?
        .collect()
        .await?;

    print_batches(&results)?;

    Ok(())
}
