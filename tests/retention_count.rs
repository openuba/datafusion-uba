#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::datasource::MemTable;
    use datafusion::error::Result;
    use datafusion::prelude::*;
    use datafusion_uba::retention::create_retention_count;
    use std::sync::Arc;
    #[tokio::test]
    async fn retention_count_1days() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("distinct_id", DataType::Int32, false),
            Field::new("event", DataType::Utf8, false),
            Field::new("ds", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["add", "buy"])),
                Arc::new(Int32Array::from(vec![20230101, 20230101])),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 2])),
                Arc::new(StringArray::from(vec!["add", "buy"])),
                Arc::new(Int32Array::from(vec![20230101, 20230101])),
            ],
        )?;

        let ctx = SessionContext::new();

        let provider = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]])?;

        ctx.register_table("event", Arc::new(provider))?;

        ctx.register_udaf(create_retention_count());

        let actual = ctx
            .sql(
                "select distinct_id,retention_count(\
                    case when event='add' then true else false end,\
                    case when event='buy' then true else false end,\
                    20230101-20230101,\
                    ds-20230101 \
                    ) as stats \
                from event group by distinct_id order by distinct_id",
            )
            .await?
            .collect()
            .await?;

        #[rustfmt::skip]
            let expected = vec![
            "+-------------+------------+",
            "| distinct_id | stats      |",
            "+-------------+------------+",
            "| 1           | [[1], [1]] |",
            "| 2           | [[1], [1]] |",
            "+-------------+------------+",
        ];
        assert_batches_eq!(expected, &actual);

        Ok(())
    }
    #[tokio::test]
    async fn retention_count_2days() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("distinct_id", DataType::Int32, false),
            Field::new("event", DataType::Utf8, false),
            Field::new("ds", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(StringArray::from(vec!["add", "add", "buy"])),
                Arc::new(Int32Array::from(vec![20230101, 20230102, 20230101])),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 2])),
                Arc::new(StringArray::from(vec!["add", "buy"])),
                Arc::new(Int32Array::from(vec![20230101, 20230102])),
            ],
        )?;

        let ctx = SessionContext::new();

        let provider = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]])?;

        ctx.register_table("event", Arc::new(provider))?;

        ctx.register_udaf(create_retention_count());

        let actual = ctx
            .sql(
                "select distinct_id,retention_count(\
                    case when event='add' then true else false end,\
                    case when event='buy' then true else false end,\
                    20230102-20230101,\
                    ds-20230101 \
                    ) as stats \
                from event group by distinct_id order by distinct_id",
            )
            .await?
            .collect()
            .await?;

        #[rustfmt::skip]
        let expected = vec![
            "+-------------+------------------+",
            "| distinct_id | stats            |",
            "+-------------+------------------+",
            "| 1           | [[1, 1], [1, 0]] |",
            "| 2           | [[1, 0], [0, 1]] |",
            "+-------------+------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        Ok(())
    }
    #[tokio::test]
    async fn retention_count_born_target_event_are_same() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("distinct_id", DataType::Int32, false),
            Field::new("event", DataType::Utf8, false),
            Field::new("ds", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["add", "add"])),
                Arc::new(Int32Array::from(vec![20230101, 20230102])),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 2])),
                Arc::new(StringArray::from(vec!["add", "add"])),
                Arc::new(Int32Array::from(vec![20230101, 20230102])),
            ],
        )?;

        let ctx = SessionContext::new();

        let provider = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]])?;

        ctx.register_table("event", Arc::new(provider))?;

        ctx.register_udaf(create_retention_count());

        let actual = ctx
            .sql(
                "select distinct_id,retention_count(\
                    case when event='add' then true else false end,\
                    case when event='add' then true else false end,\
                    20230102-20230101,\
                    ds-20230101 \
                    ) as stats \
                from event group by distinct_id order by distinct_id",
            )
            .await?
            .collect()
            .await?;

        #[rustfmt::skip]
            let expected = vec![
            "+-------------+------------------+",
            "| distinct_id | stats            |",
            "+-------------+------------------+",
            "| 1           | [[1, 1], [2, 2]] |",
            "| 2           | [[1, 1], [2, 2]] |",
            "+-------------+------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        Ok(())
    }
}
