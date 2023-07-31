// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, FieldRef, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::datasource::MemTable;
    use datafusion::error::Result;
    use datafusion::prelude::*;
    use datafusion_common::ScalarValue;
    use datafusion_uba::retention::create_retention_sum;
    use std::sync::Arc;

    #[tokio::test]
    async fn retention_sum_1days() -> Result<()> {
        // [[1, 1], [1, 0]]
        let u1_stats = ScalarValue::new_list(
            Some(vec![
                ScalarValue::new_list(
                    Some(vec![ScalarValue::UInt8(Some(1))]),
                    DataType::UInt8,
                ),
                ScalarValue::new_list(
                    Some(vec![ScalarValue::UInt8(Some(1))]),
                    DataType::UInt8,
                ),
            ]),
            DataType::List(FieldRef::new(Field::new("item", DataType::UInt8, true))),
        );

        // [[1, 0], [0, 1]]
        let u2_stats = ScalarValue::new_list(
            Some(vec![
                ScalarValue::new_list(
                    Some(vec![ScalarValue::UInt8(Some(1))]),
                    DataType::UInt8,
                ),
                ScalarValue::new_list(
                    Some(vec![ScalarValue::UInt8(Some(1))]),
                    DataType::UInt8,
                ),
            ]),
            DataType::List(FieldRef::new(Field::new("item", DataType::UInt8, true))),
        );

        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("distinct_id", DataType::Int32, false),
            Field::new(
                "stats",
                DataType::List(FieldRef::new(Field::new(
                    "item",
                    DataType::List(FieldRef::new(Field::new(
                        "item",
                        DataType::UInt8,
                        true,
                    ))),
                    true,
                ))),
                true,
            ),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(u1_stats.to_array()),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![2])), u2_stats.to_array()],
        )?;

        let ctx = SessionContext::new();

        let provider = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]])?;
        ctx.register_table("retention_count_result", Arc::new(provider))?;

        ctx.register_udaf(create_retention_sum());

        let actual = ctx
            .sql("select * from retention_count_result")
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

        let actual = ctx
            .sql("select retention_sum(stats) from retention_count_result")
            .await?
            .collect()
            .await?;

        #[rustfmt::skip]
            let expected = vec![
            "+---------------------------------------------+",
            "| retention_sum(retention_count_result.stats) |",
            "+---------------------------------------------+",
            "| [[2], [2]]                                  |",
            "+---------------------------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn retention_sum_2days() -> Result<()> {
        // [[1, 1], [1, 0]]
        let u1_stats = ScalarValue::new_list(
            Some(vec![
                ScalarValue::new_list(
                    Some(vec![
                        ScalarValue::UInt8(Some(1)),
                        ScalarValue::UInt8(Some(1)),
                    ]),
                    DataType::UInt8,
                ),
                ScalarValue::new_list(
                    Some(vec![
                        ScalarValue::UInt8(Some(1)),
                        ScalarValue::UInt8(Some(0)),
                    ]),
                    DataType::UInt8,
                ),
            ]),
            DataType::List(FieldRef::new(Field::new("item", DataType::UInt8, true))),
        );

        // [[1, 0], [0, 1]]
        let u2_stats = ScalarValue::new_list(
            Some(vec![
                ScalarValue::new_list(
                    Some(vec![
                        ScalarValue::UInt8(Some(1)),
                        ScalarValue::UInt8(Some(0)),
                    ]),
                    DataType::UInt8,
                ),
                ScalarValue::new_list(
                    Some(vec![
                        ScalarValue::UInt8(Some(0)),
                        ScalarValue::UInt8(Some(1)),
                    ]),
                    DataType::UInt8,
                ),
            ]),
            DataType::List(FieldRef::new(Field::new("item", DataType::UInt8, true))),
        );

        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("distinct_id", DataType::Int32, false),
            Field::new(
                "stats",
                DataType::List(FieldRef::new(Field::new(
                    "item",
                    DataType::List(FieldRef::new(Field::new(
                        "item",
                        DataType::UInt8,
                        true,
                    ))),
                    true,
                ))),
                true,
            ),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(u1_stats.to_array()),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![2])), u2_stats.to_array()],
        )?;

        let ctx = SessionContext::new();

        let provider = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]])?;
        ctx.register_table("retention_count_result", Arc::new(provider))?;

        ctx.register_udaf(create_retention_sum());

        let actual = ctx
            .sql("select * from retention_count_result")
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

        let actual = ctx
            .sql("select retention_sum(stats) from retention_count_result")
            .await?
            .collect()
            .await?;

        #[rustfmt::skip]
            let expected = vec![
            "+---------------------------------------------+",
            "| retention_sum(retention_count_result.stats) |",
            "+---------------------------------------------+",
            "| [[2, 1], [1, 1], [0]]                       |",
            "+---------------------------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }

    #[tokio::test]
    async fn retention_sum_3days() -> Result<()> {
        // [[1, 1, 1], [1, 1, 0]]
        let u1_stats = ScalarValue::new_list(
            Some(vec![
                ScalarValue::new_list(
                    Some(vec![
                        ScalarValue::UInt8(Some(1)),
                        ScalarValue::UInt8(Some(1)),
                        ScalarValue::UInt8(Some(1)),
                    ]),
                    DataType::UInt8,
                ),
                ScalarValue::new_list(
                    Some(vec![
                        ScalarValue::UInt8(Some(1)),
                        ScalarValue::UInt8(Some(1)),
                        ScalarValue::UInt8(Some(0)),
                    ]),
                    DataType::UInt8,
                ),
            ]),
            DataType::List(FieldRef::new(Field::new("item", DataType::UInt8, true))),
        );

        // [[1, 0, 0], [0, 0, 1]]
        let u2_stats = ScalarValue::new_list(
            Some(vec![
                ScalarValue::new_list(
                    Some(vec![
                        ScalarValue::UInt8(Some(1)),
                        ScalarValue::UInt8(Some(0)),
                        ScalarValue::UInt8(Some(0)),
                    ]),
                    DataType::UInt8,
                ),
                ScalarValue::new_list(
                    Some(vec![
                        ScalarValue::UInt8(Some(0)),
                        ScalarValue::UInt8(Some(0)),
                        ScalarValue::UInt8(Some(1)),
                    ]),
                    DataType::UInt8,
                ),
            ]),
            DataType::List(FieldRef::new(Field::new("item", DataType::UInt8, true))),
        );

        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("distinct_id", DataType::Int32, false),
            Field::new(
                "stats",
                DataType::List(FieldRef::new(Field::new(
                    "item",
                    DataType::List(FieldRef::new(Field::new(
                        "item",
                        DataType::UInt8,
                        true,
                    ))),
                    true,
                ))),
                true,
            ),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(u1_stats.to_array()),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![2])), u2_stats.to_array()],
        )?;

        let ctx = SessionContext::new();

        let provider = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]])?;
        ctx.register_table("retention_count_result", Arc::new(provider))?;

        ctx.register_udaf(create_retention_sum());

        let actual = ctx
            .sql("select * from retention_count_result")
            .await?
            .collect()
            .await?;

        #[rustfmt::skip]
            let expected = vec![
            "+-------------+------------------------+",
            "| distinct_id | stats                  |",
            "+-------------+------------------------+",
            "| 1           | [[1, 1, 1], [1, 1, 0]] |",
            "| 2           | [[1, 0, 0], [0, 0, 1]] |",
            "+-------------+------------------------+",
        ];
        assert_batches_eq!(expected, &actual);

        let actual = ctx
            .sql("select retention_sum(stats) from retention_count_result")
            .await?
            .collect()
            .await?;

        #[rustfmt::skip]
            let expected = vec![
            "+---------------------------------------------+",
            "| retention_sum(retention_count_result.stats) |",
            "+---------------------------------------------+",
            "| [[2, 1, 1], [1, 1, 1], [1, 0], [0]]         |",
            "+---------------------------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
        Ok(())
    }
}
