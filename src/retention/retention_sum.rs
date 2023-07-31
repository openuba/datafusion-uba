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
use arrow::array::Array;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use std::sync::Arc;

/// Retention Sum
///
///    `select retention_sum(stats) from retention_count_result ;`
///
/// # Description
///
/// | distinct_id |      stats    |
/// |-------------|---------------|
/// |    1        | [[1,1],[1,0]] |
/// |    2        | [[1,0],[0,1]] |
///
/// assume each batch just one distinct_id's stats:
/// after retention_sum `update_batch`:
///
/// |     update_batch  |
/// |-------------------|
/// | [[1,1],[1,0],[0]] |
/// | [[1,0],[0,1],[0]] |
///
/// after retention_sum `evaluate`:
///
/// |      evaluate     |
/// |-------------------|
/// | [[2,1],[1,1],[0]] |
///
///
///
///
#[derive(Default, Debug)]
pub struct RetentionSum {
    total_active: Vec<ScalarValue>,
}

impl RetentionSum {
    pub fn new() -> Self {
        RetentionSum {
            total_active: Vec::new(),
        }
    }
}

impl Accumulator for RetentionSum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let active_by_event = &values[0];

        if active_by_event.is_empty() {
            return Ok(());
        }

        for index in 0..active_by_event.len() {
            if let ScalarValue::List(Some(v), _) =
                ScalarValue::try_from_array(active_by_event, index)?
            {
                let mut total_active: Vec<ScalarValue> = Vec::new();
                let mut total_active_per_user = Vec::new();

                if let (
                    ScalarValue::List(Some(born_active), _),
                    ScalarValue::List(Some(target_active), _),
                ) = (&v[0], &v[1])
                {
                    for (b_index, val) in born_active.iter().enumerate() {
                        if let ScalarValue::UInt8(Some(stat)) = val {
                            total_active.push(ScalarValue::Int64(Some(*stat as i64)));
                            let mut active: Vec<ScalarValue> = Vec::new();
                            for t_index in b_index..target_active.len() {
                                let x =
                                    val.bitand(target_active.get(t_index).unwrap())?;
                                if let ScalarValue::UInt8(Some(val)) = x {
                                    active.push(ScalarValue::Int64(Some(val as i64)))
                                } else {
                                    unreachable!()
                                }
                            }

                            total_active_per_user.push(ScalarValue::new_list(
                                Some(active),
                                DataType::List(Arc::new(Field::new(
                                    "item",
                                    DataType::Int64,
                                    true,
                                ))),
                            ));
                        } else {
                            unreachable!()
                        }
                    }
                    total_active_per_user.insert(
                        0,
                        ScalarValue::new_list(Some(total_active), DataType::Int64),
                    );

                    self.total_active.push(ScalarValue::new_list(
                        Some(total_active_per_user),
                        DataType::List(Arc::new(Field::new(
                            "item",
                            DataType::Int64,
                            true,
                        ))),
                    ));
                }
            } else {
                unreachable!()
            }
        }
        println!("self.total_active:{:?}", self.total_active);

        Ok(())
    }
    fn evaluate(&self) -> Result<ScalarValue> {
        let arr_ref = &ScalarValue::iter_to_array(self.total_active.clone()).unwrap();
        let mut final_result: Vec<Vec<ScalarValue>> = Vec::new();

        for index in 0..arr_ref.len() {
            if let ScalarValue::List(Some(per_user), _) =
                ScalarValue::try_from_array(arr_ref, index)?
            {
                if final_result.is_empty() {
                    final_result.resize(per_user.len(), Vec::new());
                }
                println!("per_user:{:?}", per_user);
                for (index, part) in per_user.iter().enumerate() {
                    if let ScalarValue::List(Some(target), _) = part {
                        let mut total_cnt = final_result.get(index).unwrap().clone();

                        if total_cnt.is_empty() {
                            total_cnt.resize(target.len(), ScalarValue::Int64(Some(0)));
                        }

                        for (index, val) in target.iter().enumerate() {
                            total_cnt[index] = total_cnt[index].add(val.clone()).unwrap()
                        }
                        final_result[index] = total_cnt;
                    }
                }

                // if let (
                //     ScalarValue::List(Some(total), _),
                //     ScalarValue::List(Some(born), _),
                // ) = (&per_user[0], &per_user[1])
                // {
                //     if total_cnt.is_empty() {
                //         total_cnt.resize(total.len(), ScalarValue::Int64(Some(0)));
                //         born_cnt.resize(total.len(), ScalarValue::Int64(Some(0)));
                //         target_cnt.resize(total.len() - 1, ScalarValue::Int64(Some(0)));
                //     }
                //     for (index, val) in total.iter().enumerate() {
                //         total_cnt[index] = total_cnt[index].add(val.clone()).unwrap()
                //     }
                //     for (index, val) in born.iter().enumerate() {
                //         born_cnt[index] = born_cnt[index].add(val.clone()).unwrap()
                //     }
                // }
                // // just more than 1 day retention
                // if per_user.len() == 3 {
                //     if let ScalarValue::List(Some(target), _) = &per_user[2] {
                //         for (index, val) in target.iter().enumerate() {
                //             target_cnt[index] =
                //                 target_cnt[index].add(val.clone()).unwrap()
                //         }
                //     }
                // }
            }
        }
        println!("final_result:{:?}", final_result);
        let final_result = final_result
            .iter()
            .map(|scalars| ScalarValue::new_list(Some(scalars.clone()), DataType::Int64))
            .collect();
        Ok(ScalarValue::new_list(
            Some(final_result),
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        ))
    }
    //
    // fn evaluate1(&self) -> Result<ScalarValue> {
    //     let arr_ref = &ScalarValue::iter_to_array(self.total_active.clone()).unwrap();
    //     let mut total_cnt: Vec<ScalarValue> = Vec::new();
    //     let mut born_cnt: Vec<ScalarValue> = Vec::new();
    //     let mut target_cnt: Vec<ScalarValue> = Vec::new();
    //     for index in 0..arr_ref.len() {
    //         if let ScalarValue::List(Some(per_user), _) =
    //             ScalarValue::try_from_array(arr_ref, index)?
    //         {
    //             if let (
    //                 ScalarValue::List(Some(total), _),
    //                 ScalarValue::List(Some(born), _),
    //             ) = (&per_user[0], &per_user[1])
    //             {
    //                 if total_cnt.is_empty() {
    //                     total_cnt.resize(total.len(), ScalarValue::Int64(Some(0)));
    //                     born_cnt.resize(total.len(), ScalarValue::Int64(Some(0)));
    //                     target_cnt.resize(total.len() - 1, ScalarValue::Int64(Some(0)));
    //                 }
    //                 for (index, val) in total.iter().enumerate() {
    //                     total_cnt[index] = total_cnt[index].add(val.clone()).unwrap()
    //                 }
    //                 for (index, val) in born.iter().enumerate() {
    //                     born_cnt[index] = born_cnt[index].add(val.clone()).unwrap()
    //                 }
    //             }
    //             // just more than 1 day retention
    //             if per_user.len() == 3 {
    //                 if let ScalarValue::List(Some(target), _) = &per_user[2] {
    //                     for (index, val) in target.iter().enumerate() {
    //                         target_cnt[index] =
    //                             target_cnt[index].add(val.clone()).unwrap()
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //
    //     let mut final_result: Vec<ScalarValue> = Vec::new();
    //
    //     final_result.push(ScalarValue::new_list(Some(total_cnt), DataType::Int64));
    //     final_result.push(ScalarValue::new_list(Some(born_cnt), DataType::Int64));
    //     if !target_cnt.is_empty() {
    //         final_result.push(ScalarValue::new_list(Some(target_cnt), DataType::Int64));
    //     }
    //
    //     Ok(ScalarValue::new_list(
    //         Some(final_result),
    //         DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
    //     ))
    // }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::new_list(
            Some(self.total_active.clone()),
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ))),
        )])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        let arr = &states[0];
        (0..arr.len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            if let ScalarValue::List(Some(per_user), _) = &v[0] {
                if !per_user.is_empty() {
                    for p in per_user {
                        self.total_active.push(p.clone());
                    }
                }
            } else {
                unreachable!("")
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::retention::create_retention_sum;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, FieldRef, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::datasource::MemTable;
    use datafusion::error::Result;
    use datafusion::prelude::*;
    use datafusion_common::ScalarValue;
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
