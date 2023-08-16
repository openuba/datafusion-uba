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
                                let mut target_v = target_active.get(t_index).unwrap();

                                if target_active.get(t_index).unwrap()
                                    == &ScalarValue::UInt8(Some(2))
                                {
                                    if b_index == t_index {
                                        target_v = &ScalarValue::UInt8(Some(0));
                                    } else {
                                        target_v = &ScalarValue::UInt8(Some(1));
                                    }
                                }
                                if target_active.get(t_index).unwrap()
                                    > &ScalarValue::UInt8(Some(1))
                                    && b_index == t_index
                                {
                                    active.push(ScalarValue::Int64(Some(0)))
                                } else {
                                    let x = val.bitand(target_v)?;
                                    if let ScalarValue::UInt8(Some(val)) = x {
                                        active.push(ScalarValue::Int64(Some(val as i64)))
                                    } else {
                                        unreachable!()
                                    }
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
            }
        }
        let final_result = final_result
            .iter()
            .map(|scalars| ScalarValue::new_list(Some(scalars.clone()), DataType::Int64))
            .collect();
        Ok(ScalarValue::new_list(
            Some(final_result),
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        ))
    }

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
