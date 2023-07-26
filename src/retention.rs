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

use datafusion::arrow::datatypes::Field;
use datafusion::arrow::{array::ArrayRef, datatypes::DataType};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use std::sync::Arc;

#[derive(Debug)]
pub struct RetentionCount {
    born_event: Vec<ScalarValue>,
    target_event: Vec<ScalarValue>,
    max_unit: i64,
}

impl RetentionCount {
    pub fn new() -> Self {
        RetentionCount {
            born_event: Vec::new(),
            target_event: Vec::new(),
            max_unit: 0,
        }
    }
}

impl Default for RetentionCount {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for RetentionCount {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let time_diff_arr = &values[2];
        (0..time_diff_arr.len()).try_for_each(|index| {
            if let ScalarValue::Int64(Some(time_diff)) =
                ScalarValue::try_from_array(time_diff_arr, index)?
            {
                let time_diff = time_diff as usize;
                if time_diff > self.max_unit as usize {
                    self.max_unit = time_diff as i64;
                    self.born_event
                        .resize(self.max_unit as usize, ScalarValue::UInt8(Some(0)));
                    self.target_event
                        .resize(self.max_unit as usize, ScalarValue::UInt8(Some(0)));
                }

                if let ScalarValue::Boolean(Some(born_event)) =
                    ScalarValue::try_from_array(&values[0], index)?
                {
                    if born_event {
                        self.born_event
                            .insert(time_diff, ScalarValue::UInt8(Some(1)));
                    }
                }
                if let ScalarValue::Boolean(Some(event)) =
                    ScalarValue::try_from_array(&values[1], index)?
                {
                    if event {
                        self.target_event
                            .insert(time_diff, ScalarValue::UInt8(Some(1)));
                    }
                }
            }
            Ok(())
        })
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::new_list(
            Some(vec![
                ScalarValue::new_list(Some(self.born_event.clone()), DataType::UInt8),
                ScalarValue::new_list(Some(self.target_event.clone()), DataType::UInt8),
            ]),
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::new_list(Some(self.born_event.clone()), DataType::UInt8),
            ScalarValue::new_list(Some(self.target_event.clone()), DataType::UInt8),
            ScalarValue::Int64(Some(self.max_unit)),
        ])
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

            if let (
                ScalarValue::List(Some(v1), _),
                ScalarValue::List(Some(v2), _),
                ScalarValue::Int64(Some(max_unit)),
            ) = (&v[0], &v[1], &v[2])
            {
                if self.max_unit < *max_unit {
                    self.max_unit = *max_unit;
                    self.born_event
                        .resize(self.max_unit as usize, ScalarValue::UInt8(Some(0)));
                    self.target_event
                        .resize(self.max_unit as usize, ScalarValue::UInt8(Some(0)));
                }

                for (index, val) in v1.iter().enumerate() {
                    self.born_event.insert(index, val.clone());
                }

                for (index, val) in v2.iter().enumerate() {
                    self.target_event.insert(index, val.clone());
                }
            }
            Ok(())
        })
    }
}
