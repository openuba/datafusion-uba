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

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use datafusion_expr::{
    AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature, StateTypeFunction,
    Volatility,
};
use std::sync::Arc;

#[derive(Default, Debug)]
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

impl Accumulator for RetentionCount {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let max_unit_arr = &values[2];
        let time_diff_arr = &values[3];

        if self.max_unit == 0 {
            if let ScalarValue::Int64(Some(max_unit)) =
                ScalarValue::try_from_array(max_unit_arr, 0)?
            {
                self.max_unit = max_unit;
                self.born_event
                    .resize(self.max_unit as usize, ScalarValue::UInt8(Some(0)));
                self.target_event
                    .resize(self.max_unit as usize, ScalarValue::UInt8(Some(0)));
            }
        }

        (0..time_diff_arr.len()).try_for_each(|index| {
            if let ScalarValue::Int64(Some(time_diff)) =
                ScalarValue::try_from_array(time_diff_arr, index)?
            {
                let time_diff = time_diff as usize;

                if let ScalarValue::Boolean(Some(born_event)) =
                    ScalarValue::try_from_array(&values[0], index)?
                {
                    if born_event {
                        self.born_event[time_diff] = ScalarValue::UInt8(Some(1));
                    }
                }
                if let ScalarValue::Boolean(Some(event)) =
                    ScalarValue::try_from_array(&values[1], index)?
                {
                    if event {
                        self.target_event[time_diff] = ScalarValue::UInt8(Some(1));
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

        if self.max_unit == 0 {}

        let arr = &states[0];

        (0..arr.len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;

            if let (ScalarValue::List(Some(v1), _), ScalarValue::List(Some(v2), _)) = (&v[0], &v[1])
            {
                if self.max_unit == 0 {
                    self.born_event = v1.clone();
                    self.target_event = v2.clone();
                } else {
                    for (index, val) in v1.iter().enumerate() {
                        self.born_event[index] = val.clone();
                    }

                    for (index, val) in v2.iter().enumerate() {
                        self.target_event[index] = val.clone();
                    }
                }
            }

            Ok(())
        })
    }
}

pub fn create_retention_count() -> AggregateUDF {
    let input_type: Signature = Signature::exact(
        vec![
            DataType::Boolean,
            DataType::Boolean,
            DataType::Int64,
            DataType::Int64,
        ],
        Volatility::Immutable,
    );
    let state_type: StateTypeFunction = Arc::new(move |_| {
        Ok(Arc::new(vec![
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            DataType::Int64,
        ])
        .clone())
    });
    let return_type: ReturnTypeFunction = Arc::new(move |_| {
        Ok(Arc::new(DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            true,
        )))))
    });

    let accumulator: AccumulatorFactoryFunction = Arc::new(|_| Ok(Box::new(RetentionCount::new())));
    AggregateUDF::new(
        "retention_count",
        &input_type,
        &return_type,
        &accumulator,
        &state_type,
    )
}
