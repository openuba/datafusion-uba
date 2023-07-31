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

use crate::retention::retention_count::RetentionCount;
use crate::retention::retention_sum::RetentionSum;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_expr::{
    AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature,
    StateTypeFunction, Volatility,
};
use std::sync::Arc;

pub mod retention_count;
pub mod retention_sum;

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

    let accumulator: AccumulatorFactoryFunction =
        Arc::new(|_| Ok(Box::new(RetentionCount::new())));
    AggregateUDF::new(
        "retention_count",
        &input_type,
        &return_type,
        &accumulator,
        &state_type,
    )
}

pub fn create_retention_sum() -> AggregateUDF {
    let input_type: Signature = Signature::exact(
        vec![DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
            true,
        )))],
        Volatility::Immutable,
    );

    let state_type: StateTypeFunction = Arc::new(move |_| {
        Ok(Arc::new(vec![DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ))),
            true,
        )))])
        .clone())
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| {
        Ok(Arc::new(DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )))))
    });

    let accumulator: AccumulatorFactoryFunction =
        Arc::new(|_| Ok(Box::new(RetentionSum::new())));
    AggregateUDF::new(
        "retention_sum",
        &input_type,
        &return_type,
        &accumulator,
        &state_type,
    )
}
