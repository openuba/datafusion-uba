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

use datafusion::arrow::array::Array;
/// In this example we will declare a single-type, single return type UDAF that computes the geometric mean.
/// The geometric mean is described here: https://en.wikipedia.org/wiki/Geometric_mean
use datafusion::arrow::array::ArrayRef;
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};

#[derive(Default, Debug)]
pub struct GeometricMean {
    n: u32,
    prod: f64,
}

impl GeometricMean {
    // how the struct is initialized
    pub fn new() -> Self {
        GeometricMean { n: 0, prod: 1.0 }
    }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions
// to use them.
impl Accumulator for GeometricMean {
    // This function serializes our state to `ScalarValue`, which DataFusion uses
    // to pass this state between execution stages.
    // Note that this can be arbitrary data.
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.prod),
            ScalarValue::from(self.n),
        ])
    }

    // DataFusion expects this function to return the final value of this aggregator.
    // in this case, this is the formula of the geometric mean
    fn evaluate(&self) -> Result<ScalarValue> {
        let value = self.prod / f64::from(self.n);
        Ok(ScalarValue::from(value))
    }

    // DataFusion calls this function to update the accumulator's state for a batch
    // of inputs rows. In this case the product is updated with values from the first column
    // and the count is updated based on the row count
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let bool_arr = &values[1];

        let f32_arr = &values[0];

        (0..f32_arr.len()).try_for_each(|index| {
            let v = ScalarValue::try_from_array(f32_arr, index)?;
            let bv = ScalarValue::try_from_array(bool_arr, index)?;

            if let ScalarValue::Boolean(Some(value)) = bv {
                if value {
                    if let ScalarValue::Float64(Some(value)) = v {
                        self.prod *= value;
                        self.n += 1;
                    } else {
                        unreachable!("")
                    }
                }
            }
            Ok(())
        })
    }

    // Optimization hint: this trait also supports `update_batch` and `merge_batch`,
    // that can be used to perform these operations on arrays instead of single values.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        let f64_arr = &states[0];
        (0..f64_arr.len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            if let (ScalarValue::Float64(Some(prod)), ScalarValue::UInt32(Some(n))) =
                (&v[0], &v[1])
            {
                self.prod *= prod;
                self.n += n;
            } else {
                unreachable!("")
            }
            Ok(())
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::simple_udaf::GeometricMean;
    use datafusion::arrow::array::{BooleanArray, StringArray};
    use datafusion::arrow::util::pretty::print_batches;
    use datafusion::arrow::{
        array::Float32Array, datatypes::DataType, record_batch::RecordBatch,
    };
    use datafusion::error::Result;
    use datafusion::{logical_expr::Volatility, prelude::*};
    use datafusion_expr::{
        AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature,
        StateTypeFunction,
    };
    use std::sync::Arc;

    // create local session context with an in-memory table
    fn create_context() -> Result<SessionContext> {
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::datasource::MemTable;
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Utf8, false),
        ]));

        // define data in two partitions
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float32Array::from(vec![2.0, 4.0, 8.0])),
                Arc::new(BooleanArray::from(vec![true, true, true])),
                Arc::new(StringArray::from(vec!["a", "a", "b"])),
            ],
        )?;
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float32Array::from(vec![64.0])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(StringArray::from(vec!["b"])),
            ],
        )?;

        // declare a new context. In spark API, this corresponds to a new spark SQLsession
        let ctx = SessionContext::new();

        // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
        let provider = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]])?;
        ctx.register_table("t", Arc::new(provider))?;
        Ok(ctx)
    }

    #[tokio::test]
    async fn simple_udaf() -> Result<()> {
        let ctx = create_context()?;

        let state_type: Arc<Vec<DataType>> =
            Arc::new(vec![DataType::Float64, DataType::UInt32]);
        let state_type: StateTypeFunction = Arc::new(move |_| Ok(state_type.clone()));

        let return_type: ReturnTypeFunction =
            Arc::new(move |_| Ok(Arc::new(DataType::Float64)));
        let accumulator: AccumulatorFactoryFunction =
            Arc::new(|_| Ok(Box::new(GeometricMean::new())));
        let geometric_mean = AggregateUDF::new(
            "geo_mean",
            &Signature::exact(
                vec![DataType::Float64, DataType::Boolean],
                Volatility::Immutable,
            ),
            &return_type,
            &accumulator,
            &state_type,
        );

        // get a DataFrame from the context
        // this table has 1 column `a` f32 with values {2,4,8,64}, whose geometric mean is 8.0.
        let df = ctx.table("t").await?;

        // perform the aggregation
        let df = df.aggregate(
            vec![col("c")],
            vec![geometric_mean.call(vec![col("a"), col("b")])],
        )?;

        // note that "a" is f32, not f64. DataFusion coerces it to match the UDAF's signature.

        // execute the query
        let results = df.collect().await?;

        let _ = print_batches(&results);

        Ok(())
    }

    #[tokio::test]
    async fn a1() -> Result<()> {
        let mut v = Vec::new();

        v.insert(0, 1);
        v.resize(1, 0);
        println!("{:?}", v);
        Ok(())
    }
}
