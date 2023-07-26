use crate::retention::RetentionCount;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_expr::{
    AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature, StateTypeFunction,
    Volatility,
};
use std::sync::Arc;

pub mod retention;
pub mod simple_udaf;

pub fn create_retention_count() -> AggregateUDF {
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
        &Signature::exact(
            vec![DataType::Boolean, DataType::Boolean, DataType::Int64],
            Volatility::Immutable,
        ),
        &return_type,
        &accumulator,
        &state_type,
    )
}
