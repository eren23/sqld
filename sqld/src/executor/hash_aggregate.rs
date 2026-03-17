use std::collections::{HashMap, HashSet};

use crate::planner::logical_plan::{AggregateExpr, AggregateFunc};
use crate::sql::ast::Expr;
use crate::types::{Datum, Schema, Tuple};
use crate::utils::error::Result;

use super::executor::{intermediate_tuple, Executor};
use super::expr_eval::{compile_expr, evaluate_expr, ExprOp};

// ---------------------------------------------------------------------------
// HashAggregate — hash table keyed by GROUP BY expressions
// ---------------------------------------------------------------------------

pub struct HashAggregateExecutor {
    child: Box<dyn Executor>,
    group_by_src: Vec<Expr>,
    aggregates: Vec<AggregateExpr>,
    schema: Schema,

    group_by_ops: Vec<Vec<ExprOp>>,
    agg_input_ops: Vec<Vec<ExprOp>>,
    results: Vec<Tuple>,
    position: usize,
    initialized: bool,
}

impl HashAggregateExecutor {
    pub fn new(
        child: Box<dyn Executor>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        schema: Schema,
    ) -> Self {
        Self {
            child,
            group_by_src: group_by,
            aggregates,
            schema,
            group_by_ops: Vec::new(),
            agg_input_ops: Vec::new(),
            results: Vec::new(),
            position: 0,
            initialized: false,
        }
    }
}

impl Executor for HashAggregateExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        let input_schema = self.child.schema().clone();

        // Compile GROUP BY expressions
        self.group_by_ops = self
            .group_by_src
            .iter()
            .map(|e| compile_expr(e, &input_schema))
            .collect::<Result<Vec<_>>>()?;

        // Compile aggregate input expressions
        self.agg_input_ops = self
            .aggregates
            .iter()
            .map(|ae| compile_expr(&ae.arg, &input_schema))
            .collect::<Result<Vec<_>>>()?;

        // Build hash table: group_key → Vec<Accumulator>
        let mut groups: HashMap<Vec<Datum>, Vec<Accumulator>> = HashMap::new();
        // Track insertion order for deterministic output
        let mut group_order: Vec<Vec<Datum>> = Vec::new();

        while let Some(tuple) = self.child.next()? {
            // Extract group key
            let key: Vec<Datum> = self
                .group_by_ops
                .iter()
                .map(|ops| evaluate_expr(ops, &tuple))
                .collect::<Result<Vec<_>>>()?;

            // Extract aggregate input values
            let agg_vals: Vec<Datum> = self
                .agg_input_ops
                .iter()
                .map(|ops| evaluate_expr(ops, &tuple))
                .collect::<Result<Vec<_>>>()?;

            // Get or create accumulators for this group
            let accums = groups.entry(key.clone()).or_insert_with(|| {
                group_order.push(key.clone());
                self.aggregates
                    .iter()
                    .map(|ae| Accumulator::new(ae.func, ae.distinct))
                    .collect()
            });

            // Feed values to accumulators
            for (i, acc) in accums.iter_mut().enumerate() {
                acc.accumulate(&agg_vals[i])?;
            }
        }

        // If no input rows and no group by, emit one row with initial aggregate values
        if groups.is_empty() && self.group_by_src.is_empty() {
            let accums: Vec<Accumulator> = self
                .aggregates
                .iter()
                .map(|ae| Accumulator::new(ae.func, ae.distinct))
                .collect();
            let mut row = Vec::new();
            for acc in accums {
                row.push(acc.finalize()?);
            }
            self.results = vec![intermediate_tuple(row)];
        } else {
            // Build result tuples
            self.results.clear();
            for key in &group_order {
                let accums = groups.get(key).unwrap();
                let mut row = key.clone();
                for acc in accums {
                    row.push(acc.finalize()?);
                }
                self.results.push(intermediate_tuple(row));
            }
        }

        self.position = 0;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.position < self.results.len() {
            let t = self.results[self.position].clone();
            self.position += 1;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<()> {
        self.results.clear();
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ---------------------------------------------------------------------------
// Accumulator — per-aggregate state machine
// ---------------------------------------------------------------------------

pub struct Accumulator {
    func: AggregateFunc,
    _distinct: bool,
    count: i64,
    sum: Option<Datum>,
    min_val: Option<Datum>,
    max_val: Option<Datum>,
    // For DISTINCT support
    seen: Option<HashSet<DatumKey>>,
    // For STRING_AGG / ARRAY_AGG / BOOL_AND / BOOL_OR
    values: Vec<Datum>,
}

/// Wrapper for Datum that implements Hash+Eq for use in HashSet.
#[derive(Clone, Debug)]
struct DatumKey(Datum);

impl std::hash::Hash for DatumKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialEq for DatumKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for DatumKey {}

impl Accumulator {
    pub fn new(func: AggregateFunc, distinct: bool) -> Self {
        Self {
            func,
            _distinct: distinct,
            count: 0,
            sum: None,
            min_val: None,
            max_val: None,
            seen: if distinct { Some(HashSet::new()) } else { None },
            values: Vec::new(),
        }
    }

    pub fn accumulate(&mut self, val: &Datum) -> Result<()> {
        // DISTINCT: skip if already seen
        if let Some(ref mut seen) = self.seen {
            if !val.is_null() {
                let key = DatumKey(val.clone());
                if !seen.insert(key) {
                    return Ok(());
                }
            }
        }

        match self.func {
            AggregateFunc::Count => {
                if !val.is_null() {
                    self.count += 1;
                }
            }
            AggregateFunc::Sum => {
                if !val.is_null() {
                    self.sum = Some(match self.sum.take() {
                        None => val.clone(),
                        Some(cur) => cur.add(val)?,
                    });
                }
            }
            AggregateFunc::Avg => {
                if !val.is_null() {
                    self.count += 1;
                    self.sum = Some(match self.sum.take() {
                        None => val.clone(),
                        Some(cur) => cur.add(val)?,
                    });
                }
            }
            AggregateFunc::Min => {
                if !val.is_null() {
                    self.min_val = Some(match self.min_val.take() {
                        None => val.clone(),
                        Some(cur) => {
                            if val.sql_cmp(&cur)? == Some(std::cmp::Ordering::Less) {
                                val.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
            }
            AggregateFunc::Max => {
                if !val.is_null() {
                    self.max_val = Some(match self.max_val.take() {
                        None => val.clone(),
                        Some(cur) => {
                            if val.sql_cmp(&cur)? == Some(std::cmp::Ordering::Greater) {
                                val.clone()
                            } else {
                                cur
                            }
                        }
                    });
                }
            }
            AggregateFunc::StringAgg | AggregateFunc::ArrayAgg => {
                if !val.is_null() {
                    self.values.push(val.clone());
                }
            }
            AggregateFunc::BoolAnd => {
                if !val.is_null() {
                    self.values.push(val.clone());
                }
            }
            AggregateFunc::BoolOr => {
                if !val.is_null() {
                    self.values.push(val.clone());
                }
            }
        }
        Ok(())
    }

    pub fn finalize(&self) -> Result<Datum> {
        match self.func {
            AggregateFunc::Count => Ok(Datum::BigInt(self.count)),
            AggregateFunc::Sum => Ok(self.sum.clone().unwrap_or(Datum::Null)),
            AggregateFunc::Avg => {
                if self.count == 0 {
                    Ok(Datum::Null)
                } else {
                    let sum = self.sum.clone().unwrap_or(Datum::Null);
                    // Convert to float for division
                    let sum_f = super::expr_eval::datum_to_f64(&sum)?;
                    Ok(Datum::Float(sum_f / self.count as f64))
                }
            }
            AggregateFunc::Min => Ok(self.min_val.clone().unwrap_or(Datum::Null)),
            AggregateFunc::Max => Ok(self.max_val.clone().unwrap_or(Datum::Null)),
            AggregateFunc::StringAgg => {
                if self.values.is_empty() {
                    Ok(Datum::Null)
                } else {
                    let parts: Vec<String> = self.values.iter().map(|d| format!("{d}")).collect();
                    Ok(Datum::Text(parts.join(",")))
                }
            }
            AggregateFunc::ArrayAgg => {
                if self.values.is_empty() {
                    Ok(Datum::Null)
                } else {
                    let parts: Vec<String> = self.values.iter().map(|d| format!("{d}")).collect();
                    Ok(Datum::Text(format!("{{{}}}", parts.join(","))))
                }
            }
            AggregateFunc::BoolAnd => {
                if self.values.is_empty() {
                    Ok(Datum::Null)
                } else {
                    let result = self.values.iter().all(|v| matches!(v, Datum::Boolean(true)));
                    Ok(Datum::Boolean(result))
                }
            }
            AggregateFunc::BoolOr => {
                if self.values.is_empty() {
                    Ok(Datum::Null)
                } else {
                    let result = self.values.iter().any(|v| matches!(v, Datum::Boolean(true)));
                    Ok(Datum::Boolean(result))
                }
            }
        }
    }
}
