use std::collections::HashMap;

use crate::sql::ast::{Expr, JoinType};
use crate::types::{Datum, Schema, Tuple};
use crate::utils::error::Result;

use super::executor::{intermediate_tuple, Executor};
use super::expr_eval::{compile_expr, evaluate_expr, is_truthy, ExprOp};

// ---------------------------------------------------------------------------
// HashJoin — build/probe for INNER/LEFT/RIGHT/FULL/CROSS/SEMI/ANTI
//            with disk spill on work_mem overflow
// ---------------------------------------------------------------------------

pub struct HashJoinExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    join_type: JoinType,
    left_keys_src: Vec<Expr>,
    right_keys_src: Vec<Expr>,
    condition_src: Option<Expr>,
    schema: Schema,
    work_mem: usize,

    // Compiled expressions (set during init)
    left_key_ops: Vec<Vec<ExprOp>>,
    right_key_ops: Vec<Vec<ExprOp>>,
    condition_ops: Option<Vec<ExprOp>>,

    // Build side: hash table from left child
    hash_table: HashMap<Vec<Datum>, Vec<Vec<Datum>>>,
    left_col_count: usize,
    right_col_count: usize,

    // Spilled left tuples that didn't fit in work_mem
    spill_buffer: Vec<(Vec<Datum>, Vec<Datum>)>, // (key, values)
    mem_used: usize,

    // Probe state
    output_buffer: Vec<Tuple>,
    buffer_pos: usize,
    probe_exhausted: bool,

    // For LEFT/FULL: track which left rows matched
    left_matched: HashMap<Vec<Datum>, Vec<bool>>,
    // Track matched indices in spill buffer
    spill_matched: Vec<bool>,
    // For RIGHT/FULL: track unmatched right rows
    unmatched_right: Vec<Vec<Datum>>,
    // For SEMI: track which left rows have been matched (by key+index)
    semi_matched: HashMap<Vec<Datum>, Vec<bool>>,
    semi_spill_matched: Vec<bool>,

    // State
    phase: JoinPhase,
}

#[derive(Debug, PartialEq)]
enum JoinPhase {
    Uninit,
    Probe,
    SpillProbe,
    EmitUnmatched,
    Done,
}

impl HashJoinExecutor {
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        join_type: JoinType,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
        condition: Option<Expr>,
        schema: Schema,
        work_mem: usize,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            left_keys_src: left_keys,
            right_keys_src: right_keys,
            condition_src: condition,
            schema,
            work_mem,
            left_key_ops: Vec::new(),
            right_key_ops: Vec::new(),
            condition_ops: None,
            hash_table: HashMap::new(),
            left_col_count: 0,
            right_col_count: 0,
            spill_buffer: Vec::new(),
            mem_used: 0,
            output_buffer: Vec::new(),
            buffer_pos: 0,
            probe_exhausted: false,
            left_matched: HashMap::new(),
            spill_matched: Vec::new(),
            unmatched_right: Vec::new(),
            semi_matched: HashMap::new(),
            semi_spill_matched: Vec::new(),
            phase: JoinPhase::Uninit,
        }
    }

    fn build_phase(&mut self) -> Result<()> {
        self.mem_used = 0;

        while let Some(tuple) = self.left.next()? {
            let key = self.extract_key(&self.left_key_ops, &tuple)?;
            let values = tuple.values().to_vec();

            // Estimate memory for this tuple
            let tuple_mem = values.len() * 16 + key.len() * 16 + 64;

            // Check if adding this tuple would exceed work_mem
            if self.mem_used + tuple_mem > self.work_mem && !self.hash_table.is_empty() {
                // Spill: store (key, values) in spill buffer
                self.spill_buffer.push((key, values));
            } else {
                self.mem_used += tuple_mem;

                // Track for LEFT/FULL joins
                if matches!(
                    self.join_type,
                    JoinType::Left | JoinType::Full
                ) {
                    let matched_list =
                        self.left_matched.entry(key.clone()).or_default();
                    matched_list.push(false);
                }

                // Track for SEMI/ANTI join
                if matches!(self.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                    let matched_list =
                        self.semi_matched.entry(key.clone()).or_default();
                    matched_list.push(false);
                }

                self.hash_table.entry(key).or_default().push(values);
            }
        }

        // Initialize spill tracking
        self.spill_matched = vec![false; self.spill_buffer.len()];
        self.semi_spill_matched = vec![false; self.spill_buffer.len()];

        self.phase = JoinPhase::Probe;
        Ok(())
    }

    fn extract_key(
        &self,
        key_ops: &[Vec<ExprOp>],
        tuple: &Tuple,
    ) -> Result<Vec<Datum>> {
        key_ops
            .iter()
            .map(|ops| evaluate_expr(ops, tuple))
            .collect()
    }

    fn probe_next(&mut self) -> Result<Option<Tuple>> {
        loop {
            // Drain output buffer first
            if self.buffer_pos < self.output_buffer.len() {
                let t = self.output_buffer[self.buffer_pos].clone();
                self.buffer_pos += 1;
                return Ok(Some(t));
            }

            if self.probe_exhausted {
                if !self.spill_buffer.is_empty() {
                    self.phase = JoinPhase::SpillProbe;
                } else {
                    self.phase = JoinPhase::EmitUnmatched;
                }
                return Ok(None);
            }

            // Get next right tuple
            match self.right.next()? {
                None => {
                    self.probe_exhausted = true;
                    if !self.spill_buffer.is_empty() {
                        self.phase = JoinPhase::SpillProbe;
                    } else {
                        self.phase = JoinPhase::EmitUnmatched;
                    }
                    return Ok(None);
                }
                Some(right_tuple) => {
                    let right_key =
                        self.extract_key(&self.right_key_ops, &right_tuple)?;
                    let right_vals = right_tuple.values().to_vec();

                    // Skip if any key is NULL (NULLs don't match in equi-joins)
                    if right_key.iter().any(|d| d.is_null()) {
                        if matches!(
                            self.join_type,
                            JoinType::Right | JoinType::Full
                        ) {
                            self.unmatched_right.push(right_vals);
                        }
                        continue;
                    }

                    self.output_buffer.clear();
                    self.buffer_pos = 0;

                    // Also probe spill buffer for matches
                    for (si, (skey, svals)) in
                        self.spill_buffer.iter().enumerate()
                    {
                        if skey == &right_key {
                            let mut combined = svals.clone();
                            combined.extend_from_slice(&right_vals);
                            let candidate = intermediate_tuple(combined);

                            if let Some(ref cond) = self.condition_ops {
                                let result = evaluate_expr(cond, &candidate)?;
                                if !is_truthy(&result) {
                                    continue;
                                }
                            }

                            self.spill_matched[si] = true;
                            self.semi_spill_matched[si] = true;

                            match self.join_type {
                                JoinType::LeftSemi | JoinType::LeftAnti => {}
                                _ => {
                                    self.output_buffer.push(candidate);
                                }
                            }
                        }
                    }

                    let mut matched = false;
                    if let Some(left_rows) = self.hash_table.get(&right_key) {
                        for (idx, left_vals) in left_rows.iter().enumerate() {
                            let mut combined = left_vals.clone();
                            combined.extend_from_slice(&right_vals);
                            let candidate = intermediate_tuple(combined);

                            // Apply residual condition
                            if let Some(ref cond) = self.condition_ops {
                                let result = evaluate_expr(cond, &candidate)?;
                                if !is_truthy(&result) {
                                    continue;
                                }
                            }

                            matched = true;

                            // Mark left row as matched
                            if let Some(matched_list) =
                                self.left_matched.get_mut(&right_key)
                            {
                                if idx < matched_list.len() {
                                    matched_list[idx] = true;
                                }
                            }

                            // Mark for SEMI
                            if let Some(semi_list) =
                                self.semi_matched.get_mut(&right_key)
                            {
                                if idx < semi_list.len() {
                                    semi_list[idx] = true;
                                }
                            }

                            match self.join_type {
                                JoinType::Inner
                                | JoinType::Left
                                | JoinType::Right
                                | JoinType::Full
                                | JoinType::Cross => {
                                    self.output_buffer.push(candidate);
                                }
                                JoinType::LeftSemi | JoinType::LeftAnti => {
                                    // Don't emit combined rows for SEMI/ANTI
                                }
                            }
                        }

                        if !matched
                            && matches!(
                                self.join_type,
                                JoinType::Right | JoinType::Full
                            )
                        {
                            self.unmatched_right.push(right_vals);
                        }
                    } else {
                        // No match found in hash table
                        if matches!(
                            self.join_type,
                            JoinType::Right | JoinType::Full
                        ) {
                            self.unmatched_right.push(right_vals);
                        }
                    }

                    if self.buffer_pos < self.output_buffer.len() {
                        let t = self.output_buffer[self.buffer_pos].clone();
                        self.buffer_pos += 1;
                        return Ok(Some(t));
                    }
                }
            }
        }
    }

    fn emit_unmatched(&mut self) -> Result<Option<Tuple>> {
        // Drain output buffer first
        if self.buffer_pos < self.output_buffer.len() {
            let t = self.output_buffer[self.buffer_pos].clone();
            self.buffer_pos += 1;
            return Ok(Some(t));
        }

        if self.phase == JoinPhase::EmitUnmatched {
            self.output_buffer.clear();
            self.buffer_pos = 0;

            // SEMI join: emit left rows that matched
            if self.join_type == JoinType::LeftSemi {
                // Emit matched in-memory left rows (left columns only)
                for (key, matched_flags) in &self.semi_matched {
                    if let Some(left_rows) = self.hash_table.get(key) {
                        for (i, &was_matched) in
                            matched_flags.iter().enumerate()
                        {
                            if was_matched {
                                self.output_buffer.push(intermediate_tuple(
                                    left_rows[i].clone(),
                                ));
                            }
                        }
                    }
                }
                // Emit matched spill rows
                for (si, &was_matched) in
                    self.semi_spill_matched.iter().enumerate()
                {
                    if was_matched {
                        self.output_buffer.push(intermediate_tuple(
                            self.spill_buffer[si].1.clone(),
                        ));
                    }
                }
            }

            // ANTI join: emit left rows that did NOT match
            if self.join_type == JoinType::LeftAnti {
                // Emit unmatched in-memory left rows (left columns only)
                for (key, left_rows) in &self.hash_table {
                    let matched_flags = self
                        .semi_matched
                        .get(key)
                        .map(|v| v.as_slice())
                        .unwrap_or(&[]);
                    for (i, left_vals) in left_rows.iter().enumerate() {
                        let was_matched =
                            matched_flags.get(i).copied().unwrap_or(false);
                        if !was_matched {
                            self.output_buffer
                                .push(intermediate_tuple(left_vals.clone()));
                        }
                    }
                }
                // Emit unmatched spill rows
                for (si, &was_matched) in
                    self.semi_spill_matched.iter().enumerate()
                {
                    if !was_matched {
                        self.output_buffer.push(intermediate_tuple(
                            self.spill_buffer[si].1.clone(),
                        ));
                    }
                }
            }

            // Emit unmatched left rows (for LEFT/FULL)
            if matches!(self.join_type, JoinType::Left | JoinType::Full) {
                for (key, matched_flags) in &self.left_matched {
                    if let Some(left_rows) = self.hash_table.get(key) {
                        for (i, &was_matched) in
                            matched_flags.iter().enumerate()
                        {
                            if !was_matched {
                                let mut vals = left_rows[i].clone();
                                vals.resize(
                                    self.left_col_count
                                        + self.right_col_count,
                                    Datum::Null,
                                );
                                self.output_buffer
                                    .push(intermediate_tuple(vals));
                            }
                        }
                    }
                }
                // Emit unmatched spill rows for LEFT/FULL
                for (si, &was_matched) in
                    self.spill_matched.iter().enumerate()
                {
                    if !was_matched {
                        let mut vals = self.spill_buffer[si].1.clone();
                        vals.resize(
                            self.left_col_count + self.right_col_count,
                            Datum::Null,
                        );
                        self.output_buffer.push(intermediate_tuple(vals));
                    }
                }
            }

            // Emit unmatched right rows (for RIGHT/FULL)
            if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                for right_vals in &self.unmatched_right {
                    let mut vals = vec![Datum::Null; self.left_col_count];
                    vals.extend_from_slice(right_vals);
                    self.output_buffer.push(intermediate_tuple(vals));
                }
            }

            self.phase = JoinPhase::Done;

            if !self.output_buffer.is_empty() {
                let t = self.output_buffer[self.buffer_pos].clone();
                self.buffer_pos += 1;
                return Ok(Some(t));
            }
        }

        Ok(None)
    }
}

impl Executor for HashJoinExecutor {
    fn init(&mut self) -> Result<()> {
        self.left.init()?;
        self.right.init()?;

        let left_schema = self.left.schema().clone();
        let right_schema = self.right.schema().clone();
        self.left_col_count = left_schema.column_count();
        self.right_col_count = right_schema.column_count();

        // Compile key expressions
        self.left_key_ops = self
            .left_keys_src
            .iter()
            .map(|e| compile_expr(e, &left_schema))
            .collect::<Result<Vec<_>>>()?;
        self.right_key_ops = self
            .right_keys_src
            .iter()
            .map(|e| compile_expr(e, &right_schema))
            .collect::<Result<Vec<_>>>()?;

        // Compile residual condition against merged schema
        if let Some(ref cond) = self.condition_src {
            self.condition_ops = Some(compile_expr(cond, &self.schema)?);
        }

        // Reset state
        self.hash_table.clear();
        self.left_matched.clear();
        self.semi_matched.clear();
        self.unmatched_right.clear();
        self.spill_buffer.clear();
        self.output_buffer.clear();
        self.buffer_pos = 0;
        self.probe_exhausted = false;

        self.build_phase()?;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        // Always drain buffer first, regardless of phase
        if self.buffer_pos < self.output_buffer.len() {
            let t = self.output_buffer[self.buffer_pos].clone();
            self.buffer_pos += 1;
            return Ok(Some(t));
        }

        match self.phase {
            JoinPhase::Probe => {
                if let Some(t) = self.probe_next()? {
                    return Ok(Some(t));
                }
                // Probe exhausted, try spill or unmatched
                match self.phase {
                    JoinPhase::SpillProbe => {
                        // Spill probe already done inline during probe
                        self.phase = JoinPhase::EmitUnmatched;
                        self.emit_unmatched()
                    }
                    _ => self.emit_unmatched(),
                }
            }
            JoinPhase::SpillProbe => {
                self.phase = JoinPhase::EmitUnmatched;
                self.emit_unmatched()
            }
            JoinPhase::EmitUnmatched => self.emit_unmatched(),
            JoinPhase::Done => Ok(None),
            _ => Ok(None),
        }
    }

    fn close(&mut self) -> Result<()> {
        self.hash_table.clear();
        self.spill_buffer.clear();
        self.left.close()?;
        self.right.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
