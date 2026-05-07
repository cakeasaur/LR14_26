// demographics_validator — Rust-крейт для валидации демографических записей.
// Экспортируется в Python через PyO3 как два метода:
//     validate_record(dict)        -> dict {valid, errors}
//     validate_batch(list[dict])   -> list[dict]
//
// Правила валидации соответствуют ТЗ лабораторной работы №14, вариант 26.

use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods};

const ALLOWED_INDICATORS: &[&str] = &[
    "population",
    "birth_rate",
    "death_rate",
    "natural_growth",
    "migration_growth",
    "life_expectancy",
];

const MIN_YEAR: i64 = 1990;
const MAX_YEAR: i64 = 2030;

const MIN_REGION_LEN: usize = 2;
const MAX_REGION_LEN: usize = 100;

// ─────────────────────────────────────────────────────────────────────────
// Внутренние типы
// ─────────────────────────────────────────────────────────────────────────

struct ValidationOutcome {
    valid: bool,
    errors: Vec<String>,
}

impl ValidationOutcome {
    fn ok() -> Self {
        Self { valid: true, errors: Vec::new() }
    }
    fn add_error(&mut self, msg: impl Into<String>) {
        self.valid = false;
        self.errors.push(msg.into());
    }
    fn into_py_dict<'py>(self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new_bound(py);
        dict.set_item("valid", self.valid)?;
        dict.set_item("errors", self.errors)?;
        Ok(dict)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Логика валидации (читает поля через Bound<PyDict>)
// ─────────────────────────────────────────────────────────────────────────

fn validate_one(record: &Bound<'_, PyDict>) -> ValidationOutcome {
    let mut out = ValidationOutcome::ok();

    // ── region ──
    let region: String = match record.get_item("region") {
        Ok(Some(v)) => v.extract().unwrap_or_else(|_| {
            out.add_error("region: must be string");
            String::new()
        }),
        _ => {
            out.add_error("region: missing");
            String::new()
        }
    };
    if !region.is_empty() {
        let len = region.chars().count();
        if len < MIN_REGION_LEN || len > MAX_REGION_LEN {
            out.add_error(format!(
                "region: length {} not in [{}, {}]",
                len, MIN_REGION_LEN, MAX_REGION_LEN
            ));
        }
        if region.trim().is_empty() {
            out.add_error("region: must be non-empty");
        }
    }

    // ── year ──
    let year: i64 = match record.get_item("year") {
        Ok(Some(v)) => v.extract::<i64>().unwrap_or_else(|_| {
            out.add_error("year: must be integer");
            0
        }),
        _ => {
            out.add_error("year: missing");
            0
        }
    };
    if year != 0 && (year < MIN_YEAR || year > MAX_YEAR) {
        out.add_error(format!("year: {} not in [{}, {}]", year, MIN_YEAR, MAX_YEAR));
    }

    // ── indicator ──
    let indicator: String = match record.get_item("indicator") {
        Ok(Some(v)) => v.extract().unwrap_or_default(),
        _ => {
            out.add_error("indicator: missing");
            String::new()
        }
    };
    if !indicator.is_empty() && !ALLOWED_INDICATORS.contains(&indicator.as_str()) {
        out.add_error(format!(
            "indicator: '{}' not in {:?}",
            indicator, ALLOWED_INDICATORS
        ));
    }

    // ── value ──
    let value: f64 = match record.get_item("value") {
        Ok(Some(v)) => v.extract::<f64>().unwrap_or_else(|_| {
            out.add_error("value: must be number");
            f64::NAN
        }),
        _ => {
            out.add_error("value: missing");
            f64::NAN
        }
    };

    if !value.is_nan() && !indicator.is_empty() {
        match indicator.as_str() {
            "birth_rate" | "death_rate" => {
                if !(0.0..=100.0).contains(&value) {
                    out.add_error(format!(
                        "value: {} not in [0.0, 100.0] for {}",
                        value, indicator
                    ));
                }
            }
            "population" => {
                if !(1_000.0..=20_000_000.0).contains(&value) {
                    out.add_error(format!(
                        "value: {} not in [1_000, 20_000_000] for population",
                        value
                    ));
                }
            }
            "life_expectancy" => {
                if !(30.0..=100.0).contains(&value) {
                    out.add_error(format!(
                        "value: {} not in [30.0, 100.0] for life_expectancy",
                        value
                    ));
                }
            }
            "natural_growth" | "migration_growth" => {
                if !(-100.0..=100.0).contains(&value) {
                    out.add_error(format!(
                        "value: {} not in [-100.0, 100.0] for {}",
                        value, indicator
                    ));
                }
            }
            _ => {}
        }
    }

    out
}

// ─────────────────────────────────────────────────────────────────────────
// Python-API (Bound API из PyO3 0.22)
// ─────────────────────────────────────────────────────────────────────────

#[pyfunction]
fn validate_record<'py>(py: Python<'py>, record: &Bound<'py, PyDict>) -> PyResult<Bound<'py, PyDict>> {
    validate_one(record).into_py_dict(py)
}

#[pyfunction]
fn validate_batch<'py>(py: Python<'py>, records: &Bound<'py, PyList>) -> PyResult<Bound<'py, PyList>> {
    let results = PyList::empty_bound(py);
    for item in records.iter() {
        let dict = item.downcast::<PyDict>()?;
        let outcome = validate_one(dict).into_py_dict(py)?;
        results.append(outcome)?;
    }
    Ok(results)
}

/// Возвращает версию крейта (полезно для отладки).
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn demographics_validator(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate_record, m)?)?;
    m.add_function(wrap_pyfunction!(validate_batch, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    Ok(())
}
