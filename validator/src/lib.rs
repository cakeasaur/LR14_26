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

    // ── region ── (Option<String>: None если не извлеклось — отдельный сигнал от "")
    let region: Option<String> = match record.get_item("region") {
        Ok(Some(v)) => match v.extract::<String>() {
            Ok(s) => Some(s),
            Err(_) => {
                out.add_error("region: must be string");
                None
            }
        },
        _ => {
            out.add_error("region: missing");
            None
        }
    };
    if let Some(region) = region.as_ref() {
        if region.trim().is_empty() {
            out.add_error("region: must be non-empty");
        } else {
            let len = region.chars().count();
            if len < MIN_REGION_LEN || len > MAX_REGION_LEN {
                out.add_error(format!(
                    "region: length {} not in [{}, {}]",
                    len, MIN_REGION_LEN, MAX_REGION_LEN
                ));
            }
        }
    }

    // ── year ── (Option<i64>: None отличается от валидного 0)
    let year: Option<i64> = match record.get_item("year") {
        Ok(Some(v)) => match v.extract::<i64>() {
            Ok(y) => Some(y),
            Err(_) => {
                out.add_error("year: must be integer");
                None
            }
        },
        _ => {
            out.add_error("year: missing");
            None
        }
    };
    if let Some(y) = year {
        if y < MIN_YEAR || y > MAX_YEAR {
            out.add_error(format!("year: {} not in [{}, {}]", y, MIN_YEAR, MAX_YEAR));
        }
    }

    // ── indicator ── (Option<String>: явная ошибка типа, пустая строка тоже ошибка)
    let indicator: Option<String> = match record.get_item("indicator") {
        Ok(Some(v)) => match v.extract::<String>() {
            Ok(s) => Some(s),
            Err(_) => {
                out.add_error("indicator: must be string");
                None
            }
        },
        _ => {
            out.add_error("indicator: missing");
            None
        }
    };
    if let Some(ind) = indicator.as_ref() {
        if ind.is_empty() {
            out.add_error("indicator: must be non-empty");
        } else if !ALLOWED_INDICATORS.contains(&ind.as_str()) {
            out.add_error(format!(
                "indicator: '{}' not in {:?}",
                ind, ALLOWED_INDICATORS
            ));
        }
    }

    // ── value ──
    let value: Option<f64> = match record.get_item("value") {
        Ok(Some(v)) => match v.extract::<f64>() {
            Ok(x) if x.is_finite() => Some(x),
            Ok(_) => {
                out.add_error("value: must be finite number (not NaN/Inf)");
                None
            }
            Err(_) => {
                out.add_error("value: must be number");
                None
            }
        },
        _ => {
            out.add_error("value: missing");
            None
        }
    };

    if let (Some(value), Some(indicator)) = (value, indicator.as_ref()) {
        if !indicator.is_empty() {
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

// ─────────────────────────────────────────────────────────────────────────
// Тесты (cargo test --no-default-features --features=test-mode)
// ─────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::PyList;

    /// Хелпер: создаёт PyDict с указанными полями.
    fn make_record<'py>(
        py: Python<'py>,
        region: Option<&str>,
        year: Option<i64>,
        indicator: Option<&str>,
        value: Option<f64>,
    ) -> Bound<'py, PyDict> {
        let dict = PyDict::new_bound(py);
        if let Some(r) = region { dict.set_item("region", r).unwrap(); }
        if let Some(y) = year { dict.set_item("year", y).unwrap(); }
        if let Some(i) = indicator { dict.set_item("indicator", i).unwrap(); }
        if let Some(v) = value { dict.set_item("value", v).unwrap(); }
        dict
    }

    fn valid_record<'py>(py: Python<'py>) -> Bound<'py, PyDict> {
        make_record(py, Some("Москва"), Some(2020), Some("birth_rate"), Some(10.5))
    }

    // ─── happy path ─────────────────────────────────────────────────────

    #[test]
    fn test_valid_record_passes() {
        Python::with_gil(|py| {
            let rec = valid_record(py);
            let outcome = validate_one(&rec);
            assert!(outcome.valid, "errors: {:?}", outcome.errors);
            assert!(outcome.errors.is_empty());
        });
    }

    #[test]
    fn test_valid_record_all_indicators() {
        Python::with_gil(|py| {
            let cases = [
                ("birth_rate", 10.5),
                ("death_rate", 13.0),
                ("natural_growth", -2.5),
                ("migration_growth", 1.5),
                ("life_expectancy", 75.0),
                ("population", 1_500_000.0),
            ];
            for (ind, val) in cases {
                let rec = make_record(py, Some("Москва"), Some(2020), Some(ind), Some(val));
                let outcome = validate_one(&rec);
                assert!(
                    outcome.valid,
                    "indicator={} value={} errors={:?}",
                    ind, val, outcome.errors
                );
            }
        });
    }

    // ─── region ─────────────────────────────────────────────────────────

    #[test]
    fn test_region_missing() {
        Python::with_gil(|py| {
            let rec = make_record(py, None, Some(2020), Some("birth_rate"), Some(10.0));
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("region: missing")));
        });
    }

    #[test]
    fn test_region_empty_string() {
        Python::with_gil(|py| {
            let rec = make_record(py, Some(""), Some(2020), Some("birth_rate"), Some(10.0));
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("region: must be non-empty")));
        });
    }

    #[test]
    fn test_region_whitespace_only() {
        Python::with_gil(|py| {
            let rec = make_record(py, Some("   "), Some(2020), Some("birth_rate"), Some(10.0));
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("region: must be non-empty")));
        });
    }

    #[test]
    fn test_region_too_short() {
        Python::with_gil(|py| {
            let rec = make_record(py, Some("X"), Some(2020), Some("birth_rate"), Some(10.0));
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("region: length")));
        });
    }

    #[test]
    fn test_region_too_long() {
        Python::with_gil(|py| {
            let long = "А".repeat(101);
            let rec = make_record(py, Some(&long), Some(2020), Some("birth_rate"), Some(10.0));
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("region: length")));
        });
    }

    #[test]
    fn test_region_boundary_values() {
        Python::with_gil(|py| {
            // ровно 2 символа — OK
            let rec = make_record(py, Some("АБ"), Some(2020), Some("birth_rate"), Some(10.0));
            assert!(validate_one(&rec).valid);
            // ровно 100 символов — OK
            let exactly_100 = "А".repeat(100);
            let rec = make_record(py, Some(&exactly_100), Some(2020), Some("birth_rate"), Some(10.0));
            assert!(validate_one(&rec).valid);
        });
    }

    #[test]
    fn test_region_wrong_type() {
        Python::with_gil(|py| {
            let dict = PyDict::new_bound(py);
            dict.set_item("region", 42).unwrap();  // число вместо строки
            dict.set_item("year", 2020).unwrap();
            dict.set_item("indicator", "birth_rate").unwrap();
            dict.set_item("value", 10.0).unwrap();
            let o = validate_one(&dict);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("region: must be string")));
        });
    }

    // ─── year ───────────────────────────────────────────────────────────

    #[test]
    fn test_year_missing() {
        Python::with_gil(|py| {
            let rec = make_record(py, Some("Москва"), None, Some("birth_rate"), Some(10.0));
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("year: missing")));
        });
    }

    #[test]
    fn test_year_below_min() {
        Python::with_gil(|py| {
            let rec = make_record(py, Some("Москва"), Some(1989), Some("birth_rate"), Some(10.0));
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("year: 1989")));
        });
    }

    #[test]
    fn test_year_above_max() {
        Python::with_gil(|py| {
            let rec = make_record(py, Some("Москва"), Some(2031), Some("birth_rate"), Some(10.0));
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("year: 2031")));
        });
    }

    #[test]
    fn test_year_zero_is_invalid() {
        // Регрессия после аудита: ранее year=0 был sentinel'ом и проходил
        // невалидацию. Теперь должен явно фейлиться (0 < MIN_YEAR=1990).
        Python::with_gil(|py| {
            let rec = make_record(py, Some("Москва"), Some(0), Some("birth_rate"), Some(10.0));
            let o = validate_one(&rec);
            assert!(!o.valid, "year=0 должен быть отклонён, не sentinel");
        });
    }

    #[test]
    fn test_year_boundaries() {
        Python::with_gil(|py| {
            // 1990 — OK
            let rec = make_record(py, Some("Москва"), Some(1990), Some("birth_rate"), Some(10.0));
            assert!(validate_one(&rec).valid);
            // 2030 — OK
            let rec = make_record(py, Some("Москва"), Some(2030), Some("birth_rate"), Some(10.0));
            assert!(validate_one(&rec).valid);
        });
    }

    #[test]
    fn test_year_wrong_type() {
        Python::with_gil(|py| {
            let dict = PyDict::new_bound(py);
            dict.set_item("region", "Москва").unwrap();
            dict.set_item("year", "не число").unwrap();
            dict.set_item("indicator", "birth_rate").unwrap();
            dict.set_item("value", 10.0).unwrap();
            let o = validate_one(&dict);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("year: must be integer")));
        });
    }

    // ─── indicator ──────────────────────────────────────────────────────

    #[test]
    fn test_indicator_missing() {
        Python::with_gil(|py| {
            let rec = make_record(py, Some("Москва"), Some(2020), None, Some(10.0));
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("indicator: missing")));
        });
    }

    #[test]
    fn test_indicator_empty_string() {
        // Регрессия после аудита: ранее "" проходил.
        Python::with_gil(|py| {
            let rec = make_record(py, Some("Москва"), Some(2020), Some(""), Some(10.0));
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("indicator: must be non-empty")));
        });
    }

    #[test]
    fn test_indicator_not_in_list() {
        Python::with_gil(|py| {
            let rec = make_record(py, Some("Москва"), Some(2020), Some("unemployment"), Some(10.0));
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("indicator: 'unemployment'")));
        });
    }

    #[test]
    fn test_indicator_wrong_type() {
        // Регрессия после аудита: ранее indicator=42 тихо превращался в "".
        Python::with_gil(|py| {
            let dict = PyDict::new_bound(py);
            dict.set_item("region", "Москва").unwrap();
            dict.set_item("year", 2020).unwrap();
            dict.set_item("indicator", 42).unwrap();
            dict.set_item("value", 10.0).unwrap();
            let o = validate_one(&dict);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("indicator: must be string")));
        });
    }

    // ─── value ──────────────────────────────────────────────────────────

    #[test]
    fn test_value_missing() {
        Python::with_gil(|py| {
            let rec = make_record(py, Some("Москва"), Some(2020), Some("birth_rate"), None);
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("value: missing")));
        });
    }

    #[test]
    fn test_value_nan_rejected() {
        Python::with_gil(|py| {
            let rec = make_record(py, Some("Москва"), Some(2020), Some("birth_rate"), Some(f64::NAN));
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("must be finite")));
        });
    }

    #[test]
    fn test_value_inf_rejected() {
        Python::with_gil(|py| {
            let rec = make_record(py, Some("Москва"), Some(2020), Some("birth_rate"), Some(f64::INFINITY));
            let o = validate_one(&rec);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("must be finite")));
        });
    }

    #[test]
    fn test_value_birth_rate_out_of_range() {
        Python::with_gil(|py| {
            // Отрицательный
            let rec = make_record(py, Some("Москва"), Some(2020), Some("birth_rate"), Some(-1.0));
            assert!(!validate_one(&rec).valid);
            // Слишком большой
            let rec = make_record(py, Some("Москва"), Some(2020), Some("birth_rate"), Some(101.0));
            assert!(!validate_one(&rec).valid);
        });
    }

    #[test]
    fn test_value_population_out_of_range() {
        Python::with_gil(|py| {
            // Слишком маленькое
            let rec = make_record(py, Some("Москва"), Some(2020), Some("population"), Some(999.0));
            assert!(!validate_one(&rec).valid);
            // Слишком большое
            let rec = make_record(py, Some("Москва"), Some(2020), Some("population"), Some(20_000_001.0));
            assert!(!validate_one(&rec).valid);
        });
    }

    #[test]
    fn test_value_life_expectancy_out_of_range() {
        Python::with_gil(|py| {
            let rec = make_record(py, Some("Москва"), Some(2020), Some("life_expectancy"), Some(29.0));
            assert!(!validate_one(&rec).valid);
            let rec = make_record(py, Some("Москва"), Some(2020), Some("life_expectancy"), Some(101.0));
            assert!(!validate_one(&rec).valid);
        });
    }

    #[test]
    fn test_value_natural_growth_out_of_range() {
        Python::with_gil(|py| {
            let rec = make_record(py, Some("Москва"), Some(2020), Some("natural_growth"), Some(-101.0));
            assert!(!validate_one(&rec).valid);
            let rec = make_record(py, Some("Москва"), Some(2020), Some("natural_growth"), Some(101.0));
            assert!(!validate_one(&rec).valid);
        });
    }

    #[test]
    fn test_value_wrong_type() {
        Python::with_gil(|py| {
            let dict = PyDict::new_bound(py);
            dict.set_item("region", "Москва").unwrap();
            dict.set_item("year", 2020).unwrap();
            dict.set_item("indicator", "birth_rate").unwrap();
            dict.set_item("value", "не число").unwrap();
            let o = validate_one(&dict);
            assert!(!o.valid);
            assert!(o.errors.iter().any(|e| e.contains("value: must be number")));
        });
    }

    // ─── множественные ошибки ───────────────────────────────────────────

    #[test]
    fn test_multiple_errors_accumulated() {
        Python::with_gil(|py| {
            let dict = PyDict::new_bound(py);
            // всё неверно: пустая строка, год < min, неизвестный индикатор, NaN
            dict.set_item("region", "").unwrap();
            dict.set_item("year", 1500).unwrap();
            dict.set_item("indicator", "unknown").unwrap();
            dict.set_item("value", f64::NAN).unwrap();
            let o = validate_one(&dict);
            assert!(!o.valid);
            assert!(o.errors.len() >= 3, "ожидалось >= 3 ошибок, получено {}", o.errors.len());
        });
    }

    // ─── validate_batch ─────────────────────────────────────────────────

    #[test]
    fn test_batch_returns_same_length() {
        Python::with_gil(|py| {
            let list = PyList::empty_bound(py);
            for _ in 0..5 {
                list.append(valid_record(py)).unwrap();
            }
            let result = validate_batch(py, &list).unwrap();
            assert_eq!(result.len(), 5);
        });
    }

    #[test]
    fn test_batch_all_valid() {
        Python::with_gil(|py| {
            let list = PyList::empty_bound(py);
            for _ in 0..3 {
                list.append(valid_record(py)).unwrap();
            }
            let result = validate_batch(py, &list).unwrap();
            for item in result.iter() {
                let d = item.downcast::<PyDict>().unwrap();
                let valid: bool = d.get_item("valid").unwrap().unwrap().extract().unwrap();
                assert!(valid);
            }
        });
    }

    #[test]
    fn test_batch_mixed_valid_invalid() {
        Python::with_gil(|py| {
            let list = PyList::empty_bound(py);
            list.append(valid_record(py)).unwrap();
            list.append(make_record(py, Some(""), Some(2020), Some("birth_rate"), Some(10.0))).unwrap();
            list.append(valid_record(py)).unwrap();

            let result = validate_batch(py, &list).unwrap();
            let valids: Vec<bool> = result.iter().map(|item| {
                let d = item.downcast::<PyDict>().unwrap();
                d.get_item("valid").unwrap().unwrap().extract::<bool>().unwrap()
            }).collect();
            assert_eq!(valids, vec![true, false, true]);
        });
    }

    #[test]
    fn test_batch_empty() {
        Python::with_gil(|py| {
            let list = PyList::empty_bound(py);
            let result = validate_batch(py, &list).unwrap();
            assert_eq!(result.len(), 0);
        });
    }

    // ─── version ────────────────────────────────────────────────────────

    #[test]
    fn test_version_matches_cargo() {
        assert_eq!(version(), env!("CARGO_PKG_VERSION"));
    }
}
