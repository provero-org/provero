# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Anomaly detection checks: anomaly, row_count_change."""

from __future__ import annotations

from provero.anomaly.detectors import detect_anomaly
from provero.checks.registry import register_check
from provero.connectors.base import Connection
from provero.core.compiler import CheckConfig
from provero.core.results import CheckResult, Severity, Status
from provero.core.sql import quote_identifier


def _query_metric(connection: Connection, table: str, metric: str, column: str | None) -> float | None:
    """Query the current value of a metric from the data source."""
    qtable = quote_identifier(table)

    if metric == "row_count":
        rows = connection.execute(f"SELECT COUNT(*) as v FROM {qtable}")
        return float(rows[0]["v"])

    if metric == "null_count" and column:
        qcol = quote_identifier(column)
        rows = connection.execute(
            f"SELECT COUNT(*) FILTER (WHERE {qcol} IS NULL) as v FROM {qtable}"
        )
        return float(rows[0]["v"])

    if metric == "null_rate" and column:
        qcol = quote_identifier(column)
        rows = connection.execute(
            f"SELECT COUNT(*) as total, "
            f"COUNT(*) FILTER (WHERE {qcol} IS NULL) as nulls "
            f"FROM {qtable}"
        )
        total = rows[0]["total"]
        if total == 0:
            return 0.0
        return float(rows[0]["nulls"]) / float(total)

    if metric == "distinct_count" and column:
        qcol = quote_identifier(column)
        rows = connection.execute(f"SELECT COUNT(DISTINCT {qcol}) as v FROM {qtable}")
        return float(rows[0]["v"])

    if metric == "mean" and column:
        qcol = quote_identifier(column)
        rows = connection.execute(f"SELECT AVG({qcol}) as v FROM {qtable}")
        val = rows[0]["v"]
        return float(val) if val is not None else None

    if metric == "min" and column:
        qcol = quote_identifier(column)
        rows = connection.execute(f"SELECT MIN({qcol}) as v FROM {qtable}")
        val = rows[0]["v"]
        return float(val) if val is not None else None

    if metric == "max" and column:
        qcol = quote_identifier(column)
        rows = connection.execute(f"SELECT MAX({qcol}) as v FROM {qtable}")
        val = rows[0]["v"]
        return float(val) if val is not None else None

    return None


def _get_history(params: dict, check_name_override: str = "") -> list[float]:
    """Get historical metric values from injected store or params."""
    # If engine injected history directly, use it
    if "_history" in params:
        return list(params["_history"])

    # Fallback: read from store if suite context available
    suite_name = params.get("_suite_name", "")
    check_name = check_name_override or params.get("_check_name", "")
    metric = params.get("metric", "")
    store_path = params.get("_store_path", "")

    if not suite_name or not metric:
        return []

    from provero.store.sqlite import SQLiteStore

    store_kwargs = {"db_path": store_path} if store_path else {}
    store = SQLiteStore(**store_kwargs)
    try:
        history = store.get_metrics(suite_name, check_name, metric)
    finally:
        store.close()

    return [row["value"] for row in reversed(history)]


@register_check("anomaly")
def check_anomaly(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Generic anomaly detection on any historical metric.

    Queries the current metric value from the data source, then
    compares it against historical values from the result store.

    Params:
        metric (str): Metric to check: row_count, null_count, null_rate,
                       distinct_count, mean, min, max.
        method (str): Detection method (default "mad").
        sensitivity (str): Sensitivity level (default "medium").
        threshold (float): Direct threshold override (optional).
        column (str): Column for column-level metrics.
        _suite_name (str): Injected by engine.
        _check_name (str): Injected by engine.
        _history (list[float]): Injected historical values (optional).
        _store_path (str): Store path for history lookup (optional).
    """
    metric = check_config.params.get("metric", "")
    method = check_config.params.get("method", "mad")
    sensitivity = check_config.params.get("sensitivity", "medium")
    threshold_override = check_config.params.get("threshold")
    col = check_config.column or check_config.params.get("column")

    severity = (
        Severity(check_config.severity)
        if check_config.severity
        else Severity.WARNING
    )

    check_label = f"anomaly:{col or metric}"

    if not metric:
        return CheckResult(
            check_name=check_label,
            check_type="anomaly",
            status=Status.ERROR,
            severity=severity,
            column=col,
            observed_value="'metric' parameter is required",
        )

    # 1. Query current value from data source
    try:
        current = _query_metric(connection, table, metric, col)
    except Exception as e:
        return CheckResult(
            check_name=check_label,
            check_type="anomaly",
            status=Status.ERROR,
            severity=severity,
            column=col,
            observed_value=f"Failed to query metric '{metric}': {e}",
        )

    if current is None:
        return CheckResult(
            check_name=check_label,
            check_type="anomaly",
            status=Status.ERROR,
            severity=severity,
            column=col,
            observed_value=f"Unsupported metric '{metric}' or no data",
        )

    # 2. Get historical values (use check_label to match what store saves)
    historical = _get_history(check_config.params, check_name_override=check_label)

    if not historical:
        return CheckResult(
            check_name=check_label,
            check_type="anomaly",
            status=Status.SKIP,
            severity=severity,
            column=col,
            observed_value=f"{current} (no history available)",
            expected_value=f"method={method}, sensitivity={sensitivity}",
        )

    # 3. Run anomaly detection
    if threshold_override is not None:
        from provero.anomaly.models import SENSITIVITY_THRESHOLDS
        # Use direct threshold, bypass sensitivity mapping
        detector = {"zscore": "zscore", "mad": "mad", "iqr": "iqr"}.get(method, "mad")
        from provero.anomaly.detectors import _DETECTORS
        detect_fn = _DETECTORS.get(detector)
        if detect_fn:
            result = detect_fn(historical, current, float(threshold_override))
            result.sensitivity = sensitivity
        else:
            result = detect_anomaly(historical, current, method=method, sensitivity=sensitivity)
    else:
        result = detect_anomaly(historical, current, method=method, sensitivity=sensitivity)

    lower, upper = result.expected_range
    status = Status.FAIL if result.is_anomaly else Status.PASS

    return CheckResult(
        check_name=check_label,
        check_type="anomaly",
        status=status,
        severity=severity,
        column=col,
        observed_value=f"{current} (score={result.anomaly_score})",
        expected_value=f"[{lower:.2f}, {upper:.2f}]",
    )


@register_check("row_count_change")
def check_row_count_change(
    connection: Connection,
    table: str,
    check_config: CheckConfig,
) -> CheckResult:
    """Check that row count hasn't changed beyond acceptable thresholds.

    Queries current COUNT(*) and compares against the last stored
    row_count_change metric (its own history, not dependent on a
    separate row_count check).

    Params:
        max_decrease (str): Maximum allowed decrease, e.g. "20%".
        max_increase (str): Maximum allowed increase, e.g. "300%".
        _suite_name (str): Injected by engine.
        _store_path (str): Store path for history lookup (optional).
    """
    max_decrease_str = check_config.params.get("max_decrease", "50%")
    max_increase_str = check_config.params.get("max_increase", "500%")
    suite_name = check_config.params.get("_suite_name", "")
    store_path = check_config.params.get("_store_path", "")

    severity = (
        Severity(check_config.severity)
        if check_config.severity
        else Severity.WARNING
    )

    max_decrease = float(max_decrease_str.rstrip("%")) / 100.0
    max_increase = float(max_increase_str.rstrip("%")) / 100.0

    # Query current row count from data source
    qtable = quote_identifier(table)
    rows = connection.execute(f"SELECT COUNT(*) as total FROM {qtable}")
    current_count = rows[0]["total"]

    if not suite_name:
        return CheckResult(
            check_name="row_count_change",
            check_type="row_count_change",
            status=Status.SKIP,
            severity=severity,
            observed_value=f"{current_count:,} rows (no history available)",
            row_count=current_count,
        )

    # Look up own previous row_count_change metric (not another check's)
    from provero.store.sqlite import SQLiteStore

    store_kwargs = {"db_path": store_path} if store_path else {}
    store = SQLiteStore(**store_kwargs)
    try:
        history = store.get_metrics(suite_name, "row_count_change", "row_count", limit=1)
    finally:
        store.close()

    if not history:
        return CheckResult(
            check_name="row_count_change",
            check_type="row_count_change",
            status=Status.PASS,
            severity=severity,
            observed_value=f"{current_count:,} rows (first run)",
            expected_value="no previous data",
            row_count=current_count,
        )

    previous_count = history[0]["value"]
    if previous_count == 0:
        return CheckResult(
            check_name="row_count_change",
            check_type="row_count_change",
            status=Status.PASS,
            severity=severity,
            observed_value=f"{current_count:,} rows",
            expected_value="previous was 0",
            row_count=current_count,
        )

    change_pct = (current_count - previous_count) / previous_count
    decrease = max(0, -change_pct)
    increase = max(0, change_pct)

    failed = False
    explanation = f"{change_pct:+.1%} change"

    if decrease > max_decrease:
        failed = True
        explanation = f"Decreased {decrease:.1%} (max allowed: {max_decrease:.0%})"
    elif increase > max_increase:
        failed = True
        explanation = f"Increased {increase:.1%} (max allowed: {max_increase:.0%})"

    return CheckResult(
        check_name="row_count_change",
        check_type="row_count_change",
        status=Status.FAIL if failed else Status.PASS,
        severity=severity,
        observed_value=f"{current_count:,} rows ({explanation})",
        expected_value=f"change within -{max_decrease:.0%} to +{max_increase:.0%}",
        row_count=current_count,
    )
