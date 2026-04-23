# Calculation Sheet (Finance + Timesheet)

This sheet explains how each metric is calculated in the system.

## 1) Record-Level Calculations

Source file: `ingestion/timesheet_parser.py`

- `actual_hours`: Sum of valid numeric daily cells.
- `working_days`: Count of days where a numeric hour value is present.
- `leave_days`: Count of day cells marked as leave (case-insensitive), including markers like `OFF`, `L`, `Leave`, and blanks when treated as leave in parser rules.
- `holiday_days`: Count of day cells marked as holidays (case-insensitive), such as `PH`, `Holiday`, `Public Holiday`.

Financial record fields:

- `revenue = billable_hours * billing_rate`
- `cost = actual_hours * cost_rate`
- `profit = revenue - cost`
- `margin_pct = (profit / revenue) * 100` (0 when revenue is 0)

---

## 2) Overall Summary

Source file: `ingestion/dataset.py` → `build_overall_summary(...)`

- `total_revenue = sum(revenue)`
- `total_cost = sum(cost)`
- `total_profit = total_revenue - total_cost`
- `avg_margin_pct = ((total_revenue - total_cost) / total_revenue) * 100` (0 when revenue <= 0)
- `total_employees = count(distinct employee)`
- `total_hours = sum(actual_hours)`

---

## 3) Monthly Summary

Source file: `ingestion/dataset.py` → `build_monthly(...)`

For each month:

- `total_revenue = sum(revenue)`
- `total_cost = sum(cost)`
- `total_profit = sum(profit)`
- `avg_margin_pct = (total_profit / total_revenue) * 100` (0 when revenue <= 0)
- `employees = count(distinct employee)`

---

## 4) Project Summary (`/projects`)

Source file: `ingestion/dataset.py` → `build_project_summaries(...)`

For each project:

- `total_revenue = sum(revenue)`
- `total_cost = sum(cost)`
- `total_profit = total_revenue - total_cost`
- `gross_margin_pct = ((total_revenue - total_cost) / total_revenue) * 100` (0 when revenue <= 0)
- `employees = count(distinct employee in project)`

### Project Status Logic

- If `gross_margin_pct > 40` → `Healthy`
- If `30 <= gross_margin_pct <= 40` → `Optimal`
- If `gross_margin_pct < 30` → `At Risk`

### Project Trends

Built from month-wise series (`revenue`, `cost`, `profit`, `margin`):

- `Up` when latest period is materially above baseline
- `Down` when materially below baseline
- `Stable` for small movement band

(Implemented by `_trend_from_values(...)`.)

---

## 5) Employee Summary (`/employees`)

Source file: `ingestion/dataset.py` → `build_employee_summaries(...)`

For each employee:

- `total_hours = sum(actual_hours)`
- `total_revenue = sum(revenue)`
- `total_profit = sum(revenue - cost)`
- `utilization_pct = (total_hours / approved_hours_total) * 100`
  - where `approved_hours_total = sum(expected_hours if present else max_hours)`
  - returns `null` if approved hours total is 0

Projects nested under each employee:

- per-project `hours`, `revenue`, `profit`

Contribution band (relative ranking by total profit):

- Top 25% → `High`
- Bottom 25% → `Low`
- Others → `Optimal`

---

## 6) API Endpoints Using These Calculations

Source file: `main.py`

- `/metrics` → overall + monthly + base project map
- `/projects` → detailed project summaries (`build_project_summaries`)
- `/employees` → employee summaries (`build_employee_summaries`)

---

## 7) Safety Rules in Numeric Math

Source file: `ingestion/dataset.py`

- `_to_num(...)` safely converts invalid/blank values to `0.0`
- `_calc_margin(...)` returns `0.0` when revenue is `<= 0`

This prevents divide-by-zero and bad numeric parsing from breaking summaries.
