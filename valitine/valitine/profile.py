"""
Valitine — Mode 1: Technical Profile
Auto-profile a DataFrame and generate a snapshot report.
"""

import pandas as pd
from datetime import datetime
from pathlib import Path


def profile(df: pd.DataFrame, name: str = "dataset", output_dir: str = "reports") -> str:
    """
    Mode 1: Auto-profile a DataFrame.
    Input only df — Valitine figures out the rest.

    Returns the path to the generated .md report.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    run_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    file_date = datetime.now().strftime("%Y-%m-%d")
    filename = f"{output_dir}/{file_date}_{name}_profile.md"

    results = _run_checks(df)
    md = _build_report(df, name, run_date, results)

    with open(filename, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"[Valitine] Mode 1 report saved → {filename}")
    return filename


def _run_checks(df: pd.DataFrame) -> list[dict]:
    results = []

    for col in df.columns:
        series = df[col] #get rows each a count columns
        total = len(series) #get count rows
        null_count = series.isna().sum() #count Null from a column
        null_pct = round(null_count / total * 100, 1) if total > 0 else 0 #rount null count if null count>0
        dtype = str(series.dtype) #Data type
        unique_count = series.nunique(dropna=True) #count the unique value(drop dup)

        flags = []
        severity = "✅ ok"

        # Null check
        if null_pct >= 60:
            flags.append(f"null สูงมาก ({null_pct}%) — ควรตรวจสอบก่อนใช้งาน")
            severity = "🔴 error"
        elif null_pct >= 20:
            flags.append(f"null {null_pct}% — ควรดูเพิ่ม")
            severity = "⚠️ warn"
        elif null_pct > 0:
            flags.append(f"null {null_pct}%")
            if severity == "✅ ok":
                severity = "ℹ️ info"

        # Type hint
        if dtype == "object":
            sample = series.dropna().head(5).tolist()
            # Guess if it might be datetime
            try:
                pd.to_datetime(series.dropna().head(20))
                flags.append("type: object — อาจเป็น datetime ลอง pd.to_datetime()")
                if severity == "✅ ok":
                    severity = "ℹ️ info"
            except Exception:
                pass

        # Cardinality
        if unique_count <= 10 and dtype == "object":
            flags.append(f"cardinality ต่ำ ({unique_count} unique) — อาจเป็น category")
            if severity == "✅ ok":
                severity = "ℹ️ info"

        # Numeric range
        if pd.api.types.is_numeric_dtype(series):
            min_val = series.min()
            max_val = series.max()
            flags.append(f"range: {min_val} – {max_val}")

            # Negative values in likely-positive columns
            col_lower = col.lower()
            if min_val < 0 and any(kw in col_lower for kw in ["age", "price", "amount", "count", "qty", "quantity"]):
                flags.append(f"พบค่าติดลบ ({min_val}) — ควรตรวจสอบ")
                severity = "⚠️ warn"

        results.append({
            "column": col,
            "dtype": dtype,
            "null_count": null_count,
            "null_pct": null_pct,
            "unique": unique_count,
            "severity": severity,
            "flags": flags,
        })

    return results


def _build_report(df: pd.DataFrame, name: str, run_date: str, results: list[dict]) -> str:
    total_rows, total_cols = df.shape
    dup_count = df.duplicated().sum()
    dup_pct = round(dup_count / total_rows * 100, 1) if total_rows > 0 else 0

    error_cols = [r for r in results if "🔴" in r["severity"]]
    warn_cols = [r for r in results if "⚠️" in r["severity"]]

    lines = []
    lines.append(f"# Valitine — Mode 1: Technical Profile")
    lines.append(f"")
    lines.append(f"**Dataset:** `{name}`  ")
    lines.append(f"**Run date:** {run_date}  ")
    lines.append(f"  **Columns:** {total_cols} &nbsp;|&nbsp; **Rows:** {total_rows:,}  ")
    lines.append(f"")

    # Summary box
    lines.append(f"## Summary")
    lines.append(f"")
    lines.append(f"| | |")
    lines.append(f"|---|---|")
    lines.append(f"| Duplicate rows | {dup_count:,} ({dup_pct}%) |")
    lines.append(f"| Columns with 🔴 error | {len(error_cols)} |")
    lines.append(f"| Columns with ⚠️ warn | {len(warn_cols)} |")
    lines.append(f"")

    if dup_count > 0:
        lines.append(f"> ⚠️ พบ duplicate {dup_count:,} rows ({dup_pct}%) — ขึ้นอยู่กับ business ว่าเป็นปัญหามั้ย")
        lines.append(f"")

    # Column detail
    lines.append(f"## Column Details")
    lines.append(f"")
    lines.append(f"| # | Column | Type | Null | Unique | Status | Notes |")
    lines.append(f"|---|--------|------|------|--------|--------|-------|")

    for i, r in enumerate(results, 1):
        notes = " / ".join(r["flags"]) if r["flags"] else "—"
        lines.append(
            f"| {i} | `{r['column']}` | {r['dtype']} | {r['null_count']} ({r['null_pct']}%) "
            f"| {r['unique']} | {r['severity']} | {notes} |"
        )

    lines.append(f"")
    lines.append(f"---")
    lines.append(f"*Generated by Valitine — Mode 1 (Technical Profile)*")

    return "\n".join(lines)
