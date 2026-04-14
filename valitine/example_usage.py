"""
example_usage.py — ตัวอย่างการใช้งาน Valitine

Run: python example_usage.py
Reports จะถูกสร้างใน ./reports/
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import valitine as vt

# ─────────────────────────────────────────────
# สร้าง sample data
# ─────────────────────────────────────────────

df = pd.DataFrame({
    "customer_id":   [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "customer_name": ["Alice", "Bob", "Carol", None, "Eve", "Frank", "Grace", None, "Heidi", "Ivan"],
    "customer_age":  [25, 30, "unknown", 45, -5, 200, 33, 28, None, 41],
    "customer_email":["alice@email.com", "bob@email.com", "not-an-email",
                      "dave@email.com", "eve@email.com", "frank@email.com",
                      "grace@", "heidi@email.com", "heidi@email.com", "ivan@email.com"],
    "status":        ["active", "inactive", "active", "active", "deleted",
                      "active", "inactive", "active", "active", "inactive"],
    "created_at":    ["2023-01-01", "2023-03-15", "2099-12-31", "2022-06-01",
                      "2023-07-04", "2023-08-08", "not-a-date", "2023-09-01",
                      "2023-10-10", "2023-11-11"],
})

print("=" * 55)
print("  Valitine Example")
print("=" * 55)

# ─────────────────────────────────────────────
# MODE 1: Auto Profile
# ─────────────────────────────────────────────
print("\n── Mode 1: profile() ──")
vt.profile(df, name="customer_info", output_dir="reports")

# ─────────────────────────────────────────────
# MODE 2: Business Checks (ทำทีละ column)
# ─────────────────────────────────────────────
print("\n── Mode 2: Business Checks ──")

# ครั้งที่ 1: เช็ค age ก่อน
vt.check_age(df, "customer_age", table="customer_info", output_dir="reports")

# ครั้งที่ 2: เพิ่ม email check
vt.check_email(df, "customer_email", table="customer_info", output_dir="reports")

# ครั้งที่ 3: เช็ค date
vt.check_date(df, "created_at", table="customer_info", output_dir="reports")

# ครั้งที่ 4: เช็ค category
vt.check_category(df, "status", table="customer_info",
                  allowed=["active", "inactive"], output_dir="reports")

# Mark columns ที่ไม่ต้องเช็ค
vt.skip(df, "customer_id", table="customer_info",
        reason="Primary key ไม่จำเป็นต้องเช็ค business logic", output_dir="reports")
vt.skip(df, "customer_name", table="customer_info",
        reason="Free-text field ไม่มี rule ตายตัว", output_dir="reports")

print("\n✅ Done! ดู reports ได้ที่ ./reports/")
