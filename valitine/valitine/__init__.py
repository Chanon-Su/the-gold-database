"""
Valitine — Validate + Valentine 💌
A lightweight data validation & profiling tool for Pandas DataFrames.

Usage:
    import valitine as vt

    # Mode 1: Auto profile (technical overview)
    vt.profile(df, name="orders")

    # Mode 2: Business checks (living report per table)
    vt.check_age(df, "customer_age", table="customer_info")
    vt.check_email(df, "customer_email", table="customer_info")
    vt.check_date(df, "created_at", table="customer_info")
    vt.check_category(df, "status", table="orders", allowed=["active", "inactive"])
    vt.check_range(df, "price", table="orders", min_val=0)
    vt.skip(df, "internal_id", table="customer_info", reason="ไม่จำเป็นต้องเช็ค")
"""

from .profile import profile
from .checks import (
    check_age,
    check_email,
    check_phone,
    check_date,
    check_category,
    check_range,
    skip,
)

__version__ = "0.1.0"
__all__ = [
    "profile",
    "check_age",
    "check_email",
    "check_phone",
    "check_date",
    "check_category",
    "check_range",
    "skip",
]
