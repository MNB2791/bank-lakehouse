from pyspark.sql import Column, functions as F


def mask_ssn(c: Column) -> Column:
    """123-45-6789 -> ***-**-6789"""
    return F.concat(F.lit("***-**-"), F.substring(c, -4, 4))


def hash_pii(c: Column, salt: str) -> Column:
    """Salted SHA-256 token: joinable across tables, not reversible."""
    return F.sha2(F.concat_ws("|", c, F.lit(salt)), 256)


def direction(txn_type: Column) -> Column:
    return F.when(txn_type == "CREDIT", F.lit("IN")).otherwise(F.lit("OUT"))


def signed_amount(amount: Column, direction_col: Column) -> Column:
    return F.when(direction_col == "IN", amount).otherwise(-amount)


def age_band(dob: Column) -> Column:
    age = F.floor(F.months_between(F.current_date(), dob) / 12)
    return (F.when(age < 25, "18-24").when(age < 40, "25-39")
             .when(age < 60, "40-59").otherwise("60+"))