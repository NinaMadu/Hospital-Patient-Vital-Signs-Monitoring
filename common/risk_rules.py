"""Risk scoring rules shared by the speed layer (Spark streaming) and the batch layer.

Owner: Member B.
Thresholds come from config/thresholds.yaml. Keeping one implementation here is how the
project limits Lambda's duplicated-logic problem.

Every rule exists in two forms built from the same thresholds:
  * plain Python  - for the API, the report, Airflow checks and unit tests
        vital_points(avg_heart_rate=130, min_spo2=90)            -> Score(points=3, reasons=(...))
        lab_points(6.2, 3.5, 5.1)                                -> 1
        category(3)                                              -> "WATCH"
        combine(vital_score=2, lab_score=None)                   -> Combined(2, "WATCH", "LAB_UNAVAILABLE")
  * Spark Column  - for Q1 / Q3 (streaming) and the risk join (batch)
        df.withColumn("vital_risk_score", vital_points_col())
        df.withColumn("lab_points", lab_points_col("result_value", "reference_low", "reference_high"))
        df.withColumn("risk_category", category_col("total_risk_score"))
tests/slice_b/test_risk_rules.py checks that both forms give the same answer.

A missing value (None / NULL) adds no points: no reading is not evidence of risk. A patient
with no lab results is reported as LAB_UNAVAILABLE by combine(), never as a lab score of 0.

These are project-defined rules for a data-engineering exercise, NOT clinical guidance.
"""
from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, Any, Iterable, Mapping, NamedTuple

from common.config import load_thresholds

if TYPE_CHECKING:  # pyspark is only imported when a *_col function is called,
    from pyspark.sql import Column  # so the API image (no pyspark) can use this module.

LAB_AVAILABLE = "AVAILABLE"
LAB_UNAVAILABLE = "LAB_UNAVAILABLE"


class Score(NamedTuple):
    points: int
    reasons: tuple[str, ...]  # e.g. ("spo2_low", "heart_rate_high")


class Combined(NamedTuple):
    total: int
    category: str
    lab_status: str  # AVAILABLE / LAB_UNAVAILABLE


class RiskRules:
    """All rules for one set of thresholds. Use the module functions for the project config."""

    def __init__(self, thresholds: Mapping[str, Any]):
        v, labs = thresholds["vitals"], thresholds["labs"]
        self.spo2_below = v["spo2_min"]["below"]
        self.spo2_points = v["spo2_min"]["points"]
        self.hr_below = v["heart_rate_avg"]["below"]
        self.hr_above = v["heart_rate_avg"]["above"]
        self.hr_points = v["heart_rate_avg"]["points"]
        self.temp_above = v["temperature_max"]["above"]
        self.temp_points = v["temperature_max"]["points"]
        self.sbp_below = v["systolic_bp"]["below"]
        self.sbp_above = v["systolic_bp"]["above"]
        self.sbp_points = v["systolic_bp"]["points"]
        self.trend_windows = v["trend"]["consecutive_windows"]
        self.trend_points = v["trend"]["points"]
        self.lab_out_points = labs["out_of_range_points"]
        self.lab_far_points = labs["far_out_of_range_points"]
        self.lab_far_factor = labs["far_out_of_range_factor"]
        self.categories = _ordered_categories(thresholds["categories"])

    # ---------------------------------------------------------- plain Python --

    def vital_points(
        self,
        avg_heart_rate: float | None = None,
        min_spo2: float | None = None,
        max_temperature: float | None = None,
        max_systolic_bp: float | None = None,
        min_systolic_bp: float | None = None,
        trend: bool | None = False,
    ) -> Score:
        """Points for one window (or one day) of vitals. Arguments are the window aggregates."""
        # Each indicator scores at most once. A window can hold both a very high and a very
        # low systolic reading; blood pressure still adds its points once (reason: high).
        sbp_high = _gt(max_systolic_bp, self.sbp_above)
        sbp_low = _lt(min_systolic_bp, self.sbp_below)
        rules = [
            ("spo2_low", self.spo2_points, _lt(min_spo2, self.spo2_below)),
            ("heart_rate_low", self.hr_points, _lt(avg_heart_rate, self.hr_below)),
            ("heart_rate_high", self.hr_points, _gt(avg_heart_rate, self.hr_above)),
            ("temperature_high", self.temp_points, _gt(max_temperature, self.temp_above)),
            ("systolic_bp_high", self.sbp_points, sbp_high),
            ("systolic_bp_low", self.sbp_points, sbp_low and not sbp_high),
            ("trend", self.trend_points, bool(trend)),
        ]
        hit = [(name, pts) for name, pts, fired in rules if fired]
        return Score(sum(p for _, p in hit), tuple(name for name, _ in hit))

    def lab_flag(self, value: float | None, low: float | None, high: float | None) -> str | None:
        """'HIGH', 'LOW' or None (in range, or nothing to compare)."""
        if value is None or low is None or high is None:
            return None
        if value > high:
            return "HIGH"
        if value < low:
            return "LOW"
        return None

    def lab_points(self, value: float | None, low: float | None, high: float | None) -> int:
        """0 in range; +1 out of range; +2 beyond far_factor x the limit (high*1.5 or low/1.5)."""
        flag = self.lab_flag(value, low, high)
        if flag is None:
            return 0
        far = value > high * self.lab_far_factor if flag == "HIGH" else value < low / self.lab_far_factor
        return self.lab_far_points if far else self.lab_out_points

    def lab_score(self, results: Iterable[tuple[float | None, float | None, float | None]]) -> int | None:
        """Sum of lab_points over a patient's results; None when there are no results at all."""
        results = list(results)
        if not results:
            return None
        return sum(self.lab_points(v, lo, hi) for v, lo, hi in results)

    def category(self, total: int | None) -> str | None:
        """NORMAL / WATCH / CONCERNING for a total score (None stays None)."""
        if total is None:
            return None
        if total < 0:
            raise ValueError(f"risk score cannot be negative: {total}")
        for name, lo, hi in self.categories:
            if total >= lo and (hi is None or total <= hi):
                return name
        raise AssertionError("unreachable: categories are validated as contiguous")

    def combine(self, vital_score: int | None, lab_score: int | None) -> Combined:
        """Serving-layer merge: speed-view vital score + latest batch-view lab score."""
        total = (vital_score or 0) + (lab_score or 0)
        status = LAB_UNAVAILABLE if lab_score is None else LAB_AVAILABLE
        return Combined(total, self.category(total), status)

    # ----------------------------------------------------------- Spark Column --

    def vital_points_col(
        self,
        avg_heart_rate: str = "avg_heart_rate",
        min_spo2: str = "min_spo2",
        max_temperature: str = "max_temperature",
        max_systolic_bp: str = "max_systolic_bp",
        min_systolic_bp: str | None = "min_systolic_bp",
        trend: str | None = None,
    ) -> Column:
        """vital_points() as a Spark expression over the named columns (NULL adds 0)."""
        from pyspark.sql import functions as F

        def pts(cond, points):
            return F.when(cond, F.lit(points)).otherwise(F.lit(0))  # NULL cond -> otherwise -> 0

        hr, sbp_hi = F.col(avg_heart_rate), F.col(max_systolic_bp)
        sbp_lo = F.col(min_systolic_bp) if min_systolic_bp else F.lit(None)
        total = (
            pts(F.col(min_spo2) < self.spo2_below, self.spo2_points)
            + pts((hr < self.hr_below) | (hr > self.hr_above), self.hr_points)
            + pts(F.col(max_temperature) > self.temp_above, self.temp_points)
            + pts((sbp_hi > self.sbp_above) | (sbp_lo < self.sbp_below), self.sbp_points)
        )
        if trend:
            total = total + pts(F.col(trend), self.trend_points)
        return total.cast("int")

    def lab_flag_col(self, value: str = "result_value", low: str = "reference_low",
                     high: str = "reference_high") -> Column:
        from pyspark.sql import functions as F

        v, lo, hi = F.col(value), F.col(low), F.col(high)
        return (F.when(_any_null(v, lo, hi), F.lit(None).cast("string"))
                 .when(v > hi, F.lit("HIGH"))
                 .when(v < lo, F.lit("LOW")))

    def lab_points_col(self, value: str = "result_value", low: str = "reference_low",
                       high: str = "reference_high") -> Column:
        from pyspark.sql import functions as F

        v, lo, hi = F.col(value), F.col(low), F.col(high)
        return (
            F.when(_any_null(v, lo, hi), F.lit(0))
            .when(v > hi * self.lab_far_factor, F.lit(self.lab_far_points))
            .when(v > hi, F.lit(self.lab_out_points))
            .when(v < lo / self.lab_far_factor, F.lit(self.lab_far_points))
            .when(v < lo, F.lit(self.lab_out_points))
            .otherwise(F.lit(0))
            .cast("int")
        )

    def category_col(self, total: str = "total_risk_score") -> Column:
        from pyspark.sql import functions as F

        col = F.col(total)
        expr = None
        for name, lo, hi in self.categories:
            cond = (col >= lo) if hi is None else col.between(lo, hi)
            expr = F.when(cond, F.lit(name)) if expr is None else expr.when(cond, F.lit(name))
        return expr  # NULL or negative total -> NULL


def _lt(value, limit) -> bool:
    return value is not None and value < limit


def _gt(value, limit) -> bool:
    return value is not None and value > limit


def _any_null(*cols: Column) -> Column:
    cond = cols[0].isNull()
    for c in cols[1:]:
        cond = cond | c.isNull()
    return cond


def _ordered_categories(cats: Mapping[str, Mapping[str, int]]) -> list[tuple[str, int, int | None]]:
    """[(name, min, max)] sorted by min; checks they start at 0 and leave no gaps or overlaps."""
    ordered = sorted(((n, c["min"], c.get("max")) for n, c in cats.items()), key=lambda c: c[1])
    expected = 0
    for i, (name, lo, hi) in enumerate(ordered):
        last = i == len(ordered) - 1
        if lo != expected:
            raise ValueError(f"category {name} starts at {lo}, expected {expected}")
        if (hi is None) != last:
            raise ValueError("only the highest category may be open-ended (no max)")
        if hi is not None:
            if hi < lo:
                raise ValueError(f"category {name} has max {hi} < min {lo}")
            expected = hi + 1
    return ordered


# ------------------------------------------------------- project defaults --

@lru_cache(maxsize=1)
def default_rules() -> RiskRules:
    """Rules from config/thresholds.yaml, loaded once per process."""
    return RiskRules(load_thresholds())


def vital_points(**kwargs) -> Score:
    return default_rules().vital_points(**kwargs)


def lab_flag(value, low, high) -> str | None:
    return default_rules().lab_flag(value, low, high)


def lab_points(value, low, high) -> int:
    return default_rules().lab_points(value, low, high)


def lab_score(results) -> int | None:
    return default_rules().lab_score(results)


def category(total: int | None) -> str | None:
    return default_rules().category(total)


def combine(vital_score: int | None, lab_score: int | None) -> Combined:
    return default_rules().combine(vital_score, lab_score)


def vital_points_col(**kwargs) -> Column:
    return default_rules().vital_points_col(**kwargs)


def lab_flag_col(**kwargs) -> Column:
    return default_rules().lab_flag_col(**kwargs)


def lab_points_col(value: str = "result_value", low: str = "reference_low",
                   high: str = "reference_high") -> Column:
    return default_rules().lab_points_col(value, low, high)


def category_col(total: str = "total_risk_score") -> Column:
    return default_rules().category_col(total)
