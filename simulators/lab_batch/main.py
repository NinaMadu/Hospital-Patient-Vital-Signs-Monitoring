"""Daily pathology-lab file simulator.

Owner: Member B (Day 4).
At the end of each simulated day writes /data/landing/labs/labs_day=N.csv atomically
(tmp file -> rename -> _SUCCESS marker). Includes missing and out-of-range results.
"""
