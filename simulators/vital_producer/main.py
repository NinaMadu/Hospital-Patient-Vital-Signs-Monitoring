"""Bedside vital-sign simulator and Kafka producer.

Owner: Member A (Day 4).
Emits one JSON reading per patient every 2-5 s to topic patient-vitals, keyed by patient_id.
Supports seeded, deterministic scenarios for the demo, e.g. --scenario spike --patient P007.
"""
