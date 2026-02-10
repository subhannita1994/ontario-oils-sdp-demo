# Databricks notebook source
"""
Metrics Layer - Pipeline Contract Tables

This module provides reconciliation and observability tables that are
materialized as first-class pipeline outputs. These contract tables
enable downstream consumers to verify data quality and completeness
before consuming the data.

Tables:
- pipeline_bronze_metrics: Ingestion counts, operation breakdown, watermarks
- pipeline_silver_metrics: Applied change counts, SCD metrics, watermarks
- pipeline_quality_metrics: Schema violations, expectation results
- pipeline_contract_summary: Unified view for downstream consumers
"""
