# Phase 5 - AI / ML Weather Intelligence

Phase 5 extends the trusted Phase 3/4 Delta Lake pipeline with a dedicated AI layer. The AI layer does not bypass Silver/Gold quality controls.

## Architecture

```text
Open-Meteo
   |
Bronze Delta
   |
Silver Delta
   |
Data Quality
   |
Gold Delta
   |
01_ai_features
   |
weather_ai_features
   |
02_ai_weather_analysis
   |
weather_ai_analysis
   |
03_ai_insights
   |
weather_ai_insights
   |
04_ai_quality_check
   |
weather_ai_quality
```

## Four AI components

### 01_ai_features.py

Deterministic Spark feature engineering. No LLM calls. It creates:

- raw weather features;
- deltas and rolling statistics;
- precipitation/wind/temperature semantic categories;
- temperature z-score and anomaly flag;
- explainable severity score;
- deterministic risk;
- controlled `ai_context`;
- feature version and deterministic ID.

This is the **AI Features Layer**: it transforms Gold/Silver data into compact, semantically meaningful model-ready context.

### 02_ai_weather_analysis.py

Per-observation analysis. It always produces a deterministic analysis. Optionally, with `enable_llm=true`, it calls Databricks `ai_query` using the configured serving endpoint. The model receives only `ai_context`.

### 03_ai_insights.py

Combines AI analysis and deterministic features into a curated insight product. It assigns insight type and priority and stores the AI/deterministic output as Delta.

### 04_ai_quality_check.py

Quality gate for source data, features, AI output and AI/deterministic consistency. A consistency mismatch is marked `REVIEW`, while structurally invalid data fails the gate.

## Tables

| Table | Responsibility |
|---|---|
| `weather_ai_features` | AI-ready feature engineering |
| `weather_ai_analysis` | per-observation AI/deterministic analysis |
| `weather_ai_insights` | curated insight product |
| `weather_ai_quality` | AI quality and consistency controls |

## Generative AI

The default bundle keeps LLM disabled (`enable_llm=false`) so the complete pipeline can run without a model endpoint. To enable it, configure a valid Databricks AI serving endpoint and set `enable_llm=true` in the Phase 5 analysis task. Never commit credentials.

## Design principles

1. Spark performs deterministic calculations.
2. LLM performs interpretation, not basic arithmetic.
3. Prompt/context is controlled and versioned.
4. AI output is persisted as data.
5. Delta remains the system of record.
6. AI quality is a first-class pipeline stage.
