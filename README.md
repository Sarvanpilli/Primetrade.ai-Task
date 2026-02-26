# MLOps Batch Job

A self-contained MLOps batch pipeline that ingests BTC/USD OHLCV data, computes a rolling-mean trading signal, and outputs structured metrics as JSON.

---

## Project Structure

```
mlops_task/
├── Dockerfile          # Container definition
├── requirenments.txt   # Python dependencies
├── config.yaml         # Job configuration
├── data.csv            # Input OHLCV dataset (10,000 rows of BTC/USD 1-min candles)
├── run.py              # Main pipeline script
├── metrics.json        # Output metrics (generated on run)
└── run.log             # Execution log (generated on run)


## How It Works

1. **Load config** from `config.yaml` (`seed`, `window`, `version`)
2. **Load & validate** `data.csv` — checks for required `close` column
3. **Compute rolling mean** over the `close` price using the configured `window`
4. **Generate signal** — `1` if `close > rolling_mean`, else `0`
5. **Output metrics** to `metrics.json` and log to `run.log`

---

## Configuration

**`config.yaml`**
```yaml
seed: 42      # Random seed for reproducibility
window: 5     # Rolling mean window size (in rows)
version: "v1" # Pipeline version tag
```

---

## Local Run (Python)

### Prerequisites
```bash
pip install -r requirenments.txt
```

### Run
```bash
python run.py \
  --input data.csv \
  --config config.yaml \
  --output metrics.json \
  --log-file run.log
```

### CLI Arguments

| Argument | Description |
|---|---|
| `--input` | Path to the input CSV file |
| `--config` | Path to the YAML config file |
| `--output` | Path for the output metrics JSON file |
| `--log-file` | Path for the execution log file |

---

## Docker Build & Run

### Build
```bash
docker build -t mlops-task .
```

### Run
```bash
docker run mlops-task
```

### Run with custom output (mount local directory)
```bash
docker run -v ${PWD}/output:/app/output mlops-task \
  --input data.csv \
  --config config.yaml \
  --output output/metrics.json \
  --log-file output/run.log
```

> **Note:** The default `CMD` in the Dockerfile runs the job automatically with `data.csv` and `config.yaml` already bundled inside the image.

---

## Example Output — `metrics.json`

```json
{
    "version": "v1",
    "rows_processed": 10000,
    "metric": "signal_rate",
    "value": 0.4991,
    "latency_ms": 19,
    "seed": 42,
    "status": "success"
}
```

| Field | Description |
|---|---|
| `version` | Pipeline version from `config.yaml` |
| `rows_processed` | Total rows in the input CSV |
| `metric` | Metric name (`signal_rate`) |
| `value` | Fraction of rows where `close > rolling_mean` |
| `latency_ms` | Total job execution time in milliseconds |
| `seed` | Random seed used |
| `status` | `"success"` or `"error"` |

---

## Error Handling

If the job fails (e.g. missing file, bad config), `metrics.json` will contain a structured error:

```json
{
    "version": "v1",
    "status": "error",
    "error_message": "Missing required column: 'close'"
}
```

The process exits with **code `1`** on failure, making it compatible with CI/CD pipelines and container orchestrators.

---

## Dependencies

| Package | Purpose |
|---|---|
| `pandas` | Data loading and rolling window computation |
| `numpy` | Random seed setup |
| `pyyaml` | YAML config parsing |
