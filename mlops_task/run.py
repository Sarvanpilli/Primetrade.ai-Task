import argparse
import yaml
import pandas as pd
import numpy as np
import json
import time
import logging
import os
import sys
import io

def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

def write_metrics(output_path, metrics):
    with open(output_path, 'w') as f:
        json.dump(metrics, f, indent=4)
    # Requirement: Print final metrics JSON to stdout
    print(json.dumps(metrics, indent=4))

def main():
    # 1. CLI Argument Parsing
    parser = argparse.ArgumentParser(description="MLOps Batch Job")
    parser.add_argument("--input", required=True, help="Path to input data.csv")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--output", required=True, help="Path to metrics.json")
    parser.add_argument("--log-file", required=True, help="Path to run.log")
    args = parser.parse_args()

    # Initialize basic metrics for error case
    start_time = time.time()
    setup_logging(args.log_file)
    logging.info("Job started")
    
    config = {}
    
    try:
        # 2. Load + Validate Config
        if not os.path.exists(args.config):
            raise FileNotFoundError(f"Config file not found: {args.config}")
        
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
        
        # Validate keys
        for key in ['seed', 'window', 'version']:
            if key not in config:
                raise ValueError(f"Missing required config field: {key}")
        
        logging.info(f"Config loaded: {config}")
        
        # Set Seed for reproducibility
        np.random.seed(config['seed'])

        # 3. Load + Validate Dataset
        if not os.path.exists(args.input):
            raise FileNotFoundError(f"Input file not found: {args.input}")
            
        # data.csv wraps every row in outer double-quotes; strip them before parsing
        with open(args.input, 'r') as raw_f:
            cleaned = ''.join(line.strip().strip('"') + '\n' for line in raw_f)
        df = pd.read_csv(io.StringIO(cleaned))
        
        if df.empty:
            raise ValueError("Input CSV is empty")
        if 'close' not in df.columns:
            raise ValueError("Missing required column: 'close'")
            
        logging.info(f"Dataset loaded: {len(df)} rows")

        # 4. Processing: Rolling Mean
        # We handle the first 'window-1' rows by dropping NaNs for the signal rate calculation
        window = config['window']
        df['rolling_mean'] = df['close'].rolling(window=window).mean()
        logging.info(f"Computed rolling mean (window={window})")

        # 5. Processing: Signal Generation
        # signal = 1 if close > rolling_mean, else 0
        df['signal'] = (df['close'] > df['rolling_mean']).astype(int)
        logging.info("Generated binary signals")

        # 6. Metrics Calculation
        # Drop NaNs created by rolling window so they don't skew the mean signal rate
        valid_signals = df['signal'][window-1:] 
        
        latency_ms = int((time.time() - start_time) * 1000)
        
        metrics = {
            "version": config["version"],
            "rows_processed": len(df),
            "metric": "signal_rate",
            "value": round(float(valid_signals.mean()), 4),
            "latency_ms": latency_ms,
            "seed": config["seed"],
            "status": "success"
        }
        
        logging.info(f"Metrics summary: {metrics}")
        write_metrics(args.output, metrics)
        logging.info("Job completed successfully")

    except Exception as e:
        error_msg = str(e)
        logging.error(f"Job failed: {error_msg}")
        
        # Structured Error Output
        error_metrics = {
            "version": config.get("version", "unknown"),
            "status": "error",
            "error_message": error_msg
        }
        write_metrics(args.output, error_metrics)
        sys.exit(1) # Non-zero exit on failure

if __name__ == "__main__":
    main()