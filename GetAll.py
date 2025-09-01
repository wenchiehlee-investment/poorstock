#!/usr/bin/env python3
"""
GetAll.py - PoorStock Web Scraper (calls poorstock.py for each stock)
"""

import pandas as pd
import subprocess
from pathlib import Path
import argparse
import logging
from datetime import datetime

class PoorStockBatchRunner:
    def __init__(self, base_dir: str = "."):
        self.base_dir = Path(base_dir)
        self.csv_file = self.base_dir / "StockID_TWSE_TPEX.csv"
        self.poorstock_py = self.base_dir / "poorstock.py"
        self.poorstock_dir = self.base_dir / "poorstock"
        self.poorstock_dir.mkdir(exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.poorstock_dir / 'batch_runner.log', encoding='utf-8'),
                logging.StreamHandler(open(1, 'w', encoding='utf-8'))  # 指定 stdout 為 utf-8
            ]
        )
        self.logger = logging.getLogger(__name__)

    def load_stock_data(self) -> pd.DataFrame:
        if not self.csv_file.exists():
            raise FileNotFoundError(f"Stock CSV file not found: {self.csv_file}")
        df = pd.read_csv(self.csv_file)
        self.logger.info(f"Loaded {len(df)} stocks from {self.csv_file}")
        return df

    def run_single(self, stock_id: int):
        """Call poorstock.py for a single stock."""
        cmd = [
            "python", str(self.poorstock_py), str(stock_id)
        ]
        self.logger.info(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=self.base_dir)
        return result.returncode == 0

    def run_all(self):
        """Batch process all stocks using poorstock.py."""
        stock_df = self.load_stock_data()
        total = len(stock_df)
        success = 0
        fail = 0
        for i, row in enumerate(stock_df.itertuples(), 1):
            stock_id = row.代號
            stock_name = row.名稱
            self.logger.info(f"[{i}/{total}] Processing {stock_id} ({stock_name})")
            if self.run_single(stock_id):
                success += 1
            else:
                fail += 1
        self.logger.info(f"Batch complete: Success={success}, Fail={fail}")

    def get_status_report(self):
        """Show status report based on markdown files."""
        stock_df = self.load_stock_data()
        md_files = list(self.poorstock_dir.glob("poorstock_*.md"))
        processed = len(md_files)
        total = len(stock_df)
        return {
            "total_stocks": total,
            "md_files_generated": processed,
            "remaining": total - processed,
            "last_run": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

def main():
    parser = argparse.ArgumentParser(description='GetAll - PoorStock Batch Runner')
    parser.add_argument('--stock-id', type=int, help='Process specific stock ID only')
    parser.add_argument('--status', action='store_true', help='Show current status report')
    parser.add_argument('--base-dir', default='.', help='Base directory for files')
    args = parser.parse_args()

    runner = PoorStockBatchRunner(args.base_dir)

    if args.status:
        report = runner.get_status_report()
        print("\n=== PoorStock Batch Status Report ===")
        for key, value in report.items():
            print(f"{key}: {value}")
    elif args.stock_id:
        runner.run_single(args.stock_id)
    else:
        runner.run_all()

if __name__ == "__main__":
    main()