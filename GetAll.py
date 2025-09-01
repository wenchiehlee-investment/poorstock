#!/usr/bin/env python3
"""
GetAll.py - PoorStock Batch Runner (Windows Compatible)
Calls poorstock.py for each stock with smart processing logic
"""

import pandas as pd
import subprocess
from pathlib import Path
import argparse
import logging
from datetime import datetime
import os
import sys
import codecs

# Fix encoding issues on Windows
if sys.platform == "win32":
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

def safe_print(message):
    """Print function that handles Unicode errors gracefully."""
    try:
        print(message)
    except UnicodeEncodeError:
        # Replace problematic Unicode characters with ASCII equivalents
        safe_message = (message
                       .replace("🧠", "[BRAIN]")
                       .replace("📊", "[DATA]")
                       .replace("✅", "[OK]")
                       .replace("❌", "[ERROR]")
                       .replace("🚀", "[START]")
                       .replace("🎉", "[SUCCESS]")
                       .replace("🎯", "[TARGET]"))
        print(safe_message)

class PoorStockBatchRunner:
    def __init__(self, base_dir: str = "."):
        self.base_dir = Path(base_dir)
        self.csv_file = self.base_dir / "StockID_TWSE_TPEX.csv"
        self.poorstock_py = self.base_dir / "poorstock.py"
        self.poorstock_dir = self.base_dir / "poorstock"
        self.results_file = self.poorstock_dir / "download_results.csv"
        self.poorstock_dir.mkdir(exist_ok=True)

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.poorstock_dir / 'batch_runner.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def safe_log(self, level, message):
        """Log function that handles Unicode errors gracefully."""
        try:
            if level == "info":
                self.logger.info(message)
            elif level == "error":
                self.logger.error(message)
        except UnicodeEncodeError:
            safe_message = (message
                           .replace("🧠", "[BRAIN]")
                           .replace("📊", "[DATA]")
                           .replace("✅", "[OK]")
                           .replace("❌", "[ERROR]")
                           .replace("🚀", "[START]")
                           .replace("🎉", "[SUCCESS]"))
            if level == "info":
                self.logger.info(safe_message)
            elif level == "error":
                self.logger.error(safe_message)

    def load_stock_data(self) -> pd.DataFrame:
        """Load stock list from CSV."""
        if not self.csv_file.exists():
            raise FileNotFoundError(f"Stock CSV file not found: {self.csv_file}")
        df = pd.read_csv(self.csv_file)
        self.safe_log("info", f"Loaded {len(df)} stocks from {self.csv_file}")
        return df

    def load_or_create_results_csv(self) -> pd.DataFrame:
        """Load existing results CSV or create new one."""
        if self.results_file.exists():
            df = pd.read_csv(self.results_file)
            self.safe_log("info", f"Loaded existing results: {len(df)} records")
        else:
            df = pd.DataFrame(columns=['filename', 'last_update_time', 'success', 'process_time'])
            self.safe_log("info", "Created new results tracking file")
        return df

    def get_expected_filename(self, stock_id: int, stock_name: str) -> str:
        """Generate expected filename for a stock."""
        return f"poorstock_{stock_id}_{stock_name}.md"

    def determine_processing_strategy(self, stock_df: pd.DataFrame, results_df: pd.DataFrame) -> tuple:
        """
        Determine which processing strategy to use based on current state.
        Returns (strategy_name, stock_ids_to_process)
        """
        today = datetime.now().strftime('%Y-%m-%d')
        
        if results_df.empty:
            return "INITIAL_SCAN", stock_df['代號'].tolist()
        
        failed_stocks = []
        unprocessed_stocks = []
        old_successful_stocks = []
        
        for _, stock_row in stock_df.iterrows():
            stock_id = stock_row['代號']
            expected_filename = self.get_expected_filename(stock_id, stock_row['名稱'])
            
            result_mask = results_df['filename'] == expected_filename
            if result_mask.any():
                result_row = results_df[result_mask].iloc[0]
                
                if not result_row['success']:
                    failed_stocks.append(stock_id)
                elif result_row['success'] and not result_row['last_update_time'].startswith(today):
                    old_successful_stocks.append(stock_id)
            else:
                unprocessed_stocks.append(stock_id)
        
        # Decision logic
        if failed_stocks or unprocessed_stocks:
            return "PRIORITY", failed_stocks + unprocessed_stocks
        elif old_successful_stocks:
            return "FULL_REFRESH", stock_df['代號'].tolist()
        else:
            return "UP_TO_DATE", []

    def run_single(self, stock_id: int) -> bool:
        """Call poorstock.py for a single stock."""
        cmd = [sys.executable, str(self.poorstock_py), str(stock_id)]
        
        self.safe_log("info", f"Running: {' '.join(cmd)}")
        try:
            # Set environment variables for proper UTF-8 encoding
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            if sys.platform == "win32":
                env['PYTHONLEGACYWINDOWSSTDIO'] = '0'
            
            result = subprocess.run(
                cmd, 
                cwd=self.base_dir,
                capture_output=True,
                text=True,
                encoding='utf-8',
                env=env,
                errors='replace'  # Replace problematic characters instead of failing
            )
            
            if result.returncode == 0:
                self.safe_log("info", f"[OK] Successfully processed stock {stock_id}")
                return True
            else:
                self.safe_log("error", f"[ERROR] Failed to process stock {stock_id}")
                if result.stderr:
                    self.safe_log("error", f"Error output: {result.stderr}")
                return False
                
        except Exception as e:
            self.safe_log("error", f"[ERROR] Exception running stock {stock_id}: {e}")
            return False

    def run_intelligent_batch(self):
        """Batch process using intelligent strategy selection."""
        stock_df = self.load_stock_data()
        results_df = self.load_or_create_results_csv()
        
        strategy, stock_ids_to_process = self.determine_processing_strategy(stock_df, results_df)
        
        self.safe_log("info", f"[BRAIN] Processing strategy: {strategy}")
        self.safe_log("info", f"[DATA] Stocks to process: {len(stock_ids_to_process)}")
        
        if strategy == "UP_TO_DATE":
            self.safe_log("info", "[OK] All stocks are up to date. No processing needed.")
            return
        
        # Execute processing
        success_count = 0
        fail_count = 0
        total = len(stock_ids_to_process)
        
        for i, stock_id in enumerate(stock_ids_to_process, 1):
            stock_row = stock_df[stock_df['代號'] == stock_id].iloc[0]
            stock_name = stock_row['名稱']
            
            self.safe_log("info", f"[{i}/{total}] Processing {stock_id} ({stock_name})")
            
            if self.run_single(stock_id):
                success_count += 1
            else:
                fail_count += 1
            
            # Small delay between requests
            import time
            time.sleep(10)
        
        self.safe_log("info", f"[SUCCESS] Batch processing complete!")
        self.safe_log("info", f"[OK] Successful: {success_count}")
        self.safe_log("info", f"[ERROR] Failed: {fail_count}")

    def run_all_stocks(self):
        """Process all stocks regardless of previous status."""
        stock_df = self.load_stock_data()
        total = len(stock_df)
        success = 0
        fail = 0
        
        self.safe_log("info", f"[START] Processing all {total} stocks...")
        
        for i, row in enumerate(stock_df.itertuples(), 1):
            stock_id = row.代號
            stock_name = row.名稱
            self.safe_log("info", f"[{i}/{total}] Processing {stock_id} ({stock_name})")
            
            if self.run_single(stock_id):
                success += 1
            else:
                fail += 1
            
            # Delay between requests
            import time
            time.sleep(10)
        
        self.safe_log("info", f"[SUCCESS] All stocks processed: Success={success}, Fail={fail}")

    def get_status_report(self) -> dict:
        """Generate a status report of current processing state."""
        try:
            stock_df = self.load_stock_data()
            results_df = self.load_or_create_results_csv()
            
            strategy, to_process = self.determine_processing_strategy(stock_df, results_df)
            
            successful = len(results_df[results_df['success'] == True]) if not results_df.empty else 0
            failed = len(results_df[results_df['success'] == False]) if not results_df.empty else 0
            unprocessed = len(stock_df) - len(results_df) if not results_df.empty else len(stock_df)
            
            # Count actual markdown files
            md_files = list(self.poorstock_dir.glob("poorstock_*.md"))
            
            return {
                'total_stocks': len(stock_df),
                'successful': successful,
                'failed': failed,
                'unprocessed': unprocessed,
                'md_files_found': len(md_files),
                'current_strategy': strategy,
                'stocks_to_process': len(to_process),
                'last_check': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            self.safe_log("error", f"Error generating status report: {e}")
            return {'error': str(e)}

def main():
    parser = argparse.ArgumentParser(description='GetAll - PoorStock Batch Runner with Smart Processing')
    parser.add_argument('--stock-id', type=int, help='Process specific stock ID only')
    parser.add_argument('--status', action='store_true', help='Show current status report')
    parser.add_argument('--all', action='store_true', help='Process all stocks (ignore smart logic)')
    parser.add_argument('--base-dir', default='.', help='Base directory for files')
    
    args = parser.parse_args()
    
    runner = PoorStockBatchRunner(args.base_dir)
    
    if args.status:
        report = runner.get_status_report()
        safe_print("\n=== PoorStock Processing Status Report ===")
        for key, value in report.items():
            safe_print(f"{key.replace('_', ' ').title()}: {value}")
    elif args.stock_id:
        safe_print(f"[TARGET] Processing single stock: {args.stock_id}")
        if runner.run_single(args.stock_id):
            safe_print(f"[OK] Successfully processed stock {args.stock_id}")
        else:
            safe_print(f"[ERROR] Failed to process stock {args.stock_id}")
            sys.exit(1)
    elif args.all:
        safe_print("[START] Processing ALL stocks (bypassing smart logic)")
        runner.run_all_stocks()
    else:
        safe_print("[BRAIN] Using intelligent processing strategy")
        runner.run_intelligent_batch()

if __name__ == "__main__":
    main()