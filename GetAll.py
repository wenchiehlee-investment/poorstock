#!/usr/bin/env python3
"""
Enhanced GetAll.py - Better handling of intermittent issues
Includes intelligent retry logic and rate limiting
"""

import pandas as pd
import subprocess
from pathlib import Path
import argparse
import logging
from datetime import datetime, timedelta
import os
import sys
import codecs
import time
import random

# Fix encoding issues on Windows
if sys.platform == "win32":
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

def safe_print(message):
    """Print function that handles Unicode errors gracefully."""
    try:
        print(message)
    except UnicodeEncodeError:
        safe_message = (message
                       .replace("🧠 ", "[BRAIN]")
                       .replace("📊", "[DATA]")
                       .replace("✅", "[OK]")
                       .replace("❌", "[ERROR]")
                       .replace("🚀", "[START]")
                       .replace("🎉", "[SUCCESS]")
                       .replace("🎯", "[TARGET]")
                       .replace("🔄", "[RETRY]")
                       .replace("⏳", "[WAIT]")
                       .replace("🛡️", "[PROTECT]"))
        print(safe_message)

class EnhancedBatchRunner:
    def __init__(self, base_dir: str = "."):
        self.base_dir = Path(base_dir)
        self.csv_file = self.base_dir / "StockID_TWSE_TPEX.csv"
        self.poorstock_py = self.base_dir / "poorstock.py"
        self.poorstock_dir = self.base_dir / "poorstock"
        self.results_file = self.poorstock_dir / "download_results.csv"
        self.poorstock_dir.mkdir(exist_ok=True)
        
        # Rate limiting settings
        self.base_delay = 8  # Base delay between requests (increased)
        self.max_delay = 30  # Maximum delay
        self.failure_penalty = 5  # Additional delay after failures
        self.consecutive_failures = 0
        
        # Retry settings
        self.max_retries = 3
        self.retry_delay_base = 10
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.poorstock_dir / 'enhanced_batch_runner.log', encoding='utf-8'),
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
            safe_message = message.encode('ascii', 'replace').decode('ascii')
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
            df = pd.DataFrame(columns=['filename', 'last_update_time', 'success', 'process_time', 'retry_count'])
            self.safe_log("info", "Created new results tracking file")
        return df

    def calculate_dynamic_delay(self):
        """Calculate delay based on recent failure rate."""
        delay = self.base_delay
        
        # Add penalty for consecutive failures
        if self.consecutive_failures > 0:
            penalty = min(self.consecutive_failures * self.failure_penalty, self.max_delay - self.base_delay)
            delay += penalty
        
        # Add random jitter to avoid synchronized requests
        jitter = random.uniform(0.5, 2.0)
        delay += jitter
        
        return min(delay, self.max_delay)

    def validate_stock_file(self, stock_id: int, stock_name: str) -> dict:
        """
        Validate if stock file has complete data.
        Returns validation results and recommendations.
        """
        filename = f"poorstock_{stock_id}_{stock_name}.md"
        filepath = self.poorstock_dir / filename
        
        result = {
            'exists': filepath.exists(),
            'complete': False,
            'has_prices': False,
            'has_daily_data': False,
            'has_ownership': False,
            'has_loading_messages': False,
            'recommendation': 'skip'
        }
        
        if not result['exists']:
            result['recommendation'] = 'process'
            return result
        
        try:
            content = filepath.read_text(encoding='utf-8')
            
            # Check for loading messages (indicates incomplete data)
            loading_indicators = ['載入中', '資訊載入中', '分散表載入中']
            result['has_loading_messages'] = any(indicator in content for indicator in loading_indicators)
            
            # Check for actual data presence
            result['has_prices'] = '開盤' in content and '收盤' in content and '| 200' in content
            result['has_daily_data'] = '| 2025/' in content and '成交量' in content
            result['has_ownership'] = '持股比例' in content and '%' in content
            
            # Determine completeness
            data_checks = [result['has_prices'], result['has_daily_data'], result['has_ownership']]
            result['complete'] = sum(data_checks) >= 2  # At least 2 out of 3 data types
            
            # Recommendation logic
            if result['has_loading_messages'] or not result['complete']:
                result['recommendation'] = 'retry'
            else:
                # Check file age
                mod_time = datetime.fromtimestamp(filepath.stat().st_mtime)
                age_hours = (datetime.now() - mod_time).total_seconds() / 3600
                
                if age_hours > 24:  # File older than 24 hours
                    result['recommendation'] = 'refresh'
                else:
                    result['recommendation'] = 'skip'
            
        except Exception as e:
            self.safe_log("error", f"Error validating {filename}: {e}")
            result['recommendation'] = 'process'
        
        return result

    def run_single_with_retry(self, stock_id: int, max_retries: int = None) -> bool:
        """Run single stock with enhanced retry logic."""
        if max_retries is None:
            max_retries = self.max_retries
        
        stock_df = self.load_stock_data()
        stock_row = stock_df[stock_df['代號'] == stock_id]
        
        if stock_row.empty:
            self.safe_log("error", f"Stock {stock_id} not found in CSV")
            return False
        
        stock_name = stock_row.iloc[0]['名稱']
        
        for attempt in range(max_retries):
            try:
                # Calculate delay before attempt
                if attempt > 0:
                    delay = self.retry_delay_base * (2 ** (attempt - 1)) + random.uniform(1, 5)
                    safe_print(f"[RETRY] Waiting {delay:.1f}s before attempt {attempt + 1} for {stock_id}")
                    time.sleep(delay)
                
                # Determine if we should use Selenium for this attempt
                use_selenium = attempt > 0  # Use Selenium for retries
                selenium_flag = "--selenium" if use_selenium else ""
                
                cmd = [sys.executable, str(self.poorstock_py), str(stock_id)]
                if selenium_flag:
                    cmd.append(selenium_flag)
                
                self.safe_log("info", f"Running attempt {attempt + 1}/{max_retries}: {' '.join(cmd)}")
                
                # Set environment for proper encoding
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
                    errors='replace',
                    timeout=60  # Increased timeout for Selenium
                )
                
                if result.returncode == 0:
                    # Validate the result
                    validation = self.validate_stock_file(stock_id, stock_name)
                    
                    if validation['complete'] and not validation['has_loading_messages']:
                        safe_print(f"[OK] Stock {stock_id} completed successfully on attempt {attempt + 1}")
                        self.consecutive_failures = 0  # Reset failure counter
                        return True
                    else:
                        safe_print(f"[WARNING] Stock {stock_id} incomplete on attempt {attempt + 1}")
                        if attempt == max_retries - 1:
                            safe_print(f"[ERROR] Stock {stock_id} still incomplete after all retries")
                            self.record_failed_stock(stock_id, attempt + 1)
                            return False
                else:
                    self.safe_log("error", f"Stock {stock_id} failed attempt {attempt + 1} (exit code: {result.returncode})")
                    if result.stdout:
                        self.safe_log("error", f"STDOUT: {result.stdout.strip()}")
                    if result.stderr:
                        self.safe_log("error", f"STDERR: {result.stderr.strip()}")
                    
                    if attempt == max_retries - 1:
                        self.record_failed_stock(stock_id, attempt + 1)
                        self.consecutive_failures += 1
                        return False
                        
            except subprocess.TimeoutExpired:
                self.safe_log("error", f"Timeout processing stock {stock_id} attempt {attempt + 1}")
                if attempt == max_retries - 1:
                    self.record_failed_stock(stock_id, attempt + 1)
                    self.consecutive_failures += 1
                    return False
            except Exception as e:
                self.safe_log("error", f"Exception processing stock {stock_id} attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    self.record_failed_stock(stock_id, attempt + 1)
                    self.consecutive_failures += 1
                    return False
        
        return False

    def record_failed_stock(self, stock_id: int, retry_count: int):
        """Record a failed stock processing attempt."""
        try:
            stock_df = self.load_stock_data()
            stock_row = stock_df[stock_df['代號'] == stock_id]
            
            if not stock_row.empty:
                stock_name = stock_row.iloc[0]['名稱']
                filename = f"poorstock_{stock_id}_{stock_name}.md"
                
                process_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                if self.results_file.exists():
                    results_df = pd.read_csv(self.results_file)
                else:
                    results_df = pd.DataFrame(columns=['filename', 'last_update_time', 'success', 'process_time', 'retry_count'])
                
                # Ensure retry_count column exists
                if 'retry_count' not in results_df.columns:
                    results_df['retry_count'] = 0
                
                mask = results_df['filename'] == filename
                if mask.any():
                    results_df.loc[mask, 'success'] = False
                    results_df.loc[mask, 'process_time'] = process_time
                    results_df.loc[mask, 'last_update_time'] = 'FAILED'
                    results_df.loc[mask, 'retry_count'] = retry_count
                else:
                    new_record = pd.DataFrame([{
                        'filename': filename,
                        'last_update_time': 'FAILED',
                        'success': False,
                        'process_time': process_time,
                        'retry_count': retry_count
                    }])
                    results_df = pd.concat([results_df, new_record], ignore_index=True)
                
                results_df.to_csv(self.results_file, index=False)
                self.safe_log("info", f"Recorded failure for stock {stock_id} after {retry_count} attempts")
        except Exception as e:
            self.safe_log("error", f"Could not record failure for stock {stock_id}: {e}")

    def determine_processing_strategy(self, stock_df: pd.DataFrame) -> tuple:
        """Enhanced strategy determination with file validation."""
        today = datetime.now().strftime('%Y-%m-%d')
        
        priority_stocks = []      # Failed or incomplete stocks
        refresh_stocks = []       # Old but complete stocks
        failed_stocks = []        # Previously failed stocks to retry
        unprocessed_stocks = []   # Never processed stocks
        
        for _, stock_row in stock_df.iterrows():
            stock_id = stock_row['代號']
            stock_name = stock_row['名稱']
            
            validation = self.validate_stock_file(stock_id, stock_name)
            
            if validation['recommendation'] == 'process':
                unprocessed_stocks.append(stock_id)
            elif validation['recommendation'] == 'retry':
                priority_stocks.append(stock_id)
            elif validation['recommendation'] == 'refresh':
                refresh_stocks.append(stock_id)
        
        # Check results CSV for additional failed stocks
        if self.results_file.exists():
            results_df = pd.read_csv(self.results_file)
            failed_mask = (results_df['success'] == False) | (results_df['last_update_time'] == 'FAILED')
            failed_files = results_df[failed_mask]['filename'].tolist()
            
            for filename in failed_files:
                try:
                    stock_id = int(filename.split('_')[1])
                    if stock_id not in priority_stocks and stock_id not in failed_stocks:
                        failed_stocks.append(stock_id)
                except (ValueError, IndexError):
                    continue
        
        # Decision logic
        if priority_stocks or unprocessed_stocks or failed_stocks:
            all_priority = priority_stocks + unprocessed_stocks + failed_stocks
            return "PRIORITY", all_priority
        elif refresh_stocks:
            return "REFRESH", refresh_stocks[:20]  # Limit refresh batch
        else:
            return "UP_TO_DATE", []

    def run_intelligent_batch_enhanced(self):
        """Enhanced batch processing with better error handling."""
        stock_df = self.load_stock_data()
        
        strategy, stock_ids_to_process = self.determine_processing_strategy(stock_df)
        
        safe_print(f"[BRAIN] Strategy: {strategy}")
        safe_print(f"[DATA] Stocks to process: {len(stock_ids_to_process)}")
        
        if strategy == "UP_TO_DATE":
            safe_print("[OK] All stocks are up to date")
            return
        
        # Process stocks with enhanced logic
        success_count = 0
        fail_count = 0
        total = len(stock_ids_to_process)
        
        for i, stock_id in enumerate(stock_ids_to_process, 1):
            try:
                stock_row = stock_df[stock_df['代號'] == stock_id].iloc[0]
                stock_name = stock_row['名稱']
                
                safe_print(f"\n[{i}/{total}] Processing {stock_id} ({stock_name})")
                
                # Calculate and apply dynamic delay
                delay = self.calculate_dynamic_delay()
                if i > 1:  # No delay for first request
                    safe_print(f"[WAIT] Applying rate limit: {delay:.1f}s")
                    time.sleep(delay)
                
                # Process with retry
                if self.run_single_with_retry(stock_id):
                    success_count += 1
                    safe_print(f"[SUCCESS] {stock_id} completed")
                else:
                    fail_count += 1
                    safe_print(f"[FAILED] {stock_id} failed after all retries")
                
                # Progress update every 10 stocks
                if i % 10 == 0:
                    progress = (i / total) * 100
                    safe_print(f"[PROGRESS] {progress:.1f}% complete - Success: {success_count}, Failed: {fail_count}")
                
            except KeyboardInterrupt:
                safe_print("[INTERRUPT] Processing interrupted by user")
                break
            except Exception as e:
                safe_print(f"[ERROR] Unexpected error processing stock {stock_id}: {e}")
                fail_count += 1
        
        safe_print(f"\n[COMPLETE] Batch processing finished!")
        safe_print(f"[STATS] Success: {success_count}, Failed: {fail_count}, Total: {total}")
        
        if fail_count > 0:
            safe_print(f"[SUGGEST] Failed stocks can be retried with --retry-failed flag")

    def retry_failed_stocks(self):
        """Retry only previously failed stocks with Selenium."""
        if not self.results_file.exists():
            safe_print("[INFO] No results file found - nothing to retry")
            return
        
        results_df = pd.read_csv(self.results_file)
        failed_mask = (results_df['success'] == False) | (results_df['last_update_time'] == 'FAILED')
        failed_files = results_df[failed_mask]['filename'].tolist()
        
        failed_stock_ids = []
        for filename in failed_files:
            try:
                stock_id = int(filename.split('_')[1])
                failed_stock_ids.append(stock_id)
            except (ValueError, IndexError):
                continue
        
        if not failed_stock_ids:
            safe_print("[OK] No failed stocks found to retry")
            return
        
        safe_print(f"[RETRY] Found {len(failed_stock_ids)} failed stocks to retry")
        
        success_count = 0
        for i, stock_id in enumerate(failed_stock_ids, 1):
            safe_print(f"\n[RETRY {i}/{len(failed_stock_ids)}] Retrying stock {stock_id} with Selenium")
            
            if i > 1:
                delay = 15 + random.uniform(5, 10)  # Longer delays for retries
                safe_print(f"[WAIT] Retry delay: {delay:.1f}s")
                time.sleep(delay)
            
            if self.run_single_with_retry(stock_id, max_retries=2):
                success_count += 1
        
        safe_print(f"[COMPLETE] Retry session finished: {success_count}/{len(failed_stock_ids)} recovered")

    def get_enhanced_status_report(self) -> dict:
        """Generate enhanced status report."""
        try:
            stock_df = self.load_stock_data()
            
            complete_count = 0
            incomplete_count = 0
            failed_count = 0
            unprocessed_count = 0
            
            for _, stock_row in stock_df.iterrows():
                stock_id = stock_row['代號']
                stock_name = stock_row['名稱']
                
                validation = self.validate_stock_file(stock_id, stock_name)
                
                if not validation['exists']:
                    unprocessed_count += 1
                elif validation['complete'] and not validation['has_loading_messages']:
                    complete_count += 1
                elif validation['has_loading_messages']:
                    incomplete_count += 1
                else:
                    failed_count += 1
            
            md_files = list(self.poorstock_dir.glob("poorstock_*.md"))
            
            return {
                'total_stocks': len(stock_df),
                'complete': complete_count,
                'incomplete': incomplete_count,
                'failed': failed_count,
                'unprocessed': unprocessed_count,
                'md_files_found': len(md_files),
                'consecutive_failures': self.consecutive_failures,
                'last_check': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            return {'error': str(e)}

def main():
    parser = argparse.ArgumentParser(description='Enhanced GetAll - Smart Processing with Retry Logic')
    parser.add_argument('--stock-id', type=int, help='Process specific stock ID only')
    parser.add_argument('--status', action='store_true', help='Show enhanced status report')
    parser.add_argument('--retry-failed', action='store_true', help='Retry only previously failed stocks')
    parser.add_argument('--all', action='store_true', help='Process all stocks')
    parser.add_argument('--base-dir', default='.', help='Base directory for files')
    
    args = parser.parse_args()
    
    runner = EnhancedBatchRunner(args.base_dir)
    
    if args.status:
        report = runner.get_enhanced_status_report()
        safe_print("\n=== Enhanced PoorStock Status Report ===")
        for key, value in report.items():
            safe_print(f"{key.replace('_', ' ').title()}: {value}")
    elif args.stock_id:
        safe_print(f"[TARGET] Processing single stock: {args.stock_id}")
        if runner.run_single_with_retry(args.stock_id):
            safe_print(f"[SUCCESS] Stock {args.stock_id} processed successfully")
        else:
            safe_print(f"[FAILED] Stock {args.stock_id} processing failed")
            sys.exit(1)
    elif args.retry_failed:
        safe_print("[RETRY] Retrying failed stocks with enhanced methods")
        runner.retry_failed_stocks()
    elif args.all:
        safe_print("[START] Processing ALL stocks")
        runner.run_intelligent_batch_enhanced()
    else:
        safe_print("[BRAIN] Using enhanced intelligent processing")
        runner.run_intelligent_batch_enhanced()

if __name__ == "__main__":
    main()