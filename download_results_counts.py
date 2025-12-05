#!/usr/bin/env python3
"""
Download Results Count Analyzer for PoorStock Data Scraper

Analyzes download_results.csv in the poorstock folder and generates
comprehensive status reports with download statistics and timing information.

Usage:
    python download_results_counts.py [options]

Options:
    --output FILE     Save output to specific file (default: stdout)
    --format FORMAT   Output format: table|json (default: table)
    --detailed        Include additional metrics and timestamps
    --update-readme   Update README.md status section
    --base-dir DIR    Base directory (default: .)
    --help           Show this help message
"""

import json
import argparse
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Optional
from pathlib import Path

# ---- Timezone (ONLY for README update time) ----
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    TAIPEI_TZ = ZoneInfo("Asia/Taipei")
except Exception:
    try:
        import pytz  # Fallback
        TAIPEI_TZ = pytz.timezone("Asia/Taipei")
    except Exception:
        TAIPEI_TZ = None
        print("Warning: No timezone database available. README update time will be local time without TZ label.")

def get_taipei_time() -> datetime:
    """Return timezone-aware datetime in Asia/Taipei for README stamp; fallback to local naive."""
    if TAIPEI_TZ:
        return datetime.now(TAIPEI_TZ)
    return datetime.now()

def get_local_now_naive() -> datetime:
    """Naive 'now' for all relative-time calculations (Last Updated / Duration)."""
    return datetime.now()

class PoorStockStatsAnalyzer:
    """Analyzes PoorStock download results and stock processing status (naive times for diffs)."""
    
    def __init__(self, base_dir: str = "."):
        self.base_dir = Path(base_dir)
        self.current_time = get_local_now_naive()  # naive now for diffs
        self.poorstock_dir = self.base_dir / "poorstock"
        self.results_file = self.poorstock_dir / "download_results.csv"
        self.stock_csv = self.base_dir / "StockID_TWSE_TPEX.csv"
    
    # ---------- Parsing / Formatting ----------
    def safe_parse_date_naive(self, date_string: str) -> Optional[datetime]:
        """Parse date string as NAIVE datetime (no timezone handling)."""
        if not date_string or str(date_string).strip().upper() in ['NOT_PROCESSED', 'NEVER', '', 'FAILED', 'NAN', 'NONE']:
            return None
        s = str(date_string).strip()
        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d %H:%M:%S', '%Y/%m/%d']:
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        # ISO-ish fallback
        try:
            core = s.replace("T", " ")[:19]  # 'YYYY-MM-DD HH:MM:SS'
            return datetime.strptime(core, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
    
    def format_time_ago(self, time_diff: timedelta) -> str:
        """Convert timedelta to human-readable 'ago' (naive)."""
        if time_diff.total_seconds() < 0:
            return "Future"
        days = time_diff.days
        hours = time_diff.seconds // 3600
        minutes = (time_diff.seconds % 3600) // 60
        if days > 0:
            return f"{days} days {hours} hours ago" if hours > 0 else f"{days} days ago"
        if hours > 0:
            return f"{hours} hours {minutes} minutes ago" if minutes > 0 else f"{hours} hours ago"
        if minutes > 0:
            return f"{minutes} minutes ago"
        return "Just now"
    
    def format_duration(self, time_diff: timedelta) -> str:
        """Convert timedelta to duration (naive)."""
        if time_diff.total_seconds() <= 0:
            return "N/A"
        days = time_diff.days
        hours = time_diff.seconds // 3600
        minutes = (time_diff.seconds % 3600) // 60
        if days > 0:
            return f"{days} days {hours} hours" if hours > 0 else f"{days} days"
        if hours > 0:
            return f"{hours} hours {minutes} minutes" if minutes > 0 else f"{hours} hours"
        if minutes > 0:
            return f"{minutes} minutes"
        return "< 1 minute"
    
    # ---------- IO ----------
    def load_stock_data(self) -> Optional[pd.DataFrame]:
        if not self.stock_csv.exists():
            return None
        try:
            return pd.read_csv(self.stock_csv)
        except Exception as e:
            print(f"Warning: Could not load stock CSV: {e}")
            return None
    
    # ---------- Core analyses ----------
    def analyze_download_results(self) -> Dict:
        default_stats = {
            'total_stocks': 0,
            'successful': 0,
            'failed': 0,
            'unprocessed': 0,
            'md_files_found': 0,
            'success_rate': 0.0,
            'last_updated': 'Never',
            'processing_duration': 'N/A',
            'error': None
        }
        
        if not self.poorstock_dir.exists():
            default_stats['error'] = 'Poorstock directory not found'
            return default_stats
        
        stock_df = self.load_stock_data()
        total_stocks = len(stock_df) if stock_df is not None else 0
        default_stats['total_stocks'] = total_stocks
        
        md_files = list(self.poorstock_dir.glob("poorstock_*.md"))
        default_stats['md_files_found'] = len(md_files)
        
        if not self.results_file.exists():
            default_stats['unprocessed'] = total_stocks
            return default_stats
        
        try:
            df = pd.read_csv(self.results_file)
            if df.empty:
                default_stats['unprocessed'] = total_stocks
                return default_stats
            
            # Normalize boolean
            if df['success'].dtype == 'object':
                df['success'] = df['success'].astype(str).str.lower().str.strip().isin(['true', '1', 'yes'])
            
            successful = int(df['success'].sum())
            failed = int((df['success'] == False).sum())
            processed = len(df)
            unprocessed = max(0, total_stocks - processed)
            
            # Calculate Freshness (Successful AND < 24 hours old)
            freshly_updated = 0
            one_day_ago = self.current_time - timedelta(days=1)
            
            for _, row in df.iterrows():
                if row['success']:
                    t = self.safe_parse_date_naive(row.get('process_time'))
                    if t and t >= one_day_ago:
                        freshly_updated += 1

            stats = {
                'total_stocks': total_stocks,
                'successful': successful,
                'freshly_updated': freshly_updated,
                'failed': failed,
                'unprocessed': unprocessed,
                'md_files_found': len(md_files),
                'success_rate': (successful / processed * 100) if processed > 0 else 0.0,
                'error': None
            }
            
            # Time metrics (NAIVE only)
            self._calculate_time_metrics(df, stats)
            return stats
                
        except Exception as e:
            default_stats['error'] = f"Error reading results file: {str(e)}"
            default_stats['unprocessed'] = total_stocks
            # Ensure key exists even on error
            default_stats['freshly_updated'] = 0
            return default_stats
    
    def _calculate_time_metrics(self, df: pd.DataFrame, stats: Dict):
        try:
            successful_df = df[df['success'] == True].copy()
            if successful_df.empty:
                stats['last_updated'] = 'Never'
                stats['processing_duration'] = 'N/A'
                return
            
            process_times = []
            for _, row in successful_df.iterrows():
                t = self.safe_parse_date_naive(row.get('process_time'))
                if t:
                    process_times.append(t)
            
            if process_times:
                last_time = max(process_times)
                time_diff = self.current_time - last_time
                stats['last_updated'] = self.format_time_ago(time_diff)
                
                if len(process_times) > 1:
                    first_time = min(process_times)
                    duration_diff = last_time - first_time
                    stats['processing_duration'] = self.format_duration(duration_diff)
                else:
                    stats['processing_duration'] = "Single batch"
            else:
                stats['last_updated'] = 'Never'
                stats['processing_duration'] = 'N/A'
        except Exception as e:
            print(f"Warning: Time calculation error: {e}")
            stats['last_updated'] = 'Error calculating time'
            stats['processing_duration'] = 'N/A'
    
    def get_stock_breakdown(self) -> Dict:
        """Detailed breakdown; uses NAIVE dates for 'today/this week'."""
        stock_df = self.load_stock_data()
        if stock_df is None:
            return {'error': 'Could not load stock data'}
        
        breakdown = {
            'total': len(stock_df),
            'processed_today': 0,
            'processed_this_week': 0,
            'failed_stocks': [],
            'recent_successes': [],
            'unprocessed_stocks': []
        }
        
        if not self.results_file.exists():
            breakdown['unprocessed_stocks'] = stock_df['代號'].tolist()
            return breakdown
        
        try:
            results_df = pd.read_csv(self.results_file)
            if results_df['success'].dtype == 'object':
                results_df['success'] = results_df['success'].astype(str).str.lower().str.strip().isin(['true', '1', 'yes'])
            
            now_naive = self.current_time
            today_date = now_naive.date()
            week_ago_date = (now_naive - timedelta(days=7)).date()
            
            for _, stock_row in stock_df.iterrows():
                stock_id = stock_row['代號']
                stock_name = stock_row['名稱']
                expected_filename = f"poorstock_{stock_id}_{stock_name}.md"
                
                result_mask = results_df['filename'] == expected_filename
                if result_mask.any():
                    result_row = results_df[result_mask].iloc[0]
                    t = self.safe_parse_date_naive(result_row.get('process_time'))
                    if t:
                        proc_date = t.date()
                        if result_row['success']:
                            if proc_date == today_date:
                                breakdown['processed_today'] += 1
                            if proc_date >= week_ago_date:
                                breakdown['processed_this_week'] += 1
                                breakdown['recent_successes'].append({
                                    'stock_id': stock_id,
                                    'name': stock_name,
                                    'date': proc_date.isoformat()
                                })
                        else:
                            breakdown['failed_stocks'].append({
                                'stock_id': stock_id,
                                'name': stock_name,
                                'date': proc_date.isoformat()
                            })
                    else:
                        if not result_row['success']:
                            breakdown['failed_stocks'].append({
                                'stock_id': stock_id,
                                'name': stock_name,
                                'date': ""
                            })
                else:
                    breakdown['unprocessed_stocks'].append(stock_id)
            
            breakdown['failed_stocks'] = breakdown['failed_stocks'][:10]
            breakdown['recent_successes'] = breakdown['recent_successes'][:10]
            return breakdown
        except Exception as e:
            breakdown['error'] = f"Error analyzing stock breakdown: {e}"
            return breakdown
    
    def validate_consistency(self) -> Dict:
        stats = self.analyze_download_results()
        md_files_count = stats['md_files_found']
        csv_successful = stats['successful']
        return {
            'csv_vs_files_match': csv_successful == md_files_count,
            'csv_successful': csv_successful,
            'md_files_found': md_files_count,
            'discrepancy': abs(csv_successful - md_files_count)
        }
    
    # ---------- Outputs ----------
    def generate_markdown_table(self) -> str:
        stats = self.analyze_download_results()
        lines = [
            "| Metric | Value |",
            "|--------|-------|"
        ]
        lines.extend([
            f"| Total Stocks | {stats['total_stocks']:,} |",
            f"| Successful (Total) | {stats['successful']:,} ({stats['success_rate']:.1f}%) |",
            f"| Freshly Updated (<24h) | {stats['freshly_updated']:,} |",
            f"| Failed | {stats['failed']:,} |",
            f"| Unprocessed | {stats['unprocessed']:,} |",
            f"| MD Files Found | {stats['md_files_found']:,} |",
            f"| Last Updated | {stats['last_updated']} |",
            f"| Processing Duration | {stats['processing_duration']} |"
        ])
        if stats.get('error'):
            lines.append(f"| Status | Error: {stats['error']} |")
        return "\n".join(lines)
    
    def generate_detailed_report(self) -> str:
        stats = self.analyze_download_results()
        breakdown = self.get_stock_breakdown()
        validation = self.validate_consistency()
        timestamp_str = self.format_readable_timestamp_for_header()
        
        report = [
            "# PoorStock Download Status Report",
            f"*Generated: {timestamp_str}*\n",
            "## Summary"
        ]
        if stats.get('error'):
            report.extend([
                f"**Status**: Error - {stats['error']}",
                f"**Total Stocks**: {stats['total_stocks']:,}",
                f"**MD Files Found**: {stats['md_files_found']:,}\n"
            ])
        else:
            report.extend([
                f"**Total Stocks**: {stats['total_stocks']:,}",
                f"**Successful (Total)**: {stats['successful']:,} ({stats['success_rate']:.1f}%)",
                f"**Freshly Updated (<24h)**: {stats['freshly_updated']:,}",
                f"**Failed**: {stats['failed']:,}",
                f"**Unprocessed**: {stats['unprocessed']:,}",
                f"**MD Files Found**: {stats['md_files_found']:,}",
                f"**Last Updated**: {stats['last_updated']}",
                f"**Processing Duration**: {stats['processing_duration']}\n"
            ])
        
        report.extend([
            "## Status Overview",
            self.generate_markdown_table()
        ])
        
        if not validation['csv_vs_files_match']:
            report.extend([
                "\n## Validation Warning",
                f"⚠️  CSV shows {validation['csv_successful']} successful entries, but found {validation['md_files_found']} MD files.",
                f"Discrepancy: {validation['discrepancy']} files"
            ])
        
        if not breakdown.get('error'):
            report.extend([
                "\n## Recent Activity",
                f"- **Processed Today**: {breakdown['processed_today']} stocks",
                f"- **Processed This Week**: {breakdown['processed_this_week']} stocks",
                f"- **Failed Stocks**: {len(breakdown['failed_stocks'])} stocks",
                f"- **Unprocessed**: {len(breakdown['unprocessed_stocks'])} stocks"
            ])
            if breakdown['failed_stocks']:
                report.extend([
                    "\n### Recent Failures",
                    *[f"- {s['stock_id']} ({s['name']}) - {s['date']}" for s in breakdown['failed_stocks'][:5]]
                ])
            if breakdown['unprocessed_stocks']:
                sample = breakdown['unprocessed_stocks'][:10]
                report.extend([
                    "\n### Sample Unprocessed Stocks",
                    f"- {', '.join(map(str, sample))}"
                ])
                if len(breakdown['unprocessed_stocks']) > 10:
                    report.append(f"- ... and {len(breakdown['unprocessed_stocks']) - 10} more")
        
        report.extend([
            "\n## Notes",
            "- **MD Files Found**: Actual markdown files in poorstock/ directory",
            "- **Last Updated**: Relative time from the most recent *successful* processing (no timezone)",
            "- **Processing Duration**: Time span from first to last successful processing (no timezone)",
            "- **Success Rate**: Percentage of successfully processed stocks"
        ])
        
        return "\n".join(report)
    
    def format_readable_timestamp_for_header(self) -> str:
        """Human-readable timestamp (no timezone) for report header only."""
        return self.current_time.strftime('%Y-%m-%d %H:%M:%S')
    
    def export_json(self) -> str:
        stats = self.analyze_download_results()
        breakdown = self.get_stock_breakdown()
        validation = self.validate_consistency()
        export_data = {
            'generated_at': self.current_time.isoformat(),
            'timezone_note': 'Relative times are naive; README update time uses Asia/Taipei.',
            'summary': stats,
            'breakdown': breakdown if not breakdown.get('error') else None,
            'validation': validation
        }
        return json.dumps(export_data, indent=2, ensure_ascii=False)


# ---------- CLI ----------
def main():
    parser = argparse.ArgumentParser(
        description='Analyze PoorStock download results and generate status reports',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--output', '-o', help='Save output to file (default: stdout)')
    parser.add_argument('--format', '-f', choices=['table', 'json'], default='table', help='Output format (default: table)')
    parser.add_argument('--detailed', '-d', action='store_true', help='Generate detailed report with breakdown')
    parser.add_argument('--update-readme', action='store_true', help='Update README.md status section')
    parser.add_argument('--base-dir', default='.', help='Base directory (default: current directory)')
    args = parser.parse_args()
    
    analyzer = PoorStockStatsAnalyzer(args.base_dir)
    print("Analyzing PoorStock download results...")
    
    if args.format == 'json':
        output = analyzer.export_json()
    elif args.detailed:
        output = analyzer.generate_detailed_report()
    else:
        output = analyzer.generate_markdown_table()
    
    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(output)
            print(f"Results saved to: {args.output}")
        except Exception as e:
            print(f"Error saving to file: {e}")
            print("\nOutput:")
            print(output)
    else:
        print(output)
    
    if args.update_readme:
        try:
            update_readme_status(analyzer.generate_markdown_table(), args.base_dir)
            print("README.md status section updated successfully")
        except Exception as e:
            print(f"Error updating README.md: {e}")


def update_readme_status(table_content: str, base_dir: str = "."):
    """Update the status table in README.md. Only here we show Taipei timezone."""
    readme_path = Path(base_dir) / 'README.md'
    if not readme_path.exists():
        raise FileNotFoundError("README.md not found in current directory")
    
    with open(readme_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    status_start = content.find('## Status')
    current_time_str = get_taipei_time().strftime('%Y-%m-%d %H:%M:%S %Z')  # e.g., 2025-09-03 22:59:41 CST
    
    if status_start == -1:
        new_content = content.rstrip() + f"\n\n## Status\nUpdate time: {current_time_str}\n\n{table_content}\n"
    else:
        next_section = content.find('\n## ', status_start + 1)
        if next_section == -1:
            next_section = len(content)
        new_content = (
            content[:status_start] +
            "## Status\n" +
            f"Update time: {current_time_str}\n\n" +
            table_content +
            "\n\n" +
            content[next_section:]
        )
    
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.write(new_content)


if __name__ == '__main__':
    main()
