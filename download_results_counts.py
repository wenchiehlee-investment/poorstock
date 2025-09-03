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
    --help           Show this help message
"""

import os
import csv
import json
import argparse
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path

# Taiwan timezone support
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    TAIPEI_TZ = ZoneInfo("Asia/Taipei")
except ImportError:
    try:
        import pytz  # Fallback for older Python versions
        TAIPEI_TZ = pytz.timezone('Asia/Taipei')
    except ImportError:
        TAIPEI_TZ = None
        print("Warning: Neither zoneinfo nor pytz available. Using system timezone.")

def get_naive_taipei_time():
    """Get current time in Taiwan timezone as naive datetime for consistent comparison."""
    if TAIPEI_TZ:
        return datetime.now(TAIPEI_TZ).replace(tzinfo=None)
    else:
        # Fallback: assume local time is close enough
        return datetime.now()

def get_taipei_time() -> datetime:
    """Get current timezone-aware datetime in Asia/Taipei (fallback: local naive)."""
    if TAIPEI_TZ:
        return datetime.now(TAIPEI_TZ)
    return datetime.now()

class PoorStockStatsAnalyzer:
    """Analyzes PoorStock download results and stock processing status."""
    
    def __init__(self, base_dir: str = "."):
        self.base_dir = Path(base_dir)
        self.current_time = get_naive_taipei_time()
        self.poorstock_dir = self.base_dir / "poorstock"
        self.results_file = self.poorstock_dir / "download_results.csv"
        self.stock_csv = self.base_dir / "StockID_TWSE_TPEX.csv"
    
    def safe_parse_date(self, date_string: str) -> Optional[datetime]:
        """Parse date string with fallback for special values."""
        if not date_string or str(date_string).strip().upper() in ['NOT_PROCESSED', 'NEVER', '', 'FAILED', 'NAN', 'NONE']:
            return None
        
        try:
            # Handle various date formats - return as naive datetime
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d %H:%M:%S']:
                try:
                    return datetime.strptime(str(date_string).strip(), fmt)
                except ValueError:
                    continue
            return None
        except Exception:
            return None
    
    def format_time_ago(self, time_diff: timedelta) -> str:
        """Convert timedelta to human-readable 'ago' format."""
        if time_diff.total_seconds() < 0:
            return "Future"
        
        days = time_diff.days
        hours = time_diff.seconds // 3600
        minutes = (time_diff.seconds % 3600) // 60
        
        if days > 0:
            if hours > 0:
                return f"{days} days {hours} hours ago"
            else:
                return f"{days} days ago"
        elif hours > 0:
            if minutes > 0:
                return f"{hours} hours {minutes} minutes ago"
            else:
                return f"{hours} hours ago"
        elif minutes > 0:
            return f"{minutes} minutes ago"
        else:
            return "Just now"
    
    def format_duration(self, time_diff: timedelta) -> str:
        """Convert timedelta to duration format."""
        if time_diff.total_seconds() <= 0:
            return "N/A"
        
        days = time_diff.days
        hours = time_diff.seconds // 3600
        minutes = (time_diff.seconds % 3600) // 60
        
        if days > 0:
            if hours > 0:
                return f"{days} days {hours} hours"
            else:
                return f"{days} days"
        elif hours > 0:
            if minutes > 0:
                return f"{hours} hours {minutes} minutes"
            else:
                return f"{hours} hours"
        elif minutes > 0:
            return f"{minutes} minutes"
        else:
            return "< 1 minute"
    
    def load_stock_data(self) -> Optional[pd.DataFrame]:
        """Load stock list from CSV."""
        if not self.stock_csv.exists():
            return None
        try:
            return pd.read_csv(self.stock_csv)
        except Exception as e:
            print(f"Warning: Could not load stock CSV: {e}")
            return None
    
    def analyze_download_results(self) -> Dict:
        """Analyze the poorstock download results CSV file."""
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
        
        # Check if poorstock directory exists
        if not self.poorstock_dir.exists():
            default_stats['error'] = 'Poorstock directory not found'
            return default_stats
        
        # Load stock data to get total count
        stock_df = self.load_stock_data()
        total_stocks = len(stock_df) if stock_df is not None else 0
        default_stats['total_stocks'] = total_stocks
        
        # Count actual markdown files
        md_files = list(self.poorstock_dir.glob("poorstock_*.md"))
        default_stats['md_files_found'] = len(md_files)
        
        # Analyze results CSV if it exists
        if not self.results_file.exists():
            default_stats['unprocessed'] = total_stocks
            return default_stats
        
        try:
            # Use pandas for cleaner CSV handling
            df = pd.read_csv(self.results_file)
            
            if df.empty:
                default_stats['unprocessed'] = total_stocks
                return default_stats
            
            # Handle different boolean representations
            # Convert string 'True'/'False' to actual booleans if needed
            if df['success'].dtype == 'object':
                df['success'] = df['success'].astype(str).str.lower().str.strip().isin(['true', '1', 'yes'])
            
            # Calculate basic statistics
            successful = int(df['success'].sum())
            failed = int((df['success'] == False).sum())
            processed = len(df)
            unprocessed = max(0, total_stocks - processed)
            
            stats = {
                'total_stocks': total_stocks,
                'successful': successful,
                'failed': failed,
                'unprocessed': unprocessed,
                'md_files_found': len(md_files),
                'success_rate': (successful / processed * 100) if processed > 0 else 0.0,
                'error': None
            }
            
            # Calculate time-based metrics - ONLY from successful entries
            self._calculate_time_metrics(df, stats)
            
            return stats
                
        except Exception as e:
            default_stats['error'] = f"Error reading results file: {str(e)}"
            default_stats['unprocessed'] = total_stocks
            return default_stats
    
    def _calculate_time_metrics(self, df: pd.DataFrame, stats: Dict):
        """Calculate time-based metrics from successful entries only."""
        try:
            # Filter to successful entries only (as per instructions)
            successful_df = df[df['success'] == True].copy()
            
            if successful_df.empty:
                stats['last_updated'] = 'Never'
                stats['processing_duration'] = 'N/A'
                return
            
            # Parse process times from successful entries
            process_times = []
            for _, row in successful_df.iterrows():
                time_parsed = self.safe_parse_date(row['process_time'])
                if time_parsed:
                    process_times.append(time_parsed)
            
            if process_times:
                # Last updated (most recent successful processing time)
                last_time = max(process_times)
                time_diff = self.current_time - last_time
                stats['last_updated'] = self.format_time_ago(time_diff)
                
                # Processing duration (time span from first to last successful processing)
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
        """Get detailed breakdown by stock status (as per instructions)."""
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
            
            # Handle boolean conversion
            if results_df['success'].dtype == 'object':
                results_df['success'] = results_df['success'].astype(str).str.lower().str.strip().isin(['true', '1', 'yes'])
            
            # Use Taipei timezone for date boundaries
            now_tpe = get_taipei_time()
            today = now_tpe.strftime('%Y-%m-%d')
            week_ago = (now_tpe - timedelta(days=7)).strftime('%Y-%m-%d')
            
            # Analyze each stock
            for _, stock_row in stock_df.iterrows():
                stock_id = stock_row['代號']
                stock_name = stock_row['名稱']
                expected_filename = f"poorstock_{stock_id}_{stock_name}.md"
                
                result_mask = results_df['filename'] == expected_filename
                if result_mask.any():
                    result_row = results_df[result_mask].iloc[0]
                    process_date = str(result_row['process_time'])[:10] if result_row.get('process_time') is not None else ''
                    
                    if result_row['success']:
                        if process_date == today:
                            breakdown['processed_today'] += 1
                        elif process_date >= week_ago:
                            breakdown['processed_this_week'] += 1
                        
                        if process_date >= week_ago:
                            breakdown['recent_successes'].append({
                                'stock_id': stock_id,
                                'name': stock_name,
                                'date': process_date
                            })
                    else:
                        breakdown['failed_stocks'].append({
                            'stock_id': stock_id,
                            'name': stock_name,
                            'date': process_date
                        })
                else:
                    breakdown['unprocessed_stocks'].append(stock_id)
            
            # Limit lists for readability
            breakdown['failed_stocks'] = breakdown['failed_stocks'][:10]
            breakdown['recent_successes'] = breakdown['recent_successes'][:10]
            
            return breakdown
            
        except Exception as e:
            breakdown['error'] = f"Error analyzing stock breakdown: {e}"
            return breakdown
    
    def validate_consistency(self) -> Dict:
        """Cross-reference validation between CSV and actual files."""
        stats = self.analyze_download_results()
        md_files_count = stats['md_files_found']
        csv_successful = stats['successful']
        
        validation = {
            'csv_vs_files_match': csv_successful == md_files_count,
            'csv_successful': csv_successful,
            'md_files_found': md_files_count,
            'discrepancy': abs(csv_successful - md_files_count)
        }
        
        return validation
    
    def generate_markdown_table(self) -> str:
        """Generate simple markdown status table."""
        stats = self.analyze_download_results()
        
        lines = [
            "| Metric | Value |",
            "|--------|-------|"
        ]
        
        lines.extend([
            f"| Total Stocks | {stats['total_stocks']:,} |",
            f"| Successful | {stats['successful']:,} ({stats['success_rate']:.1f}%) |",
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
        """Generate detailed analysis report."""
        stats = self.analyze_download_results()
        breakdown = self.get_stock_breakdown()
        validation = self.validate_consistency()
        
        # Format timestamp with timezone info
        timestamp_str = self.format_taipei_timestamp()
        
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
                f"**Successful**: {stats['successful']:,} ({stats['success_rate']:.1f}%)",
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
        
        # Add validation info
        if not validation['csv_vs_files_match']:
            report.extend([
                "\n## Validation Warning",
                f"⚠️  CSV shows {validation['csv_successful']} successful entries, but found {validation['md_files_found']} MD files.",
                f"Discrepancy: {validation['discrepancy']} files"
            ])
        
        # Add breakdown if available
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
                    *[f"- {stock['stock_id']} ({stock['name']}) - {stock['date']}" 
                      for stock in breakdown['failed_stocks'][:5]]
                ])
            
            if breakdown['unprocessed_stocks']:
                unprocessed_sample = breakdown['unprocessed_stocks'][:10]
                report.extend([
                    "\n### Sample Unprocessed Stocks",
                    f"- {', '.join(map(str, unprocessed_sample))}"
                ])
                if len(breakdown['unprocessed_stocks']) > 10:
                    report.append(f"- ... and {len(breakdown['unprocessed_stocks']) - 10} more")
        
        report.extend([
            "\n## Notes",
            "- **MD Files Found**: Actual markdown files in poorstock/ directory",
            "- **Last Updated**: Time since most recent *successful* processing attempt",
            "- **Processing Duration**: Time span from first to last successful processing",
            "- **Success Rate**: Percentage of successfully processed stocks"
        ])
        
        return "\n".join(report)
    
    def format_taipei_timestamp(self) -> str:
        """Format current time with Taiwan timezone information."""
        if TAIPEI_TZ:
            taipei_time = get_taipei_time()
            return taipei_time.strftime('%Y-%m-%d %H:%M:%S %Z')
        else:
            return self.current_time.strftime('%Y-%m-%d %H:%M:%S (Taiwan)')
    
    def export_json(self) -> str:
        """Export results as JSON."""
        stats = self.analyze_download_results()
        breakdown = self.get_stock_breakdown()
        validation = self.validate_consistency()
        
        export_data = {
            'generated_at': self.current_time.isoformat(),
            'timezone': 'Asia/Taipei',
            'summary': stats,
            'breakdown': breakdown if not breakdown.get('error') else None,
            'validation': validation
        }
        
        return json.dumps(export_data, indent=2, ensure_ascii=False)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Analyze PoorStock download results and generate status reports',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument('--output', '-o', 
                       help='Save output to file (default: stdout)')
    parser.add_argument('--format', '-f', 
                       choices=['table', 'json'], 
                       default='table',
                       help='Output format (default: table)')
    parser.add_argument('--detailed', '-d',
                       action='store_true',
                       help='Generate detailed report with breakdown')
    parser.add_argument('--update-readme',
                       action='store_true', 
                       help='Update README.md status section')
    parser.add_argument('--base-dir',
                       default='.',
                       help='Base directory (default: current directory)')
    
    args = parser.parse_args()
    
    # Initialize analyzer
    analyzer = PoorStockStatsAnalyzer(args.base_dir)
    
    print("Analyzing PoorStock download results...")
    
    # Generate output based on format
    if args.format == 'json':
        output = analyzer.export_json()
    elif args.detailed:
        output = analyzer.generate_detailed_report()
    else:
        output = analyzer.generate_markdown_table()
    
    # Handle output destination
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
    
    # Update README.md if requested
    if args.update_readme:
        try:
            update_readme_status(analyzer.generate_markdown_table(), args.base_dir)
            print("README.md status section updated successfully")
        except Exception as e:
            print(f"Error updating README.md: {e}")


def update_readme_status(table_content: str, base_dir: str = "."):
    """Update the status table in README.md with Taiwan timezone."""
    readme_path = Path(base_dir) / 'README.md'
    
    if not readme_path.exists():
        raise FileNotFoundError("README.md not found in current directory")
    
    with open(readme_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find or create the status section
    status_start = content.find('## Status')
    if status_start == -1:
        # Add status section at the end
        current_time_str = get_taipei_time().strftime('%Y-%m-%d %H:%M:%S %Z')
        
        new_content = content.rstrip() + f"\n\n## Status\nUpdate time: {current_time_str}\n\n{table_content}\n"
    else:
        # Find the end of the status section
        next_section = content.find('\n## ', status_start + 1)
        if next_section == -1:
            next_section = len(content)
        
        # Generate current timestamp in Taiwan timezone
        current_time_str = get_taipei_time().strftime('%Y-%m-%d %H:%M:%S %Z')
        
        # Replace the status section
        new_content = (
            content[:status_start] +
            "## Status\n" +
            f"Update time: {current_time_str}\n\n" +
            table_content +
            "\n\n" +
            content[next_section:]
        )
    
    # Write back to file
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.write(new_content)


if __name__ == '__main__':
    main()
