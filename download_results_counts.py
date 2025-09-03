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

def get_taipei_time():
    """Get current time in Taiwan timezone."""
    if TAIPEI_TZ:
        return datetime.now(TAIPEI_TZ)
    else:
        return datetime.now()

class PoorStockStatsAnalyzer:
    """Analyzes PoorStock download results and stock processing status."""
    
    def __init__(self, base_dir: str = "."):
        self.base_dir = Path(base_dir)
        self.current_time = get_taipei_time()
        self.poorstock_dir = self.base_dir / "poorstock"
        self.results_file = self.poorstock_dir / "download_results.csv"
        self.stock_csv = self.base_dir / "StockID_TWSE_TPEX.csv"
    
    def safe_parse_date(self, date_string: str) -> Optional[datetime]:
        """Parse date string with fallback for special values."""
        if not date_string or date_string.strip() in ['NOT_PROCESSED', 'NEVER', '']:
            return None
        
        try:
            # Handle various date formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d %H:%M:%S']:
                try:
                    dt = datetime.strptime(date_string.strip(), fmt)
                    # Assume parsed datetime is in Taiwan timezone
                    if TAIPEI_TZ:
                        dt = dt.replace(tzinfo=TAIPEI_TZ)
                    return dt
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
            default_stats['error'] = 'Results CSV not found'
            return default_stats
        
        try:
            with open(self.results_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                # Required columns
                required_cols = ['filename', 'last_update_time', 'success', 'process_time']
                if not all(col in reader.fieldnames for col in required_cols):
                    default_stats['error'] = 'Invalid CSV format'
                    default_stats['unprocessed'] = total_stocks
                    return default_stats
                
                rows = list(reader)
                
                if not rows:
                    default_stats['unprocessed'] = total_stocks
                    return default_stats
                
                # Calculate basic statistics
                successful = sum(1 for row in rows if row['success'].lower() == 'true')
                failed = sum(1 for row in rows if row['success'].lower() == 'false')
                processed = len(rows)
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
                
                # Calculate time-based metrics
                process_times = []
                for row in rows:
                    time_parsed = self.safe_parse_date(row['process_time'])
                    if time_parsed:
                        process_times.append(time_parsed)
                
                if process_times:
                    # Last updated (most recent processing time)
                    last_time = max(process_times)
                    
                    # Ensure both times are timezone-aware for proper comparison
                    if TAIPEI_TZ and last_time.tzinfo is None:
                        last_time = last_time.replace(tzinfo=TAIPEI_TZ)
                    
                    # Calculate time difference
                    if self.current_time.tzinfo and last_time.tzinfo:
                        time_diff = self.current_time - last_time
                    else:
                        # Fallback for timezone-naive comparison
                        current_naive = self.current_time.replace(tzinfo=None) if self.current_time.tzinfo else self.current_time
                        last_naive = last_time.replace(tzinfo=None) if last_time.tzinfo else last_time
                        time_diff = current_naive - last_naive
                    
                    stats['last_updated'] = self.format_time_ago(time_diff)
                    
                    # Processing duration (time span from first to last processing)
                    if len(process_times) > 1:
                        first_time = min(process_times)
                        duration_diff = last_time - first_time
                        stats['processing_duration'] = self.format_duration(duration_diff)
                    else:
                        stats['processing_duration'] = "Single batch"
                else:
                    stats['last_updated'] = 'Never'
                    stats['processing_duration'] = 'N/A'
                
                return stats
                
        except Exception as e:
            default_stats['error'] = f"Error reading results file: {str(e)}"
            default_stats['unprocessed'] = total_stocks
            return default_stats
    
    def get_stock_breakdown(self) -> Dict:
        """Get detailed breakdown by stock status."""
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
            today = datetime.now().strftime('%Y-%m-%d')
            week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            
            # Analyze each stock
            for _, stock_row in stock_df.iterrows():
                stock_id = stock_row['代號']
                stock_name = stock_row['名稱']
                expected_filename = f"poorstock_{stock_id}_{stock_name}.md"
                
                result_mask = results_df['filename'] == expected_filename
                if result_mask.any():
                    result_row = results_df[result_mask].iloc[0]
                    process_date = result_row['process_time'][:10] if result_row['process_time'] else ''
                    
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
            "- **Last Updated**: Time since most recent processing attempt",
            "- **Processing Duration**: Time span from first to last processing in batch",
            "- **Success Rate**: Percentage of successfully processed stocks"
        ])
        
        return "\n".join(report)
    
    def format_taipei_timestamp(self) -> str:
        """Format current time with Taiwan timezone information."""
        if TAIPEI_TZ:
            return self.current_time.strftime('%Y-%m-%d %H:%M:%S %Z')
        else:
            return self.current_time.strftime('%Y-%m-%d %H:%M:%S (Taiwan)')
    
    def export_json(self) -> str:
        """Export results as JSON."""
        stats = self.analyze_download_results()
        breakdown = self.get_stock_breakdown()
        
        export_data = {
            'generated_at': self.current_time.isoformat(),
            'timezone': 'Asia/Taipei',
            'summary': stats,
            'breakdown': breakdown if not breakdown.get('error') else None
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
        taipei_time = get_taipei_time()
        if TAIPEI_TZ:
            current_time_str = taipei_time.strftime('%Y-%m-%d %H:%M:%S %Z')
        else:
            current_time_str = taipei_time.strftime('%Y-%m-%d %H:%M:%S (Taiwan)')
        
        new_content = content.rstrip() + f"\n\n## Status\nUpdate time: {current_time_str}\n\n{table_content}\n"
    else:
        # Find the end of the status section
        next_section = content.find('\n## ', status_start + 1)
        if next_section == -1:
            next_section = len(content)
        
        # Generate current timestamp in Taiwan timezone
        taipei_time = get_taipei_time()
        if TAIPEI_TZ:
            current_time_str = taipei_time.strftime('%Y-%m-%d %H:%M:%S %Z')
        else:
            current_time_str = taipei_time.strftime('%Y-%m-%d %H:%M:%S (Taiwan)')
        
        # Replace the status section with Taiwan timezone timestamp
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