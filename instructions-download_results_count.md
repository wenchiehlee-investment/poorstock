# Download Results Count Analyzer - PoorStock Project Design Document

## Project Overview
Create a Python script `download_results_counts.py` that analyzes PoorStock download status by scanning the `poorstock/download_results.csv` file and generating comprehensive status reports for stock data scraping progress.

## Purpose
Provide automated monitoring and reporting for the PoorStock data scraper system, enabling quick assessment of download progress, success rates, and timing across all Taiwan stock symbols.

## Core Requirements

### Input Analysis
- **Single Directory**: Analyze `poorstock/download_results.csv` file
- **CSV Format**: Parse standard tracking format with columns: `filename,last_update_time,success,process_time`
- **Stock Integration**: Cross-reference with `StockID_TWSE_TPEX.csv` for comprehensive analysis
- **File Verification**: Count actual markdown files in `poorstock/` directory

### Output Generation
- **Status Table**: Generate status table for README.md integration
- **Real-time Metrics**: Calculate current statistics including time differences
- **Stock Breakdown**: Provide detailed analysis by stock processing status

## Project Structure

```
poorstock/
├── README.md                    # Main documentation
├── StockID_TWSE_TPEX.csv       # Master stock list (代號,名稱)
├── poorstock.py                 # Single stock scraper
├── GetAll.py                   # Batch runner
├── download_results_counts.py  # Status analyzer (this script)
└── poorstock/                  # Output directory
    ├── download_results.csv    # Processing results
    └── poorstock_*.md          # Stock analysis files
```

## Technical Specifications

### File Analysis Strategy
```python
# Single file analysis
RESULTS_FILE = "poorstock/download_results.csv"
STOCK_CSV = "StockID_TWSE_TPEX.csv"
POORSTOCK_DIR = "poorstock/"

# Expected filename pattern
# poorstock_{stock_id}_{stock_name}.md
```

### CSV Parsing Logic
```python
# Expected CSV structure:
# filename,last_update_time,success,process_time,retry_count
# poorstock_2412_中華電.md,2025-01-15 10:30:25,true,2025-01-15 10:30:25
# 
# Parse requirements:
# - Handle empty files (header only)
# - Handle missing files (directory exists but no CSV)
# - Handle malformed dates (use 'NEVER', 'NOT_PROCESSED')
# - Count boolean success values correctly
# - Cross-reference with stock master list
# - timezone of CSV is UTC
```

### Metric Calculations

#### 1. Total Stocks
- **Source**: Row count from `StockID_TWSE_TPEX.csv`
- **Purpose**: Establish baseline for completion percentage

#### 2. Successful Count  
- **Source**: Count rows where `success=true` in results CSV
- **Cross-check**: Verify against actual markdown files in directory

#### 3. Failed Count
- **Source**: Count rows where `success=false` in results CSV
- **Analysis**: Identify stocks that failed processing

#### 4. Unprocessed Count
- **Logic**: `Total Stocks - (Successful + Failed)`
- **Purpose**: Show stocks not yet attempted

#### 5. MD Files Found
- **Source**: Count `poorstock_*.md` files in directory
- **Validation**: Should match successful count

#### 6. Last Updated
- **Source**: Most recent `process_time` from results CSV on `success=true`
- **Format**: "X days Y hours ago" or "Never"

#### 7. Processing Duration
- **Source**: Time span from first to last `process_time`
- **Format**: "X days Y hours" or "Single batch"


### Time Handling Strategy

#### Taiwan Timezone Support
```python
try:
    from zoneinfo import ZoneInfo
    TAIPEI_TZ = ZoneInfo("Asia/Taipei")
except ImportError:
    import pytz
    TAIPEI_TZ = pytz.timezone('Asia/Taipei')
```

#### Date Format Support
- **Primary**: `2025-01-15 14:30:25` (from poorstock.py)
- **Special Values**: `NOT_PROCESSED`, `NEVER`
- **Timezone**: Assume Taiwan timezone for all timestamps

#### Display Formats
```python
def format_time_ago(time_diff):
    """Human-readable time elapsed format"""
    # "2 days 4 hours ago"
    # "3 hours 15 minutes ago" 
    # "Just now"

def format_duration(time_diff):
    """Processing duration format"""
    # "5 days 12 hours"
    # "2 hours 30 minutes"
    # "Single batch"
```

## Output Formats

### Simple Status Table (Default)
```markdown
| Metric | Value |
|--------|-------|
| Total Stocks | 1,000 |
| Successful | 850 (85.0%) |
| Failed | 50 |
| Unprocessed | 100 |
| MD Files Found | 850 |
| Last Updated | 2 hours ago |
| Processing Duration | 3 days 4 hours |
```

### Detailed Report Format
```markdown
# PoorStock Download Status Report
*Generated: 2025-01-15 14:30:25 CST*

## Summary
**Total Stocks**: 1,000
**Successful**: 850 (85.0%)
**Failed**: 50
**Unprocessed**: 100
**MD Files Found**: 850
**Last Updated**: 2 hours ago
**Processing Duration**: 3 days 4 hours

## Status Overview
[Status table here]

## Recent Activity
- **Processed Today**: 25 stocks
- **Processed This Week**: 150 stocks
- **Failed Stocks**: 5 stocks
- **Unprocessed**: 100 stocks

### Recent Failures
- 2412 (中華電) - 2025-01-15
- 2330 (台積電) - 2025-01-14
...

### Sample Unprocessed Stocks
- 1234, 5678, 9012, ... and 97 more
```

## Advanced Features

### Stock-Specific Analysis
```python
def get_stock_breakdown():
    """Analyze by individual stock status"""
    return {
        'processed_today': count,
        'processed_this_week': count,
        'failed_stocks': [list of failed stocks with details],
        'unprocessed_stocks': [list of stock IDs],
        'recent_successes': [list with dates]
    }
```

### Cross-Reference Validation
```python
def validate_consistency():
    """Check consistency between CSV and actual files"""
    csv_successful = count_csv_success()
    md_files_count = count_md_files()
    
    if csv_successful != md_files_count:
        report_discrepancy()
```

### Processing Strategy Integration
```python
def analyze_processing_needs():
    """Determine what GetAll.py should process next"""
    return {
        'strategy': 'PRIORITY|FULL_REFRESH|UP_TO_DATE',
        'stocks_to_process': [list],
        'estimated_time': duration
    }
```

## Command Line Interface

### Basic Usage
```bash
# Show current status
python download_results_counts.py

# Generate detailed report
python download_results_counts.py --detailed

# Update README.md
python download_results_counts.py --update-readme

# Export as JSON
python download_results_counts.py --format json

# Save to file
python download_results_counts.py --detailed --output report.md
```

### Options
```bash
Options:
  --output FILE     Save output to file instead of stdout
  --format FORMAT   Output format: table|json (default: table)
  --detailed        Include stock breakdown and recent activity
  --update-readme   Update README.md status section automatically
  --base-dir DIR    Base directory (default: current directory)
  --help           Show help message
```

## Integration Points

### README.md Integration
```markdown
## Status
Update time: 2025-01-15 14:30:25 CST

| Metric | Value |
|--------|-------|
| Total Stocks | 1,000 |
| Successful | 850 (85.0%) |
| Failed | 50 |
| Unprocessed | 100 |
| MD Files Found | 850 |
| Last Updated | 2 hours ago |
| Processing Duration | 3 days 4 hours |
```

### GetAll.py Integration
```python
# GetAll.py can use this script for status
def check_current_status():
    result = subprocess.run(['python', 'download_results_counts.py', '--format', 'json'])
    status = json.loads(result.stdout)
    return status['summary']
```

### Automation Integration
```bash
# GitHub Actions workflow
- name: Update Status
  run: |
    python download_results_counts.py --update-readme
    git add README.md
    git commit -m "Update download status"
```

## Error Handling

### Missing Files
```python
# Handle missing poorstock directory
if not poorstock_dir.exists():
    return default_stats_with_error()

# Handle missing results CSV
if not results_file.exists():
    return stats_with_unprocessed_count()

# Handle missing stock CSV
if not stock_csv.exists():
    return basic_stats_without_totals()
```

### Data Validation
```python
# Validate CSV structure
required_columns = ['filename', 'last_update_time', 'success', 'process_time']
if not all(col in csv_columns for col in required_columns):
    return invalid_format_error()

# Validate filename patterns
def is_valid_poorstock_filename(filename):
    return re.match(r'poorstock_\d+_.*\.md', filename)

# Handle encoding issues
def safe_file_read(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except UnicodeDecodeError:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            return f.read()
```

### Performance Considerations
```python
# Efficient file counting
def count_md_files():
    return len(list(poorstock_dir.glob('poorstock_*.md')))

# Memory-efficient CSV reading
def analyze_large_csv():
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        # Process row by row without loading entire file
        for row in reader:
            process_row(row)
```

## Testing Strategy

### Unit Tests
```python
def test_time_calculations():
    # Test various time differences
    # Test timezone handling
    # Test special date values

def test_csv_parsing():
    # Test with real poorstock CSV
    # Test with missing columns
    # Test with empty files

def test_stock_analysis():
    # Test cross-referencing with stock CSV
    # Test filename pattern matching
    # Test file counting
```

### Integration Tests
```python
def test_with_real_data():
    # Use actual poorstock directory
    # Verify consistency between CSV and files
    # Test README.md update functionality

def test_error_scenarios():
    # Missing directories
    # Corrupted CSV files
    # Permission errors
    # Network issues (for future enhancements)
```

### Edge Case Testing
- Empty poorstock directory
- Partial processing results
- Future dates in timestamps
- Unicode characters in stock names
- Very large stock lists (5000+ stocks)
- Concurrent access to CSV files

## Maintenance Considerations

### Extensibility
```python
# Easy to add new metrics
def calculate_additional_metrics(results_df):
    return {
        'average_processing_time': calculate_avg_time(),
        'peak_processing_hours': find_peak_hours(),
        'failure_patterns': analyze_failures()
    }

# Support for different stock exchanges
EXCHANGE_CONFIGS = {
    'TWSE_TPEX': {'csv': 'StockID_TWSE_TPEX.csv', 'prefix': 'poorstock_'},
    'OTHER': {'csv': 'other_stocks.csv', 'prefix': 'other_'}
}
```

### Monitoring Integration
```python
# Metrics for external monitoring systems
def export_prometheus_metrics():
    return f"""
    poorstock_total_stocks {stats['total_stocks']}
    poorstock_successful {stats['successful']}
    poorstock_failed {stats['failed']}
    poorstock_success_rate {stats['success_rate']}
    """

# Alert conditions
def check_alert_conditions(stats):
    alerts = []
    if stats['success_rate'] < 90:
        alerts.append('Low success rate')
    if stats['failed'] > 100:
        alerts.append('High failure count')
    return alerts
```

### Documentation Updates
- Keep sync with poorstock.py changes
- Update when CSV format evolves
- Maintain compatibility with GetAll.py
- Track performance improvements

This design creates a focused, maintainable solution for monitoring PoorStock download status with robust error handling, Taiwan timezone support, and flexible output options specifically tailored for the single-directory stock scraping project structure.