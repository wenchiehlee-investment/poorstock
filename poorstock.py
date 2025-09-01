#!/usr/bin/env python3
"""
PoorStock.py - Simple Stock Scraper
Usage: python poorstock.py 2412
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import sys
from pathlib import Path
import re

def format_current_price_table(data):
    """Format current price data into a proper markdown table."""
    if not data or len(data) < 4:
        return ["*本日股價資訊載入中...*\n"]
    
    table = []
    table.append("| 項目 | 價格 |")
    table.append("|------|------|")
    
    # Try to match headers with values
    headers = ['開盤', '最高', '最低', '收盤']
    values = []
    
    for item in data:
        if item.replace('.', '').replace(',', '').isdigit():
            values.append(item)
    
    # Create table rows
    for i, header in enumerate(headers):
        if i < len(values):
            table.append(f"| {header} | {values[i]} |")
        else:
            table.append(f"| {header} | - |")
    
    table.append("")  # Empty line after table
    return table

def format_daily_price_table(data):
    """Format daily price data into a proper markdown table."""
    if not data or len(data) < 10:
        return ["*每日股價資訊載入中...*\n"]
    
    table = []
    table.append("| 日期 | 開盤價 | 最高價 | 最低價 | 收盤價 | 成交量 |")
    table.append("|------|--------|--------|--------|--------|--------|")
    
    # Group data: date followed by 5 price/volume values
    i = 0
    row_count = 0
    while i < len(data) and row_count < 30:  # Limit to 30 rows
        if re.match(r'202[45]/\d{2}/\d{2}', data[i]):
            # This is a date, collect next 5 values
            row = [data[i]]  # Start with date
            
            # Look for the next 5 numeric values
            values_collected = 0
            j = i + 1
            while j < len(data) and values_collected < 5:
                if data[j].replace('.', '').replace(',', '').isdigit() and '.' in data[j]:
                    row.append(data[j])
                    values_collected += 1
                j += 1
            
            # Fill missing values with '-'
            while len(row) < 6:
                row.append('-')
            
            # Add table row
            table.append(f"| {' | '.join(row)} |")
            row_count += 1
            i = j
        else:
            i += 1
    
    table.append("")  # Empty line after table
    return table

def format_ownership_table(data):
    """Format stock ownership distribution data into a proper markdown table."""
    if not data or len(data) < 10:
        return ["*股權分散表載入中...*\n"]
    
    table = []
    table.append("| 日期 | 100張以下持股比例 | 100-1000張持股比例 | 1000張以上持股比例 | 總股東人數 |")
    table.append("|------|-------------------|--------------------|--------------------|----------|")
    
    # Group data: date followed by 3 percentages and 1 number
    i = 0
    row_count = 0
    while i < len(data) and row_count < 25:  # Limit to 25 rows
        if re.match(r'202[45]/\d{2}/\d{2}', data[i]):
            # This is a date, collect next 4 values
            row = [data[i]]  # Start with date
            
            # Look for 3 percentages and 1 number
            values_collected = 0
            j = i + 1
            while j < len(data) and values_collected < 4:
                if '%' in data[j] or (data[j].replace(',', '').isdigit() and len(data[j]) > 3):
                    row.append(data[j])
                    values_collected += 1
                j += 1
            
            # Fill missing values with '-'
            while len(row) < 5:
                row.append('-')
            
            # Add table row
            table.append(f"| {' | '.join(row)} |")
            row_count += 1
            i = j
        else:
            i += 1
    
    table.append("")  # Empty line after table
    return table

def scrape_poorstock(stock_id, base_dir="."):
    """
    Scrape stock data from poorstock.com for a specific stock ID.
    
    Args:
        stock_id (int): Stock ID to scrape
        base_dir (str): Base directory for files
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    # Setup directories and files
    base_path = Path(base_dir)
    poorstock_dir = base_path / "poorstock"
    poorstock_dir.mkdir(exist_ok=True)
    
    csv_file = base_path / "StockID_TWSE_TPEX.csv"
    results_file = poorstock_dir / "download_results.csv"
    
    try:
        # Load stock data to get company name
        if not csv_file.exists():
            print(f"❌ Stock CSV file not found: {csv_file}")
            return False
        
        stock_df = pd.read_csv(csv_file)
        stock_row = stock_df[stock_df['代號'] == stock_id]
        
        if stock_row.empty:
            print(f"❌ Stock ID {stock_id} not found in CSV")
            return False
        
        stock_name = stock_row.iloc[0]['名稱']
        filename = f"poorstock_{stock_id}_{stock_name}.md"
        
        print(f"📈 Processing stock: {stock_id} ({stock_name})")
        
        # Scrape the webpage
        url = f"https://poorstock.com/stock/{stock_id}"
        print(f"🌐 Fetching: {url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            # Remove gzip encoding to get uncompressed content
            'Accept-Encoding': 'identity'
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        response = session.get(url, timeout=15)
        response.raise_for_status()
        
        # Handle encoding issues
        if response.encoding is None or response.encoding == 'ISO-8859-1':
            response.encoding = 'utf-8'
        
        # Check if we got readable content
        try:
            content_test = response.text[:100]
            if any(ord(c) > 127 and ord(c) < 256 for c in content_test):
                # Looks like encoding issue, try different approach
                response.encoding = 'utf-8'
        except:
            pass
        
        print(f"📊 Response encoding: {response.encoding}")
        print(f"📊 Content type: {response.headers.get('content-type', 'unknown')}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        print(f"📊 Page loaded successfully")
        
        # Extract all content using a comprehensive approach
        content_parts = []
        
        # Add title
        title = soup.find('title')
        if title:
            title_text = title.get_text().strip()
            content_parts.append(f"# {title_text}\n")
            print(f"📝 Title: {title_text[:50]}...")
        
        # Get full text for analysis
        full_text = soup.get_text()
        print(f"📊 Total content length: {len(full_text)} characters")
        
        # Debug: Check first 200 characters to verify readability
        first_chars = full_text[:200].strip()
        print(f"📋 First 200 chars: {first_chars[:100]}...")
        
        # Verify we have Chinese content
        has_chinese = any(ord(c) > 0x4e00 and ord(c) < 0x9fff for c in first_chars)
        print(f"✅ Contains Chinese characters: {has_chinese}")
        
        # Check what we can find - enhanced debugging
        key_found = {
            '股價': '股價' in full_text,
            'AI分析': 'AI分析' in full_text or 'AI的K線圖分析' in full_text,
            '股權分散': '股權分散' in full_text,
            '評論': '評論' in full_text or 'ANONYMOUS' in full_text,
            '2412': '2412' in full_text,
            '資料日期': '資料日期' in full_text,
            '每天14:00後更新': '每天14:00後更新' in full_text,
            '2025/08/29': '2025/08/29' in full_text,
        }
        
        for key, found in key_found.items():
            print(f"✅ Contains '{key}': {found}")
        
        # Look for data date patterns more broadly
        import re
        date_patterns = re.findall(r'資料日期.*?更新', full_text)
        if date_patterns:
            print(f"📅 Found date patterns: {date_patterns}")
        
        # Also look for 2025/08 patterns
        date_info_patterns = re.findall(r'202\d/\d{2}/\d{2}.*?更新', full_text)
        if date_info_patterns:
            print(f"📅 Found date info patterns: {date_info_patterns}")
        
        # Save debug file
        debug_file = poorstock_dir / f"debug_text_{stock_id}.txt"
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(f"Content Length: {len(full_text)}\n")
            f.write(f"First 500 chars:\n{full_text[:500]}\n")
            f.write(f"\n--- Full Content ---\n")
            f.write(full_text)
        print(f"🔍 Debug file saved: {debug_file}")
        
        # If we don't have readable content, exit early
        if not has_chinese and len(first_chars) < 50:
            print("❌ Content appears to be unreadable or empty")
            return False
        
        # Extract content sections
        lines = [line.strip() for line in full_text.split('\n') if line.strip()]
        
        # Process content systematically with cleaner structure
        current_section = ""
        sections_added = set()
        i = 0
        
        # Data collectors for tables
        daily_price_data = []
        ownership_data = []
        current_price_data = []
        data_date_info = ""
        
        # Enhanced date detection using full text with regex
        import re
        
        # Try multiple patterns to find the date information
        date_patterns = [
            r'資料日期[：:\s]*2025/\d{2}/\d{2}[^。]*?更新[。]?',
            r'2025/\d{2}/\d{2}[^。]*?每天\d{2}:\d{2}後更新',
            r'資料日期[：:\s]*2025/\d{2}/\d{2}',
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, full_text, re.DOTALL)
            if match:
                data_date_info = match.group(0).strip()
                # Clean up any HTML tags or extra whitespace
                data_date_info = re.sub(r'<[^>]+>', '', data_date_info)
                data_date_info = re.sub(r'\s+', ' ', data_date_info)
                print(f"📅 Found complete date info: {data_date_info}")
                break
        
        # If still no match, try looking for the components separately and combine them
        if not data_date_info:
            date_part = re.search(r'2025/\d{2}/\d{2}', full_text)
            update_part = re.search(r'每天\d{2}:\d{2}後更新', full_text)
            
            if date_part and update_part:
                data_date_info = f"資料日期：{date_part.group(0)}，{update_part.group(0)}。"
                print(f"📅 Constructed date info: {data_date_info}")
            elif date_part:
                data_date_info = f"資料日期：{date_part.group(0)}"
                print(f"📅 Found partial date info: {data_date_info}")
        
        while i < len(lines):
            line = lines[i]
            
            # Check for major section headers - consolidate into cleaner structure
            if any(keyword in line for keyword in ["每日股價資訊", "本日股價資訊", "K線圖與股價本日交易資訊"]):
                if "daily_price_section" not in sections_added:
                    # Add consolidated daily price section with date info
                    content_parts.append("\n## 每日股價資訊\n")
                    if data_date_info:
                        content_parts.append(f"**{data_date_info}**\n")
                        print(f"📅 Added date info to section: {data_date_info}")
                    else:
                        print("⚠️ No date info found to add")
                    sections_added.add("daily_price_section")
                    print("📍 Found daily price section (consolidated)")
                current_section = "daily_price"
                
            elif "AI的K線圖分析" in line or "股價走勢分析" in line:
                # First, process any collected table data
                if current_price_data or daily_price_data:
                    # Add current price table FIRST (as in revised version)
                    if current_price_data:
                        content_parts.extend(format_current_price_table(current_price_data))
                        current_price_data = []
                    # Then add historical data table
                    if daily_price_data:
                        content_parts.extend(format_daily_price_table(daily_price_data))
                        daily_price_data = []
                
                if "ai_analysis" not in sections_added:
                    current_section = "ai_analysis"
                    content_parts.append("\n## AI股價走勢分析與操作建議\n")
                    sections_added.add("ai_analysis")
                    print("📍 Found AI analysis section")
                else:
                    current_section = "ai_analysis"  # Continue in same section
                
            elif "股權分散表" in line and "每週股權分散表" not in line:
                # Don't add "## 股權分散表" header, just start collecting data
                current_section = "ownership"
                print("📍 Found ownership section (no header)")
                    
            elif "每週股權分散表" in line:
                if "weekly_ownership" not in sections_added:
                    current_section = "ownership"
                    content_parts.append("\n### 每週股權分散表分級資料\n")
                    sections_added.add("weekly_ownership")
                    print("📍 Found weekly ownership section")
                else:
                    current_section = "ownership"
                    
            elif "評論討論區" in line:
                # Skip discussion section entirely - not needed
                print("📍 Skipping discussion section (not needed)")
                current_section = "skip"
                
            # Collect data based on current section (skip if section is "skip")
            elif current_section == "daily_price":
                if any(keyword in line for keyword in ['開', '高', '低', '收']):
                    current_price_data.append(line)
                elif re.match(r'202[45]/\d{2}/\d{2}', line):
                    daily_price_data.append(line)
                elif line.replace('.', '').replace(',', '').isdigit() and '.' in line and len(line) < 10:
                    # Could be either current or historical price data
                    if len(current_price_data) < 4:  # Assume first 4 numbers are current prices
                        current_price_data.append(line)
                    else:
                        daily_price_data.append(line)
            
            elif current_section == "ai_analysis" and len(line) > 20:
                if any(keyword in line for keyword in ["一、", "二、", "三、", "四、"]):
                    content_parts.append(f"\n### {line}\n")
                elif "元" in line and any(keyword in line for keyword in ["支撐", "壓力", "目標"]):
                    content_parts.append(f"\n**{line}**\n")
                elif len(line) > 30:
                    content_parts.append(f"{line}\n")
            
            elif current_section == "ownership":
                if re.match(r'202[45]/\d{2}/\d{2}', line):
                    ownership_data.append(line)
                elif re.search(r'\d+\.\d+%', line):
                    ownership_data.append(line)
                elif line.replace(',', '').isdigit() and len(line) > 3:
                    ownership_data.append(line)
            
            # Skip processing for discussion section
            elif current_section == "skip":
                pass  # Ignore all discussion content
            
            # General content that doesn't fit specific sections
            elif current_section == "" and len(line) > 30:
                if any(keyword in line for keyword in ["股票", "股價", "分析", "投資"]):
                    content_parts.append(f"{line}\n")
            
            i += 1
            
            # Safety limit
            if len(content_parts) > 400:  # Reduced limit since we skip discussions
                break
        
        # Process any remaining table data
        if daily_price_data:
            content_parts.extend(format_daily_price_table(daily_price_data))
        if current_price_data:
            content_parts.extend(format_current_price_table(current_price_data))
        if ownership_data:
            content_parts.extend(format_ownership_table(ownership_data))
        
        print(f"📄 Extracted {len(content_parts)} content items")
        
        # Add metadata
        content_parts.append(f"\n---\n")
        content_parts.append(f"**股票代號:** {stock_id}")
        content_parts.append(f"**公司名稱:** {stock_name}")
        content_parts.append(f"**資料來源:** {url}")
        content_parts.append(f"**抓取時間:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Combine content
        content = "\n".join(content_parts)
        
        # Clean up formatting
        content = re.sub(r'\n{3,}', '\n\n', content)
        
        print(f"📄 Final content length: {len(content)} characters")
        
        # Save to markdown file
        filepath = poorstock_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✅ Content saved to: {filepath}")
        
        # Update results CSV
        process_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_mod_time = datetime.fromtimestamp(filepath.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        
        # Load or create results CSV
        if results_file.exists():
            results_df = pd.read_csv(results_file)
        else:
            results_df = pd.DataFrame(columns=['filename', 'last_update_time', 'success', 'process_time'])
        
        # Update or add record
        mask = results_df['filename'] == filename
        if mask.any():
            results_df.loc[mask, 'last_update_time'] = file_mod_time
            results_df.loc[mask, 'success'] = True
            results_df.loc[mask, 'process_time'] = process_time
        else:
            new_record = {
                'filename': filename,
                'last_update_time': file_mod_time,
                'success': True,
                'process_time': process_time
            }
            results_df = pd.concat([results_df, pd.DataFrame([new_record])], ignore_index=True)
        
        # Save results CSV
        results_df.to_csv(results_file, index=False)
        print(f"📊 Results updated in: {results_file}")
        
        return True
        
    except requests.RequestException as e:
        print(f"❌ Network error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def batch_process_all(base_dir="."):
    """Process all stocks from the CSV file."""
    
    base_path = Path(base_dir)
    csv_file = base_path / "StockID_TWSE_TPEX.csv"
    
    if not csv_file.exists():
        print(f"❌ Stock CSV file not found: {csv_file}")
        return
    
    stock_df = pd.read_csv(csv_file)
    total_stocks = len(stock_df)
    
    print(f"🚀 Starting batch processing of {total_stocks} stocks...")
    
    successful = 0
    failed = 0
    
    for i, (_, row) in enumerate(stock_df.iterrows(), 1):
        stock_id = row['代號']
        stock_name = row['名稱']
        
        print(f"\n[{i}/{total_stocks}] Processing {stock_id} ({stock_name})")
        
        if scrape_poorstock(stock_id, base_dir):
            successful += 1
        else:
            failed += 1
        
        # Small delay to be respectful to the server
        import time
        time.sleep(2)
    
    print(f"\n🎉 Batch processing complete!")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python poorstock.py <stock_id>     # Process single stock")
        print("  python poorstock.py --all          # Process all stocks")
        print("  python poorstock.py 2412           # Process stock 2412 (中華電)")
        sys.exit(1)
    
    if sys.argv[1] == "--all":
        batch_process_all()
    else:
        try:
            stock_id = int(sys.argv[1])
            success = scrape_poorstock(stock_id)
            if success:
                print(f"\n🎉 Successfully processed stock {stock_id}")
            else:
                print(f"\n😞 Failed to process stock {stock_id}")
                sys.exit(1)
        except ValueError:
            print("❌ Invalid stock ID. Please provide a valid integer.")
            sys.exit(1)