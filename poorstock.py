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

def format_current_price_table(data, historical_data=None):
    """Format current price data into a proper markdown table."""
    table = []
    table.append("| 項目 | 價格 |")
    table.append("|------|------|")
    
    headers = ['開盤', '最高', '最低', '收盤']
    values = []
    
    # Extract numeric values from data
    if data:
        for item in data:
            item_str = str(item).strip()
            if item_str and (item_str.replace('.', '').replace(',', '').isdigit() or 
                           ('.' in item_str and item_str.replace('.', '').replace(',', '').isdigit())):
                values.append(item_str)
    
    # If we don't have enough current price values, try to get from historical data
    if len(values) < 4 and historical_data and len(historical_data) >= 5:
        print(f"📊 Current price values found: {len(values)}, using historical data fallback")
        # Find the most recent complete data row (should start with a date)
        for i, item in enumerate(historical_data):
            if re.match(r'202[45]/\d{2}/\d{2}', str(item)):
                # Found a date, next 4 items should be OHLC
                recent_values = []
                for j in range(1, 5):  # Skip date, get next 4 values
                    if i + j < len(historical_data):
                        val = str(historical_data[i + j]).strip()
                        if val and (val.replace('.', '').replace(',', '').isdigit() or 
                                  ('.' in val and val.replace('.', '').replace(',', '').isdigit())):
                            recent_values.append(val)
                
                # Use the most recent historical data if we have 4 values
                if len(recent_values) >= 4:
                    values = recent_values[:4]  # Take first 4 (OHLC)
                    print(f"📊 Using recent historical values: {values}")
                    break
    
    # Create table rows
    for i, header in enumerate(headers):
        if i < len(values) and values[i]:
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
    while i < len(data) and row_count < 30:
        if re.match(r'202[45]/\d{2}/\d{2}', data[i]):
            # This is a date, collect next 5 values
            row = [data[i]]  # Start with date
            
            # Look for the next 5 numeric values
            values_collected = 0
            j = i + 1
            while j < len(data) and values_collected < 5:
                item = data[j].strip()
                if item and (item.replace('.', '').replace(',', '').isdigit() or 
                           ('.' in item and item.replace('.', '').replace(',', '').isdigit())):
                    row.append(item)
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
    
    table.append("")
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
    while i < len(data) and row_count < 25:
        if re.match(r'202[45]/\d{2}/\d{2}', data[i]):
            # This is a date, collect next 4 values
            row = [data[i]]
            
            # Look for 3 percentages and 1 number
            values_collected = 0
            j = i + 1
            while j < len(data) and values_collected < 4:
                item = data[j].strip()
                if item and ('%' in item or (item.replace(',', '').replace('.', '').isdigit() and len(item) > 3)):
                    row.append(item)
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
    
    table.append("")
    return table

def scrape_poorstock(stock_id, base_dir="."):
    """
    Scrape stock data from poorstock.com for a specific stock ID.
    """
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
        
        # Scrape the webpage with proper encoding handling
        url = f"https://poorstock.com/stock/{stock_id}"
        print(f"🌐 Fetching: {url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Accept-Encoding': 'identity'
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        response = session.get(url, timeout=15)
        response.raise_for_status()
        response.encoding = 'utf-8'
        
        soup = BeautifulSoup(response.text, 'html.parser')
        print(f"📊 Page loaded successfully")
        
        # Get full text content
        full_text = soup.get_text()
        print(f"📊 Total content length: {len(full_text)} characters")
        
        # Verify content quality
        has_chinese = any(ord(c) > 0x4e00 and ord(c) < 0x9fff for c in full_text[:200])
        print(f"✅ Contains Chinese characters: {has_chinese}")
        
        if not has_chinese:
            print("❌ Content appears to be unreadable")
            return False
        
        # Build content
        content_parts = []
        
        # Add title
        title = soup.find('title')
        if title:
            title_text = title.get_text().strip()
            content_parts.append(f"# {title_text}\n")
        
        # Extract data date information
        data_date_info = ""
        date_patterns = [
            r'資料日期[：:\s]*2025/\d{2}/\d{2}[^。]*?更新[。]?',
            r'2025/\d{2}/\d{2}[^。]*?每天\d{2}:\d{2}後更新',
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, full_text, re.DOTALL)
            if match:
                data_date_info = re.sub(r'<[^>]+>', '', match.group(0).strip())
                data_date_info = re.sub(r'\s+', ' ', data_date_info)
                print(f"📅 Found date info: {data_date_info}")
                break
        
        if not data_date_info:
            date_part = re.search(r'2025/\d{2}/\d{2}', full_text)
            update_part = re.search(r'每天\d{2}:\d{2}後更新', full_text)
            if date_part and update_part:
                data_date_info = f"資料日期：{date_part.group(0)}，{update_part.group(0)}。"
        
        # Process content sections
        lines = [line.strip() for line in full_text.split('\n') if line.strip()]
        
        daily_price_data = []
        ownership_data = []
        current_price_data = []
        sections_added = set()
        current_section = ""
        
        # Extract data systematically
        for i, line in enumerate(lines):
            # Section detection
            if any(keyword in line for keyword in ["每日股價資訊", "本日股價資訊", "K線圖與股價本日交易資訊"]):
                if "daily_price_section" not in sections_added:
                    content_parts.append("\n## 每日股價資訊\n")
                    if data_date_info:
                        content_parts.append(f"**{data_date_info}**\n")
                    sections_added.add("daily_price_section")
                current_section = "daily_price"
                
            elif "AI的K線圖分析" in line or "股價走勢分析" in line:
                # Process tables before AI section
                if daily_price_data or current_price_data:
                    if current_price_data:
                        content_parts.extend(format_current_price_table(current_price_data, daily_price_data))
                        current_price_data = []
                    if daily_price_data:
                        content_parts.extend(format_daily_price_table(daily_price_data))
                        daily_price_data = []
                
                if "ai_analysis" not in sections_added:
                    content_parts.append("\n## AI股價走勢分析與操作建議\n")
                    sections_added.add("ai_analysis")
                current_section = "ai_analysis"
                
            elif "每週股權分散表" in line:
                if "weekly_ownership" not in sections_added:
                    content_parts.append("\n### 每週股權分散表分級資料\n")
                    sections_added.add("weekly_ownership")
                current_section = "ownership"
                
            elif "評論討論區" in line:
                current_section = "skip"
                
            # Data collection
            elif current_section == "daily_price":
                # More aggressive current price detection
                if any(keyword in line for keyword in ['開', '高', '低', '收']):
                    current_price_data.append(line)
                    # Look ahead for values in the next few lines
                    for j in range(i+1, min(i+8, len(lines))):
                        next_line = lines[j].strip()
                        if next_line and (next_line.replace('.', '').replace(',', '').isdigit() or 
                                        ('.' in next_line and next_line.replace('.', '').replace(',', '').isdigit())) and len(next_line) < 10:
                            current_price_data.append(next_line)
                            if len(current_price_data) >= 8:  # Headers + 4 values should be enough
                                break
                elif re.match(r'202[45]/\d{2}/\d{2}', line):
                    daily_price_data.append(line)
                elif line.replace('.', '').replace(',', '').isdigit() and '.' in line and len(line) < 10:
                    # Check if we're still in current price context (near headers)
                    recent_headers = any('開' in l or '高' in l or '低' in l or '收' in l 
                                       for l in lines[max(0, i-5):i])
                    if recent_headers and len(current_price_data) < 12:
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
                elif re.search(r'\d+\.\d+%', line) or (line.replace(',', '').isdigit() and len(line) > 3):
                    ownership_data.append(line)
        
        # Process remaining tables
        if daily_price_data or current_price_data:
            if current_price_data:
                content_parts.extend(format_current_price_table(current_price_data, daily_price_data))
            if daily_price_data:
                content_parts.extend(format_daily_price_table(daily_price_data))
        if ownership_data:
            content_parts.extend(format_ownership_table(ownership_data))
        
        # Add metadata
        content_parts.extend([
            f"\n---\n",
            f"**股票代號:** {stock_id}",
            f"**公司名稱:** {stock_name}",
            f"**資料來源:** {url}",
            f"**抓取時間:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])
        
        # Save content
        content = "\n".join(content_parts)
        content = re.sub(r'\n{3,}', '\n\n', content)
        
        filepath = poorstock_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✅ Content saved to: {filepath}")
        print(f"📄 Final content length: {len(content)} characters")
        
        # Update results CSV
        process_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_mod_time = datetime.fromtimestamp(filepath.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        
        if results_file.exists():
            results_df = pd.read_csv(results_file)
        else:
            results_df = pd.DataFrame(columns=['filename', 'last_update_time', 'success', 'process_time'])
        
        mask = results_df['filename'] == filename
        if mask.any():
            results_df.loc[mask, 'last_update_time'] = file_mod_time
            results_df.loc[mask, 'success'] = True
            results_df.loc[mask, 'process_time'] = process_time
        else:
            new_record = pd.DataFrame([{
                'filename': filename,
                'last_update_time': file_mod_time,
                'success': True,
                'process_time': process_time
            }])
            results_df = pd.concat([results_df, new_record], ignore_index=True)
        
        results_df.to_csv(results_file, index=False)
        print(f"📊 Results updated: {results_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
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
        
        # Delay between requests
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