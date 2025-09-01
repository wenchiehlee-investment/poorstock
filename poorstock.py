#!/usr/bin/env python3
"""
PoorStock.py - FINAL FIXED VERSION with correct table parsing
Usage: python poorstock_fixed_final.py 2412
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import sys
from pathlib import Path
import re

# Fix encoding issues on Windows
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

def safe_print(message):
    """Print function that handles Unicode errors gracefully."""
    try:
        print(message)
    except UnicodeEncodeError:
        safe_message = (message
                       .replace("📈", "[CHART]")
                       .replace("📊", "[DATA]")
                       .replace("🌐", "[WEB]")
                       .replace("✅", "[OK]")
                       .replace("❌", "[ERROR]")
                       .replace("🚀", "[START]")
                       .replace("🎉", "[SUCCESS]")
                       .replace("😞", "[FAILED]")
                       .replace("📅", "[DATE]")
                       .replace("📄", "[FILE]"))
        print(safe_message)

def extract_data_from_specific_tables(soup):
    """Extract data directly from the correct tables (Table 2 and Table 4)."""
    
    tables = soup.find_all('table')
    safe_print(f"[DATA] Found {len(tables)} total tables")
    
    data = {
        'current_prices': {},
        'daily_prices': [],
        'ownership_data': []
    }
    
    if len(tables) < 4:
        safe_print("[ERROR] Not enough tables found on page")
        return data
    
    # TABLE 2 (index 1): Daily Prices - has 121 rows
    daily_table = tables[1]  # Second table (0-based index)
    daily_rows = daily_table.find_all('tr')
    safe_print(f"[DATA] Daily prices table has {len(daily_rows)} rows")
    
    # Extract current day prices from the LAST row of daily table
    if len(daily_rows) >= 2:
        last_row = daily_rows[-1]  # Last row should be today
        cells = [td.get_text().strip() for td in last_row.find_all(['td', 'th'])]
        
        if len(cells) >= 5 and re.match(r'202[45]/\d{2}/\d{2}', cells[0]):
            data['current_prices'] = {
                '開盤': cells[1],
                '最高': cells[2], 
                '最低': cells[3],
                '收盤': cells[4]
            }
            safe_print(f"[DATA] Extracted current prices from last row: {cells[0]}")
    
    # Extract ALL daily price rows (skip header row)
    for row in daily_rows[1:]:  # Skip header row
        cells = [td.get_text().strip() for td in row.find_all(['td', 'th'])]
        
        if len(cells) >= 6 and re.match(r'202[45]/\d{2}/\d{2}', cells[0]):
            data['daily_prices'].append({
                'date': cells[0],
                'open': cells[1],
                'high': cells[2],
                'low': cells[3], 
                'close': cells[4],
                'volume': cells[5]
            })
    
    safe_print(f"[DATA] Extracted {len(data['daily_prices'])} daily price records")
    
    # TABLE 4 (index 3): Ownership Data - has 50 rows  
    if len(tables) >= 4:
        ownership_table = tables[3]  # Fourth table (0-based index)
        ownership_rows = ownership_table.find_all('tr')
        safe_print(f"[DATA] Ownership table has {len(ownership_rows)} rows")
        
        # Extract ALL ownership rows (skip header row)
        for row in ownership_rows[1:]:  # Skip header row
            cells = [td.get_text().strip() for td in row.find_all(['td', 'th'])]
            
            if len(cells) >= 5 and re.match(r'202[345]/\d{2}/\d{2}', cells[0]):
                data['ownership_data'].append({
                    'date': cells[0],
                    'small': cells[1],    # 100張以下
                    'medium': cells[2],   # 100-1000張  
                    'large': cells[3],    # 1000張以上
                    'total_holders': cells[4]  # 總股東人數
                })
        
        safe_print(f"[DATA] Extracted {len(data['ownership_data'])} ownership records")
    
    return data

def format_current_price_table(current_prices):
    """Format current price data into markdown table."""
    table = []
    table.append("| 項目 | 價格 |")
    table.append("|------|------|")
    
    headers = ['開盤', '最高', '最低', '收盤']
    for header in headers:
        value = current_prices.get(header, '-')
        table.append(f"| {header} | {value} |")
    
    table.append("")
    return table

def format_daily_price_table(daily_data):
    """Format daily price data into markdown table."""
    if not daily_data:
        return ["*每日股價資訊載入中...*\n"]
    
    table = []
    table.append("| 日期 | 開盤價 | 最高價 | 最低價 | 收盤價 | 成交量 |")
    table.append("|------|--------|--------|--------|--------|--------|")
    
    # Reverse to show most recent first, limit to 30 rows
    for record in reversed(daily_data[-30:]):
        table.append(f"| {record['date']} | {record['open']} | {record['high']} | {record['low']} | {record['close']} | {record['volume']} |")
    
    table.append("")
    return table

def format_ownership_table(ownership_data):
    """Format ownership data into markdown table."""
    if not ownership_data:
        return ["*股權分散表載入中...*\n"]
    
    table = []
    table.append("| 日期 | 100張以下持股比例 | 100-1000張持股比例 | 1000張以上持股比例 | 總股東人數 |")
    table.append("|------|-------------------|--------------------|--------------------|----------|")
    
    # Reverse to show most recent first, limit to 25 rows
    for record in reversed(ownership_data[-25:]):
        table.append(f"| {record['date']} | {record['small']} | {record['medium']} | {record['large']} | {record['total_holders']} |")
    
    table.append("")
    return table

def extract_ai_content(full_text):
    """Extract AI analysis content."""
    ai_content = []
    
    # Find AI section
    ai_start = full_text.find('AI')
    if ai_start > 0:
        ai_section = full_text[ai_start:ai_start+8000]  # Get reasonable chunk
        lines = [line.strip() for line in ai_section.split('\n') if line.strip()]
        
        for line in lines:
            # Skip very short lines
            if len(line) < 20:
                continue
                
            # Add section headers  
            if any(keyword in line for keyword in ["一、", "二、", "三、", "四、"]):
                ai_content.append(f"\n### {line}\n")
            # Add key price levels
            elif "元" in line and any(keyword in line for keyword in ["支撐", "壓力", "目標"]):
                ai_content.append(f"\n**{line}**\n")
            # Add substantial content
            elif len(line) > 30 and not line.startswith(('http', 'www', '©')):
                ai_content.append(f"{line}\n")
    
    return ai_content[:50]  # Limit AI content length

def scrape_poorstock(stock_id, base_dir="."):
    """
    Scrape stock data with FIXED table parsing targeting Table 2 and Table 4.
    """
    base_path = Path(base_dir)
    poorstock_dir = base_path / "poorstock"
    poorstock_dir.mkdir(exist_ok=True)
    
    csv_file = base_path / "StockID_TWSE_TPEX.csv"
    results_file = poorstock_dir / "download_results.csv"
    
    try:
        # Load stock data to get company name
        if not csv_file.exists():
            safe_print(f"[ERROR] Stock CSV file not found: {csv_file}")
            return False
        
        stock_df = pd.read_csv(csv_file)
        stock_row = stock_df[stock_df['代號'] == stock_id]
        
        if stock_row.empty:
            safe_print(f"[ERROR] Stock ID {stock_id} not found in CSV")
            return False
        
        stock_name = stock_row.iloc[0]['名稱']
        filename = f"poorstock_{stock_id}_{stock_name}.md"
        
        safe_print(f"[CHART] Processing stock: {stock_id} ({stock_name})")
        
        # Scrape the webpage
        url = f"https://poorstock.com/stock/{stock_id}"
        safe_print(f"[WEB] Fetching: {url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        response.encoding = 'utf-8'
        
        soup = BeautifulSoup(response.text, 'html.parser')
        safe_print(f"[DATA] Page loaded successfully")
        
        full_text = soup.get_text()
        safe_print(f"[DATA] Total content length: {len(full_text)} characters")
        
        # Extract data using FIXED table targeting
        data = extract_data_from_specific_tables(soup)
        
        # Build content
        content_parts = []
        
        # Add title
        title = soup.find('title')
        if title:
            title_text = title.get_text().strip()
            content_parts.append(f"# {title_text}\n")
        
        # Add current prices section
        content_parts.append("\n## 每日股價資訊\n")
        
        # Extract date info
        date_match = re.search(r'資料日期[：:\s]*202[45]/\d{2}/\d{2}[^。]*?更新[。]?', full_text)
        if date_match:
            content_parts.append(f"**{date_match.group(0).strip()}**\n")
        
        # Add current prices table
        content_parts.extend(format_current_price_table(data['current_prices']))
        
        # Add daily prices table  
        content_parts.extend(format_daily_price_table(data['daily_prices']))
        
        # Add AI analysis section
        content_parts.append("\n## AI股價走勢分析與操作建議\n")
        ai_content = extract_ai_content(full_text)
        content_parts.extend(ai_content)
        
        # Add ownership section
        content_parts.append("\n### 每週股權分散表分級資料\n")
        content_parts.extend(format_ownership_table(data['ownership_data']))
        
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
        
        safe_print(f"[OK] Content saved to: {filepath}")
        safe_print(f"[FILE] Final content length: {len(content)} characters")
        
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
        safe_print(f"[DATA] Results updated: {results_file}")
        
        return True
        
    except Exception as e:
        safe_print(f"[ERROR] Error: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python poorstock_fixed_final.py <stock_id>")
        sys.exit(1)
    
    try:
        stock_id = int(sys.argv[1])
        success = scrape_poorstock(stock_id)
        if success:
            safe_print(f"\n[SUCCESS] Successfully processed stock {stock_id}")
        else:
            safe_print(f"\n[FAILED] Failed to process stock {stock_id}")
            sys.exit(1)
    except ValueError:
        safe_print("[ERROR] Invalid stock ID. Please provide a valid integer.")
        sys.exit(1)