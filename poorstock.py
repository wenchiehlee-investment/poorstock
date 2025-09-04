#!/usr/bin/env python3
"""
Enhanced PoorStock.py - Handles Dynamic Loading and Rate Limiting
Addresses intermittent data loading issues
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import sys
from pathlib import Path
import re
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

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
                       .replace("📄", "[FILE]")
                       .replace("⏳", "[WAIT]")
                       .replace("🔄", "[RETRY]"))
        print(safe_message)

class EnhancedPoorStockScraper:
    def __init__(self, use_selenium=False, headless=True):
        """
        Initialize scraper with multiple fallback methods.
        
        Args:
            use_selenium: Use Selenium for JavaScript rendering
            headless: Run browser in headless mode
        """
        self.use_selenium = use_selenium
        self.headless = headless
        self.driver = None
        self.session = requests.Session()
        
        # Enhanced headers to avoid detection
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.session.headers.update(self.headers)
    
    def setup_selenium(self):
        """Setup Selenium WebDriver with proper options."""
        if self.driver:
            return
        
        try:
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument('--headless')
            
            # Anti-detection options
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            safe_print("[OK] Selenium WebDriver initialized")
            
        except Exception as e:
            safe_print(f"[ERROR] Failed to setup Selenium: {e}")
            self.use_selenium = False
    
    def fetch_with_selenium(self, url, wait_for_data=True):
        """Fetch page using Selenium with proper wait for dynamic content."""
        try:
            self.setup_selenium()
            if not self.driver:
                return None
            
            safe_print(f"[WEB] Fetching with Selenium: {url}")
            self.driver.get(url)
            
            if wait_for_data:
                # Wait for tables to load (up to 15 seconds)
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.TAG_NAME, "table"))
                    )
                    
                    # Additional wait for dynamic content
                    time.sleep(3)
                    safe_print("[WAIT] Waited for dynamic content to load")
                    
                except TimeoutException:
                    safe_print("[WARNING] Timeout waiting for tables, proceeding anyway")
            
            html = self.driver.page_source
            safe_print(f"[DATA] Got HTML content: {len(html)} characters")
            return html
            
        except Exception as e:
            safe_print(f"[ERROR] Selenium fetch failed: {e}")
            return None
    
    def fetch_with_requests(self, url, retries=3):
        """Fetch page using requests with retry logic and rate limiting."""
        for attempt in range(retries):
            try:
                if attempt > 0:
                    # Exponential backoff with jitter
                    delay = (2 ** attempt) + random.uniform(1, 3)
                    safe_print(f"[RETRY] Waiting {delay:.1f}s before retry {attempt + 1}")
                    time.sleep(delay)
                
                safe_print(f"[WEB] Fetching with requests (attempt {attempt + 1}): {url}")
                
                # Add random delay to avoid rate limiting
                time.sleep(random.uniform(1, 3))
                
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                response.encoding = 'utf-8'
                
                safe_print(f"[DATA] Got response: {len(response.text)} characters")
                return response.text
                
            except requests.exceptions.RequestException as e:
                safe_print(f"[ERROR] Request failed (attempt {attempt + 1}): {e}")
                if attempt == retries - 1:
                    return None
        
        return None
    
    def fetch_page(self, url):
        """Fetch page with automatic fallback between methods."""
        html = None
        
        # Try Selenium first if enabled (better for dynamic content)
        if self.use_selenium:
            html = self.fetch_with_selenium(url)
            if html and self.validate_html_content(html):
                safe_print("[OK] Successfully fetched with Selenium")
                return html
            else:
                safe_print("[WARNING] Selenium fetch incomplete, trying requests")
        
        # Fallback to requests
        html = self.fetch_with_requests(url)
        if html and self.validate_html_content(html):
            safe_print("[OK] Successfully fetched with requests")
            return html
        
        # If requests failed and Selenium wasn't tried, try Selenium
        if not self.use_selenium:
            safe_print("[WARNING] Requests failed, trying Selenium as fallback")
            self.use_selenium = True
            html = self.fetch_with_selenium(url)
            if html and self.validate_html_content(html):
                safe_print("[OK] Successfully fetched with Selenium fallback")
                return html
        
        safe_print("[ERROR] All fetch methods failed")
        return None
    
    def validate_html_content(self, html):
        """Validate that HTML contains expected content."""
        if not html or len(html) < 1000:
            return False
        
        # Check for loading messages that indicate incomplete data
        loading_indicators = ['載入中', 'loading', '請稍候', '資料更新中']
        soup = BeautifulSoup(html, 'html.parser')
        text_content = soup.get_text().lower()
        
        # Count how many loading indicators are present
        loading_count = sum(1 for indicator in loading_indicators if indicator in text_content)
        
        # Also check if we have actual table data
        tables = soup.find_all('table')
        has_meaningful_tables = len(tables) >= 2
        
        if loading_count > 2:  # Too many loading messages
            safe_print(f"[WARNING] Content validation failed: {loading_count} loading indicators found")
            return False
        
        if not has_meaningful_tables:
            safe_print("[WARNING] Content validation failed: insufficient tables")
            return False
        
        return True
    
    def identify_table_by_content(self, table):
        """Identify table type by content analysis."""
        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                return 'unknown'
            
            # Get all cell text for analysis
            all_text = table.get_text()
            header_cells = [td.get_text().strip() for td in rows[0].find_all(['th', 'td'])]
            
            # Check for loading messages in table
            if any(loading in all_text for loading in ['載入中', 'loading', '請稍候']):
                return 'loading'
            
            # Daily prices: many rows, date pattern, OHLCV columns
            if (len(rows) > 20 and 
                any('日期' in cell for cell in header_cells) and
                any('開盤' in cell or '收盤' in cell for cell in header_cells)):
                
                # Verify with actual data
                for row in rows[1:4]:  # Check first few data rows
                    cells = [td.get_text().strip() for td in row.find_all(['td', 'th'])]
                    if cells and re.match(r'202[0-9]/\d{2}/\d{2}', cells[0]):
                        return 'daily_prices'
            
            # Ownership: fewer rows, percentage data, specific headers
            elif (5 < len(rows) < 60 and 
                  any('持股比例' in cell or '100張' in cell for cell in header_cells)):
                return 'ownership'
            
            # Current prices: small table with price info
            elif (len(rows) <= 10 and 
                  any(price in cell for price in ['開盤', '收盤', '最高', '最低'] for cell in header_cells)):
                return 'current_prices'
            
            return 'unknown'
            
        except Exception as e:
            safe_print(f"[ERROR] Table identification error: {e}")
            return 'unknown'
    
    def extract_data_with_validation(self, soup):
        """Extract data with enhanced validation and error handling."""
        tables = soup.find_all('table')
        safe_print(f"[DATA] Found {len(tables)} tables")
        
        data = {
            'current_prices': {},
            'daily_prices': [],
            'ownership_data': []
        }
        
        # Analyze each table
        daily_table = None
        ownership_table = None
        current_table = None
        loading_tables = []
        
        for i, table in enumerate(tables):
            table_type = self.identify_table_by_content(table)
            safe_print(f"[DATA] Table {i}: '{table_type}'")
            
            if table_type == 'loading':
                loading_tables.append(i)
            elif table_type == 'daily_prices':
                daily_table = table
            elif table_type == 'ownership':
                ownership_table = table
            elif table_type == 'current_prices':
                current_table = table
        
        if loading_tables:
            safe_print(f"[WARNING] Found {len(loading_tables)} tables still loading")
        
        # Extract with enhanced error handling
        self.extract_current_prices(current_table, daily_table, data)
        self.extract_daily_prices(daily_table, data)
        self.extract_ownership_data(ownership_table, data)
        
        return data
    
    def extract_current_prices(self, current_table, daily_table, data):
        """Extract current prices with fallback logic."""
        try:
            if current_table:
                rows = current_table.find_all('tr')
                for row in rows:
                    cells = [td.get_text().strip() for td in row.find_all(['td', 'th'])]
                    if len(cells) >= 2:
                        key, value = cells[0], cells[1]
                        if any(price_key in key for price_key in ['開盤', '最高', '最低', '收盤']):
                            data['current_prices'][key] = value
                safe_print(f"[DATA] Extracted current prices from dedicated table")
            
            # Fallback to daily table if current prices not found or incomplete
            if len(data['current_prices']) < 4 and daily_table:
                daily_rows = daily_table.find_all('tr')
                if len(daily_rows) >= 2:
                    cells = [td.get_text().strip() for td in daily_rows[1].find_all(['td', 'th'])]
                    if len(cells) >= 5 and re.match(r'202[45]/\d{2}/\d{2}', cells[0]):
                        data['current_prices'] = {
                            '開盤': cells[1],
                            '最高': cells[2], 
                            '最低': cells[3],
                            '收盤': cells[4]
                        }
                        safe_print("[DATA] Extracted current prices from daily table")
                        
        except Exception as e:
            safe_print(f"[ERROR] Error extracting current prices: {e}")
    
    def extract_daily_prices(self, daily_table, data):
        """Extract daily prices with validation."""
        if not daily_table:
            safe_print("[WARNING] No daily prices table found")
            return
        
        try:
            rows = daily_table.find_all('tr')
            extracted = 0
            
            for row in rows[1:]:  # Skip header
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
                    extracted += 1
            
            safe_print(f"[DATA] Extracted {extracted} daily price records")
            
        except Exception as e:
            safe_print(f"[ERROR] Error extracting daily prices: {e}")
    
    def extract_ownership_data(self, ownership_table, data):
        """Extract ownership data with validation."""
        if not ownership_table:
            safe_print("[WARNING] No ownership table found")
            return
        
        try:
            rows = ownership_table.find_all('tr')
            extracted = 0
            
            for row in rows[1:]:  # Skip header
                cells = [td.get_text().strip() for td in row.find_all(['td', 'th'])]
                
                if len(cells) >= 5 and re.match(r'202[345]/\d{2}/\d{2}', cells[0]):
                    data['ownership_data'].append({
                        'date': cells[0],
                        'small': cells[1],
                        'medium': cells[2],   
                        'large': cells[3],
                        'total_holders': cells[4]
                    })
                    extracted += 1
            
            safe_print(f"[DATA] Extracted {extracted} ownership records")
            
        except Exception as e:
            safe_print(f"[ERROR] Error extracting ownership data: {e}")
    
    def cleanup(self):
        """Clean up resources."""
        if self.driver:
            try:
                self.driver.quit()
                safe_print("[OK] Selenium driver closed")
            except Exception as e:
                safe_print(f"[WARNING] Error closing driver: {e}")

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
    
    for record in reversed(ownership_data[-25:]):
        table.append(f"| {record['date']} | {record['small']} | {record['medium']} | {record['large']} | {record['total_holders']} |")
    
    table.append("")
    return table

def extract_ai_content(full_text):
    """Extract AI analysis content."""
    ai_content = []
    
    ai_start = full_text.find('AI')
    if ai_start > 0:
        ai_section = full_text[ai_start:ai_start+8000]
        lines = [line.strip() for line in ai_section.split('\n') if line.strip()]
        
        for line in lines:
            if len(line) < 20:
                continue
            if any(keyword in line for keyword in ["一、", "二、", "三、", "四、"]):
                ai_content.append(f"\n### {line}\n")
            elif "元" in line and any(keyword in line for keyword in ["支撐", "壓力", "目標"]):
                ai_content.append(f"\n**{line}**\n")
            elif len(line) > 30 and not line.startswith(('http', 'www', '©')):
                ai_content.append(f"{line}\n")
    
    return ai_content[:50]

def scrape_poorstock_enhanced(stock_id, base_dir=".", use_selenium=None):
    """
    Enhanced scraping with dynamic content handling.
    
    Args:
        stock_id: Stock ID to scrape
        base_dir: Base directory for files
        use_selenium: Force Selenium usage (auto-detect if None)
    """
    base_path = Path(base_dir)
    poorstock_dir = base_path / "poorstock"
    poorstock_dir.mkdir(exist_ok=True)
    
    csv_file = base_path / "StockID_TWSE_TPEX.csv"
    results_file = poorstock_dir / "download_results.csv"
    
    # Auto-detect if Selenium is needed
    if use_selenium is None:
        # Check if this stock was previously problematic
        if results_file.exists():
            results_df = pd.read_csv(results_file)
            stock_results = results_df[results_df['filename'].str.contains(str(stock_id))]
            if not stock_results.empty and not stock_results.iloc[0]['success']:
                use_selenium = True
                safe_print("[DECISION] Using Selenium due to previous failures")
            else:
                use_selenium = False
        else:
            use_selenium = False
    
    scraper = EnhancedPoorStockScraper(use_selenium=use_selenium)
    
    try:
        # Load stock data
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
        
        # Fetch page content
        url = f"https://poorstock.com/stock/{stock_id}"
        html = scraper.fetch_page(url)
        
        if not html:
            safe_print("[ERROR] Failed to fetch page content")
            return False
        
        soup = BeautifulSoup(html, 'html.parser')
        full_text = soup.get_text()
        safe_print(f"[DATA] Processing content: {len(full_text)} characters")
        
        # Extract data with validation
        data = scraper.extract_data_with_validation(soup)
        
        # Validate extraction results
        success_metrics = {
            'current_prices': len(data['current_prices']) >= 3,
            'daily_prices': len(data['daily_prices']) > 0,
            'ownership_data': len(data['ownership_data']) > 0
        }
        
        safe_print(f"[DATA] Extraction results: {success_metrics}")
        
        # Build and save content
        content_parts = []
        
        # Title
        title = soup.find('title')
        if title:
            content_parts.append(f"# {title.get_text().strip()}\n")
        
        # Current prices section
        content_parts.append("\n## 每日股價資訊\n")
        
        # Date info
        date_match = re.search(r'資料日期[：:\s]*202[45]/\d{2}/\d{2}[^。]*?更新[。]?', full_text)
        if date_match:
            content_parts.append(f"**{date_match.group(0).strip()}**\n")
        
        content_parts.extend(format_current_price_table(data['current_prices']))
        content_parts.extend(format_daily_price_table(data['daily_prices']))
        
        # AI analysis
        content_parts.append("\n## AI股價走勢分析與操作建議\n")
        ai_content = extract_ai_content(full_text)
        content_parts.extend(ai_content)
        
        # Ownership data
        content_parts.append("\n### 每週股權分散表分級資料\n")
        content_parts.extend(format_ownership_table(data['ownership_data']))
        
        # Metadata
        content_parts.extend([
            f"\n---\n",
            f"**股票代號:** {stock_id}",
            f"**公司名稱:** {stock_name}", 
            f"**資料來源:** {url}",
            f"**抓取時間:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ])
        
        # Save file
        content = "\n".join(content_parts)
        content = re.sub(r'\n{3,}', '\n\n', content)
        
        filepath = poorstock_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        safe_print(f"[OK] Content saved: {filepath}")
        safe_print(f"[FILE] Content length: {len(content)} characters")
        
        # Update results
        process_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_mod_time = datetime.fromtimestamp(filepath.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        
        if results_file.exists():
            results_df = pd.read_csv(results_file)
        else:
            results_df = pd.DataFrame(columns=['filename', 'last_update_time', 'success', 'process_time'])
        
        mask = results_df['filename'] == filename
        success = all(success_metrics.values())
        
        if mask.any():
            results_df.loc[mask, 'last_update_time'] = file_mod_time
            results_df.loc[mask, 'success'] = success
            results_df.loc[mask, 'process_time'] = process_time
        else:
            new_record = pd.DataFrame([{
                'filename': filename,
                'last_update_time': file_mod_time,
                'success': success,
                'process_time': process_time
            }])
            results_df = pd.concat([results_df, new_record], ignore_index=True)
        
        results_df.to_csv(results_file, index=False)
        
        return success
        
    except Exception as e:
        safe_print(f"[ERROR] Scraping error: {e}")
        return False
    finally:
        scraper.cleanup()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python poorstock.py <stock_id> [--selenium]")
        sys.exit(1)
    
    try:
        stock_id = int(sys.argv[1])
        use_selenium = '--selenium' in sys.argv
        
        success = scrape_poorstock_enhanced(stock_id, use_selenium=use_selenium)
        if success:
            safe_print(f"\n[SUCCESS] Successfully processed stock {stock_id}")
        else:
            safe_print(f"\n[FAILED] Failed to process stock {stock_id}")
            sys.exit(1)
    except ValueError:
        safe_print("[ERROR] Invalid stock ID. Please provide a valid integer.")
        sys.exit(1)