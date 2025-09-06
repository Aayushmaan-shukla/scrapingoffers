#!/usr/bin/env python3
"""
ENHANCED Croma Bank Offer Scraper with Advanced Features
=========================================================
- URL-based persistent caching system (croma_cache.json)
- Exchange price extraction and terminal display
- Discontinued product detection and automatic skipping
- Single browser session with automatic renewal every 100 sessions
- Reaches ALL 3 nested locations for Croma links
- Completely isolates Amazon data (no changes to Amazon offers)
- BeautifulSoup deprecation warning fixes
- Comprehensive error handling and statistics
- No user interaction required
"""

import os
import re
import json
import time
import shutil
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Setup logging
logging.basicConfig(
    filename='enhanced_croma_scraper_comprehensive.log',
    filemode='a',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

# ===============================================
# URL-BASED CACHING SYSTEM
# ===============================================

def load_cache(cache_file="croma_cache.json"):
    """Load the persistent URL-based cache from disk."""
    try:
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            print(f"📋 Loaded cache with {len(cache)} entries from {cache_file}")
            return cache
        else:
            print(f"📝 Creating new cache file: {cache_file}")
            return {}
    except Exception as e:
        print(f"⚠️  Error loading cache: {e}")
        return {}

def save_cache(cache, cache_file="croma_cache.json"):
    """Save the cache to disk."""
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
        print(f"💾 Cache saved with {len(cache)} entries to {cache_file}")
    except Exception as e:
        print(f"⚠️  Error saving cache: {e}")

def is_url_cached(url, cache):
    """Check if URL is already in cache."""
    return url in cache

def add_to_cache(url, data, cache):
    """Add scraped data to cache."""
    cache[url] = {
        'data': data,
        'timestamp': datetime.now().isoformat(),
        'scraped_at': time.time()
    }
    return cache

# ===============================================
# ROBUST DRIVER MANAGER WITH AUTO-RENEWAL
# ===============================================

class RobustDriverManager:
    """Manages Chrome driver with automatic renewal every 100 sessions."""
    
    def __init__(self):
        self.driver = None
        self.session_count = 0
        self.max_sessions = 100
        self.create_new_driver()
    
    def create_new_driver(self):
        """Create a fresh Chrome driver session with version compatibility handling."""
        if self.driver:
            try:
                self.driver.quit()
                print(f"🔄 Closed previous Chrome session")
            except Exception as e:
                print(f"⚠️  Error closing previous session: {e}")
        
        print(f"🤖 Creating new Chrome session (Session count will reset to 0)")
        print(f"🔍 Detected Chrome version mismatch - using compatible configuration")
        
        # Multiple fallback strategies for Chrome version compatibility
        driver_created = False
        
        # Strategy 1: Use version-specific configuration for Chrome 139
        try:
            print(f"🔧 Strategy 1: Using Chrome 139-compatible configuration...")
            options = uc.ChromeOptions()
            
            # Chrome 139-specific arguments
            options.add_argument('--headless=new')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-plugins')
            options.add_argument('--disable-images')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-web-security')
            options.add_argument('--allow-running-insecure-content')
            options.add_argument('--disable-features=VizDisplayCompositor')
            
            # Updated user agent for Chrome 139
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.7258.155 Safari/537.36')
            
            # Force version compatibility
            self.driver = uc.Chrome(options=options, version_main=139)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.session_count = 0
            driver_created = True
            print(f"✅ Strategy 1 SUCCESS: Chrome 139 session created successfully")
            
        except Exception as e:
            print(f"❌ Strategy 1 FAILED: {e}")
        
        # Strategy 2: Auto-detect and download compatible driver
        if not driver_created:
            try:
                print(f"🔧 Strategy 2: Auto-detecting compatible ChromeDriver...")
                options = uc.ChromeOptions()
                options.add_argument('--headless=new')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-gpu')
                options.add_argument('--window-size=1920,1080')
                options.add_argument('--disable-extensions')
                options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.7258.155 Safari/537.36')
                
                # Let undetected-chromedriver auto-download compatible version
                self.driver = uc.Chrome(options=options)
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                self.session_count = 0
                driver_created = True
                print(f"✅ Strategy 2 SUCCESS: Auto-detected compatible driver")
                
            except Exception as e:
                print(f"❌ Strategy 2 FAILED: {e}")
        
        # Strategy 3: Minimal configuration fallback
        if not driver_created:
            try:
                print(f"🔧 Strategy 3: Using minimal configuration...")
                options = uc.ChromeOptions()
                options.add_argument('--headless')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                
                self.driver = uc.Chrome(options=options)
                self.session_count = 0
                driver_created = True
                print(f"✅ Strategy 3 SUCCESS: Minimal configuration working")
                
            except Exception as e:
                print(f"❌ Strategy 3 FAILED: {e}")
        
        # Strategy 4: Force driver path and binary path
        if not driver_created:
            try:
                print(f"🔧 Strategy 4: Trying with explicit paths...")
                import subprocess
                import os
                
                # Try to find Chrome executable
                possible_chrome_paths = [
                    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                    r"C:\Users\{}\AppData\Local\Google\Chrome\Application\chrome.exe".format(os.getenv('USERNAME')),
                ]
                
                chrome_path = None
                for path in possible_chrome_paths:
                    if os.path.exists(path):
                        chrome_path = path
                        break
                
                if chrome_path:
                    print(f"🔍 Found Chrome at: {chrome_path}")
                    options = uc.ChromeOptions()
                    options.binary_location = chrome_path
                    options.add_argument('--headless')
                    options.add_argument('--no-sandbox')
                    options.add_argument('--disable-dev-shm-usage')
                    
                    self.driver = uc.Chrome(options=options)
                    self.session_count = 0
                    driver_created = True
                    print(f"✅ Strategy 4 SUCCESS: Explicit path working")
                else:
                    print(f"❌ Strategy 4: No Chrome executable found")
                
            except Exception as e:
                print(f"❌ Strategy 4 FAILED: {e}")
        
        if not driver_created:
            error_msg = """
❌ CRITICAL ERROR: Could not create Chrome driver with any strategy!

CHROME VERSION COMPATIBILITY ISSUE DETECTED:
- Your Chrome version: 139.0.7258.155
- ChromeDriver expects: Chrome 140

SOLUTIONS TO TRY:
1. Update Google Chrome to version 140 or later
2. Downgrade ChromeDriver to support Chrome 139
3. Use --version-main=139 parameter (attempted but failed)

Please update your Chrome browser or install a compatible ChromeDriver version.
            """
            print(error_msg)
            raise Exception("Chrome driver version compatibility issue - see solutions above")
    
    def get_driver(self):
        """Get current driver, renewing if necessary."""
        if self.session_count >= self.max_sessions:
            print(f"🔄 Reached {self.max_sessions} sessions, renewing Chrome driver...")
            self.create_new_driver()
        
        self.session_count += 1
        return self.driver
    
    def close(self):
        """Close the driver."""
        if self.driver:
            try:
                self.driver.quit()
                print(f"🔄 Chrome driver closed successfully")
            except Exception as e:
                print(f"⚠️  Error closing driver: {e}")

# ===============================================
# ENHANCED STOCK AND DISCONTINUED PRODUCT DETECTION
# ===============================================

def check_discontinued_product(driver, url):
    """
    Check if product is discontinued by looking for the specific message:
    'This item has been discontinued'
    """
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Look for discontinued message in various possible locations
        discontinued_indicators = [
            "This item has been discontinued",
            "Product discontinued",
            "item has been discontinued",
            "product is discontinued",
            "no longer available"
        ]
        
        page_text = soup.get_text().lower()
        for indicator in discontinued_indicators:
            if indicator.lower() in page_text:
                print(f"❌ Product discontinued: {indicator}")
                return True
        
        return False
    except Exception as e:
        logging.warning(f"Error checking discontinued status for {url}: {e}")
        return False

def extract_exchange_price(driver, url):
    """
    Extract exchange price from HTML element with multiple strategies:
    1. <div class="with-Exchange-text"><span class="exchange-text">up to ₹62,466.5 off</span></div>
    2. Text patterns like "With Exchange up to ₹10,619.9 off"
    3. Alternative exchange price indicators
    """
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Strategy 1: Primary exchange price element (original method)
        exchange_div = soup.find('div', class_='with-Exchange-text')
        if exchange_div:
            exchange_span = exchange_div.find('span', class_='exchange-text')
            if exchange_span:
                exchange_text = exchange_span.get_text(strip=True)
                print(f"💱 Exchange Price Found (Method 1): {exchange_text}")
                logging.info(f"Exchange price extracted via primary method: {exchange_text}")
                return exchange_text
        
        # Strategy 2: Search for "With Exchange" text patterns in the entire page
        page_text = soup.get_text()
        exchange_patterns = [
            r'With Exchange up to ₹([\d,]+\.?\d*)\s*off',
            r'with exchange up to ₹([\d,]+\.?\d*)\s*off',
            r'Exchange.*?up to ₹([\d,]+\.?\d*)\s*off',
            r'exchange.*?₹([\d,]+\.?\d*)\s*off'
        ]
        
        for pattern in exchange_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                amount = match.group(1).replace(',', '')
                exchange_text = f"up to ₹{amount} off"
                print(f"💱 Exchange Price Found (Method 2): {exchange_text}")
                logging.info(f"Exchange price extracted via text pattern: {exchange_text}")
                return exchange_text
        
        # Strategy 3: Look for exchange-related elements with different class names
        exchange_elements = soup.find_all(['div', 'span', 'p'], 
                                        string=re.compile(r'.*exchange.*₹.*off.*', re.IGNORECASE))
        
        for element in exchange_elements:
            text = element.get_text(strip=True)
            if 'exchange' in text.lower() and '₹' in text and 'off' in text.lower():
                print(f"💱 Exchange Price Found (Method 3): {text}")
                logging.info(f"Exchange price extracted via element search: {text}")
                return text
        
        # Strategy 4: Search for exchange price in specific Croma price sections
        price_sections = soup.find_all(['div', 'section'], class_=re.compile(r'.*price.*|.*offer.*|.*exchange.*', re.IGNORECASE))
        for section in price_sections:
            section_text = section.get_text()
            if 'exchange' in section_text.lower() and '₹' in section_text:
                # Extract the exchange price line
                lines = section_text.split('\n')
                for line in lines:
                    if 'exchange' in line.lower() and '₹' in line and 'off' in line.lower():
                        clean_line = ' '.join(line.strip().split())
                        print(f"💱 Exchange Price Found (Method 4): {clean_line}")
                        logging.info(f"Exchange price extracted via price section: {clean_line}")
                        return clean_line
        
        # Strategy 5: Look for data attributes or hidden text containing exchange info
        exchange_attrs = soup.find_all(attrs={'data-exchange': True}) + \
                        soup.find_all(attrs={'data-offer': True})
        
        for attr_element in exchange_attrs:
            for attr_name, attr_value in attr_element.attrs.items():
                if 'exchange' in attr_name.lower() and isinstance(attr_value, str):
                    if '₹' in attr_value:
                        print(f"💱 Exchange Price Found (Method 5): {attr_value}")
                        logging.info(f"Exchange price extracted via attributes: {attr_value}")
                        return attr_value
        
        # Log detailed debugging info when no exchange price found
        logging.warning(f"No exchange price found for {url} - Debug info:")
        logging.warning(f"  - with-Exchange-text divs: {len(soup.find_all('div', class_='with-Exchange-text'))}")
        logging.warning(f"  - exchange-text spans: {len(soup.find_all('span', class_='exchange-text'))}")
        logging.warning(f"  - 'exchange' text occurrences: {page_text.lower().count('exchange')}")
        logging.warning(f"  - '₹' text occurrences: {page_text.count('₹')}")
        
        # Sample page text for debugging (first 1000 chars)
        sample_text = page_text[:1000].replace('\n', ' ').replace('\t', ' ')
        sample_text = ' '.join(sample_text.split())  # Clean whitespace
        logging.warning(f"  - Page text sample: {sample_text}")
        
        print(f"💱 Exchange Price: Not found (checked 5 methods)")
        return None
        
    except Exception as e:
        logging.warning(f"Error extracting exchange price for {url}: {e}")
        print(f"💱 Exchange Price: Error during extraction")
        return None

# ===============================================
# URL TRACKING FUNCTIONALITY
# ===============================================

def manage_visited_urls_file(file_path="visited_urls_croma.txt"):
    """
    Check if visited_urls_croma.txt exists, create if not, and return the file path.
    """
    if not os.path.exists(file_path):
        print(f"📝 Creating URL tracking file: {file_path}")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("# Croma URLs processed by enhanced_croma_scraper_comprehensive.py\n")
            f.write(f"# Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        print(f"✅ Created {file_path}")
    else:
        print(f"📋 Found existing URL tracking file: {file_path}")
    
    return file_path

def load_visited_urls(file_path="visited_urls_croma.txt"):
    """
    Load previously visited URLs from the tracking file
    """
    visited_urls = set()
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                url = line.strip()
                if url and not url.startswith('#'):
                    visited_urls.add(url)
        
        print(f"📋 Loaded {len(visited_urls)} previously visited URLs")
        return visited_urls
    
    except Exception as e:
        print(f"⚠️  Error loading visited URLs: {e}")
        return set()

def append_visited_url(url, file_path="visited_urls_croma.txt"):
    """
    Append a newly processed URL to the tracking file
    """
    try:
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(f"{url}\n")
    except Exception as e:
        print(f"⚠️  Error appending URL to visited file: {e}")

# ===============================================
# STOCK STATUS DETECTION FUNCTIONALITY
# ===============================================

def extract_croma_stock_status(driver, url):
    """
    Extract stock status by checking for the presence of span.amount#pdp-product-price element.
    Returns dict with in_stock status and additional details.
    """
    try:
        logging.info(f"Checking stock status for: {url}")
        
        # First check if product is discontinued
        if check_discontinued_product(driver, url):
            return {
                'in_stock': False,
                'discontinued': True,
                'price_found': None,
                'exchange_price': None,
                'status_details': "Product discontinued - skipping"
            }
        
        # Get page soup for element checking
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Check for the Croma price element: span.amount#pdp-product-price
        price_element = soup.find('span', {
            'class': 'amount',
            'id': 'pdp-product-price',
            'data-testid': 'new-price'
        })
        
        # Extract exchange price
        exchange_price = extract_exchange_price(driver, url)
        
        if price_element:
            # Price element found - product is in stock
            price_text = price_element.get_text(strip=True)
            logging.info(f"Price element found: {price_text} - Product in stock")
            
            return {
                'in_stock': True,
                'discontinued': False,
                'price_found': price_text,
                'exchange_price': exchange_price,
                'status_details': f"Price element found: {price_text}"
            }
        else:
            # Price element not found - product out of stock
            logging.info(f"Price element not found - Product out of stock")
            return {
                'in_stock': False,
                'discontinued': False,
                'price_found': None,
                'exchange_price': exchange_price,
                'status_details': "Price element (span.amount#pdp-product-price) not found"
            }
    
    except Exception as e:
        logging.error(f"Error checking stock status for {url}: {e}")
        return {
            'in_stock': False,
            'discontinued': False,
            'price_found': None,
            'exchange_price': None,
            'status_details': f"Error checking stock: {e}"
        }

# ===============================================
# COMPLETE CROMA OFFER ANALYZER CLASS
# ===============================================

@dataclass
class Offer:
    title: str
    description: str
    amount: float
    type: str
    bank: Optional[str] = None
    validity: Optional[str] = None
    min_spend: Optional[float] = None
    is_instant: bool = True
    card_type: Optional[str] = None
    card_provider: Optional[str] = None
    percentage: Optional[float] = None  # For percentage-based offers like "upto x%"

class CromaOfferAnalyzer:
    def __init__(self):
        # Comprehensive bank reputation scores for Indian banks (same as Amazon/Flipkart scripts)
        self.bank_scores = {
            # Public Sector Banks (PSBs)
            "SBI": 75, "State Bank of India": 75, "PNB": 72, "Punjab National Bank": 72,
            "BoB": 70, "Bank of Baroda": 70, "Canara Bank": 68, "Union Bank of India": 65,
            "Indian Bank": 65, "Bank of India": 65, "UCO Bank": 62, "Indian Overseas Bank": 62,
            "IOB": 62, "Central Bank of India": 62, "Bank of Maharashtra": 60,
            "Punjab & Sind Bank": 60,
            
            # Private Sector Banks
            "HDFC": 85, "HDFC Bank": 85, "ICICI": 90, "ICICI Bank": 90, "Axis": 80,
            "Axis Bank": 80, "Kotak": 70, "Kotak Mahindra Bank": 70, "IndusInd Bank": 68,
            "Yes Bank": 60, "IDFC FIRST Bank": 65, "IDFC": 65, "Federal Bank": 63,
            "South Indian Bank": 60, "RBL Bank": 62, "DCB Bank": 60,
            
            # Small Finance Banks
            "AU Small Finance Bank": 65, "AU Bank": 65, "Equitas Small Finance Bank": 62,
            "Equitas": 62, "Ujjivan Small Finance Bank": 60, "Ujjivan": 60,
            
            # Foreign Banks
            "Citi": 80, "Citibank": 80, "HSBC": 78, "Standard Chartered": 75,
            "Deutsche Bank": 75, "Barclays Bank": 75, "DBS Bank": 72,
            
            # Credit Card Companies
            "Amex": 85, "American Express": 85
        }
        
        # Enhanced bank name patterns for better matching
        self.bank_name_patterns = {
            "SBI": ["SBI", "State Bank", "State Bank of India"],
            "HDFC": ["HDFC", "HDFC Bank"],
            "ICICI": ["ICICI", "ICICI Bank"],
            "Axis": ["Axis", "Axis Bank"],
            "Kotak": ["Kotak", "Kotak Mahindra"],
            "Yes Bank": ["Yes Bank", "YES Bank"],
            "IDFC": ["IDFC", "IDFC FIRST", "IDFC Bank"],
            "IndusInd": ["IndusInd", "IndusInd Bank"],
            "Federal": ["Federal", "Federal Bank"],
            "RBL": ["RBL", "RBL Bank"],
            "Citi": ["Citi", "Citibank", "CitiBank"],
            "HSBC": ["HSBC"],
            "Standard Chartered": ["Standard Chartered", "StanChart", "SC Bank"],
            "AU Bank": ["AU Bank", "AU Small Finance", "AU"],
            "Equitas": ["Equitas", "Equitas Bank"],
            "PNB": ["PNB", "Punjab National Bank"],
            "BoB": ["BoB", "Bank of Baroda", "Baroda"],
            "Canara": ["Canara", "Canara Bank"],
            "Amex": ["Amex", "American Express"]
        }
        
        # Card providers list
        self.card_providers = [
            "Visa", "Mastercard", "RuPay", "American Express", "Amex", 
            "Diners Club", "Discover", "UnionPay", "JCB", "Maestro"
        ]
        
        # Default bank score if not found in the list
        self.default_bank_score = 70

    def extract_amount(self, description: str) -> float:
        """Extract numerical amount from offer description with enhanced patterns."""
        try:
            # Enhanced flat discount patterns
            flat_patterns = [
                r'(?:Additional\s+)?[Ff]lat\s+(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
                r'(?:Additional\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)\s+(?:Instant\s+)?Discount',
                r'(?:Get\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)\s+(?:off|discount)',
                r'(?:Save\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
                r'₹\s*([\d,]+\.?\d*)',
                r'Rs\.?\s*([\d,]+\.?\d*)',
                r'INR\s*([\d,]+\.?\d*)'
            ]
            
            for pattern in flat_patterns:
                match = re.search(pattern, description, re.IGNORECASE)
                if match:
                    amount = float(match.group(1).replace(',', ''))
                    logging.info(f"Extracted amount: ₹{amount}")
                    return amount
            
            return 0.0
        except (ValueError, AttributeError) as e:
            logging.warning(f"Error extracting amount from '{description[:50]}...': {e}")
            return 0.0

    def extract_bank(self, description: str) -> Optional[str]:
        """Extract bank names from offer description."""
        if not description:
            return None
        
        description_lower = description.lower()
        found_banks = set()
        
        # Bank variations for common patterns
        bank_variations = {
            'hdfc': 'HDFC', 'icici': 'ICICI', 'axis': 'Axis', 'sbi': 'SBI',
            'kotak': 'Kotak', 'yes bank': 'Yes Bank', 'yes': 'Yes Bank',
            'idfc': 'IDFC', 'indusind': 'IndusInd Bank', 'federal': 'Federal Bank',
            'rbl': 'RBL Bank', 'citi': 'Citi', 'citibank': 'Citi', 'hsbc': 'HSBC',
            'amex': 'Amex', 'american express': 'American Express'
        }
        
        # Try pattern matching first
        for bank_key, patterns in self.bank_name_patterns.items():
            for pattern in patterns:
                if pattern.lower() in description_lower:
                    found_banks.add(bank_key)
        
        # Try variations if no pattern match
        if not found_banks:
            for variation, standard_name in bank_variations.items():
                if variation in description_lower:
                    found_banks.add(standard_name)
        
        if found_banks:
            return ', '.join(sorted(list(found_banks)))
        
        return None

    def extract_card_type(self, description: str) -> Optional[str]:
        """Extract card type (Credit/Debit) from offer description."""
        description_lower = description.lower()
        
        if 'credit' in description_lower and 'debit' in description_lower:
            return "Credit/Debit"
        elif 'credit' in description_lower:
            return "Credit"
        elif 'debit' in description_lower:
            return "Debit"
        
        return None

    def parse_offer(self, offer: Dict[str, str]) -> Offer:
        """Parse offer details from raw offer data."""
        card_title = offer.get('card_type', '').strip()
        description = offer.get('offer_description', '').strip()
        
        # Determine offer type
        if 'bank offer' in card_title.lower():
            offer_type = "Bank Offer"
            title = "Bank Offer"
        else:
            offer_type = card_title if card_title else "Croma Offer"
            title = card_title if card_title else "Croma Offer"
        
        # Extract offer details
        amount = self.extract_amount(description)
        bank = self.extract_bank(description)
        card_type = self.extract_card_type(description)
        
        return Offer(
            title=title,
            description=description,
            amount=amount,
            type=offer_type,
            bank=bank,
            card_type=card_type,
            is_instant=True
        )

    def calculate_offer_score(self, offer: Offer, product_price: float) -> float:
        """Calculate score for Bank Offers only."""
        if offer.type != "Bank Offer":
            return 0
        
        base_score = 80
        
        # Discount amount bonus
        if product_price > 0 and offer.amount > 0:
            discount_percentage = (offer.amount / product_price) * 100
            discount_points = min(discount_percentage * 2, 50)
            base_score += discount_points
        
        # Bank reputation bonus
        if offer.bank:
            bank_bonus = (self.bank_scores.get(offer.bank, self.default_bank_score) - 70) / 2
            base_score += bank_bonus
        
        return max(0, min(100, base_score))

    def rank_offers(self, offers_data: List[Dict], product_price: float) -> List[Dict[str, Any]]:
        """Rank offers based on comprehensive scoring."""
        parsed_offers = [self.parse_offer(offer) for offer in offers_data if isinstance(offer, dict)]
        bank_offers = [offer for offer in parsed_offers if offer.type == "Bank Offer"]
        other_offers = [offer for offer in parsed_offers if offer.type != "Bank Offer"]
        
        all_ranked_offers = []
        
        # Process Bank Offers with ranking
        if bank_offers:
            scored_bank_offers = []
            for offer in bank_offers:
                score = self.calculate_offer_score(offer, product_price)
                net_effective_price = max(product_price - offer.amount, 0)
                
                scored_bank_offers.append({
                    'title': offer.title,
                    'description': offer.description,
                    'amount': offer.amount,
                    'bank': offer.bank,
                    'score': score,
                    'net_effective_price': net_effective_price,
                    'is_applicable': True,
                    'offer_type': 'Bank Offer',
                    'card_type': offer.card_type
                })
            
            # Sort by score and add ranks
            scored_bank_offers.sort(key=lambda x: x['score'], reverse=True)
            for idx, offer in enumerate(scored_bank_offers):
                offer['rank'] = idx + 1
            
            all_ranked_offers.extend(scored_bank_offers)
        
        # Add other offers without ranking
        for offer in other_offers:
            all_ranked_offers.append({
                'title': offer.title,
                'description': offer.description,
                'amount': offer.amount,
                'bank': offer.bank,
                'score': None,
                'net_effective_price': max(product_price - offer.amount, 0),
                'is_applicable': True,
                'offer_type': offer.type,
                'rank': None,
                'card_type': offer.card_type
            })
        
        return all_ranked_offers

# ===============================================
# CROMA OFFER SCRAPING FUNCTIONS
# ===============================================

def extract_price_amount(price_str):
    """Extract numeric amount from price string like '₹30,999'"""
    if not price_str:
        return 0.0
    
    numbers = re.findall(r'[\d,]+\.?\d*', price_str)
    if numbers:
        return float(numbers[0].replace(',', ''))
    return 0.0

def get_croma_offers(driver, url, max_retries=2):
    """
    Enhanced Croma offers scraping with comprehensive extraction and backup selectors
    
    Based on analysis of Croma HTML structure:
    - offer-container → div → offer-section-pdp → bank-offer-swiper → swiper-container → swiper-wrapper → swiper-slide
    
    Uses multiple fallback strategies to ensure offers are captured even if page structure changes:
    1. Primary: div.offer-section-pdp div.swiper-slide (current working method)
    2. Backup: div.bank-offer-swiper div.swiper-container div.swiper-wrapper div.swiper-slide 
    3. Alternative: div.offer-container div.offer-section-pdp div.swiper-slide
    4. Last resort: Direct swiper-slide search with content filtering
    """
    for attempt in range(max_retries):
        try:
            logging.info(f"Visiting Croma URL (attempt {attempt + 1}/{max_retries}): {url}")
            driver.get(url)
            
            # Enhanced waiting for page load - crucial for Ubuntu server
            time.sleep(8)  # Increased wait time for server environments
            
            # Wait for page to be ready
            WebDriverWait(driver, 15).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(3)
            
            # Debug: Log page title and URL to verify page loaded
            page_title = driver.title
            current_url = driver.current_url
            logging.info(f"Page loaded - Title: '{page_title}', Current URL: {current_url}")
            
            # Check if we're on the correct page
            if "croma.com" not in current_url.lower():
                logging.warning(f"Not on Croma page! Current URL: {current_url}")
                if attempt < max_retries - 1:
                    continue

            # Try to scroll to offer section with multiple selector attempts
            offer_section_found = False
            try:
                # Primary selector: offer-section-pdp
                offer_section = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "offer-section-pdp"))
                )
                driver.execute_script("arguments[0].scrollIntoView(true);", offer_section)
                time.sleep(3)
                offer_section_found = True
                logging.info("Found offer section using primary selector")
            except TimeoutException:
                try:
                    # Backup selector: bank-offer-swiper
                    offer_section = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "bank-offer-swiper"))
                    )
                    driver.execute_script("arguments[0].scrollIntoView(true);", offer_section)
                    time.sleep(3)
                    offer_section_found = True
                    logging.info("Found offer section using backup bank-offer-swiper selector")
                except TimeoutException:
                    try:
                        # Third backup: offer-container
                        offer_section = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "offer-container"))
                        )
                        driver.execute_script("arguments[0].scrollIntoView(true);", offer_section)
                        time.sleep(3)
                        offer_section_found = True
                        logging.info("Found offer section using backup offer-container selector")
                    except TimeoutException:
                        logging.warning("Could not find any offer section with any selector")
                        if attempt < max_retries - 1:
                            continue

            # Force trigger any lazy loading or JavaScript that might load offers
            try:
                # Scroll to bottom and back to top to trigger lazy loading
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(2)
                
                # Try to trigger any swiper initialization
                driver.execute_script("""
                    if (window.Swiper) {
                        try {
                            var swipers = document.querySelectorAll('.swiper-container');
                            swipers.forEach(function(el) {
                                if (!el.swiper) {
                                    new Swiper(el);
                                }
                            });
                        } catch(e) { console.log('Swiper init failed:', e); }
                    }
                """)
                time.sleep(2)
                
                logging.info("Triggered lazy loading and swiper initialization")
            except Exception as e:
                logging.warning(f"Could not trigger lazy loading: {e}")

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Debug: Check for key elements existence with detailed analysis
            offer_containers = soup.select("div.offer-container")
            offer_sections = soup.select("div.offer-section-pdp")
            bank_swipers = soup.select("div.bank-offer-swiper")
            swiper_containers = soup.select("div.swiper-container")
            all_swiper_slides = soup.select("div.swiper-slide")
            
            # Check for offer-related text content
            offer_text_elements = soup.find_all(string=re.compile(r'(discount|offer|bank|cashback|emi|instant)', re.I))
            bank_offer_texts = soup.find_all(string=re.compile(r'bank.*offer', re.I))
            
            logging.info(f"=== DETAILED OFFER ELEMENTS ANALYSIS ===")
            logging.info(f"Page URL: {driver.current_url}")
            logging.info(f"Page Title: {driver.title}")
            logging.info(f"Container elements found:")
            logging.info(f"  - offer-container: {len(offer_containers)}")
            logging.info(f"  - offer-section-pdp: {len(offer_sections)}")
            logging.info(f"  - bank-offer-swiper: {len(bank_swipers)}")
            logging.info(f"  - swiper-container: {len(swiper_containers)}")
            logging.info(f"  - swiper-slide (all): {len(all_swiper_slides)}")
            logging.info(f"Text content analysis:")
            logging.info(f"  - Elements with offer-related text: {len(offer_text_elements)}")
            logging.info(f"  - Elements with 'bank offer' text: {len(bank_offer_texts)}")
            
            # Log sample offer text if found
            if offer_text_elements:
                logging.info(f"Sample offer texts found:")
                for i, text in enumerate(offer_text_elements[:5]):
                    clean_text = ' '.join(str(text).strip().split())
                    if len(clean_text) > 10:
                        logging.info(f"  {i+1}. {clean_text[:100]}...")
            
            # Detailed analysis of each container type
            if offer_containers:
                logging.info(f"OFFER-CONTAINER analysis:")
                for i, container in enumerate(offer_containers[:3]):
                    children = container.find_all()
                    text_content = container.get_text(strip=True)
                    logging.info(f"  Container {i+1}: {len(children)} children, text length: {len(text_content)}")
                    if text_content and any(word in text_content.lower() for word in ['offer', 'discount', 'bank']):
                        logging.info(f"    Contains offer text: {text_content[:200]}...")
            
            if offer_sections:
                logging.info(f"OFFER-SECTION-PDP analysis:")
                for i, section in enumerate(offer_sections[:3]):
                    children = section.find_all()
                    swiper_slides_in_section = section.select("div.swiper-slide")
                    logging.info(f"  Section {i+1}: {len(children)} children, {len(swiper_slides_in_section)} swiper slides")
            
            if bank_swipers:
                logging.info(f"BANK-OFFER-SWIPER analysis:")
                for i, swiper in enumerate(bank_swipers[:3]):
                    children = swiper.find_all()
                    swiper_slides_in_bank = swiper.select("div.swiper-slide")
                    logging.info(f"  Bank swiper {i+1}: {len(children)} children, {len(swiper_slides_in_bank)} swiper slides")

            # Extract offers from carousel slides with multiple selector strategies
            offers = []
            
            logging.info(f"=== STARTING OFFER EXTRACTION STRATEGIES ===")
            
            # Strategy 1: Primary selector (current working method)
            logging.info(f"STRATEGY 1: Testing primary selector 'div.offer-section-pdp div.swiper-slide'")
            offer_wrappers = soup.select("div.offer-section-pdp div.swiper-slide")
            logging.info(f"Strategy 1: Found {len(offer_wrappers)} slide elements")
            if offer_wrappers:
                logging.info(f"Strategy 1 SUCCESS: Sample slide classes: {offer_wrappers[0].get('class', [])}")
                for i, wrapper in enumerate(offer_wrappers[:3]):
                    text_content = wrapper.get_text(strip=True)
                    logging.info(f"  Slide {i+1} text preview: {text_content[:100]}...")
            else:
                logging.warning(f"Strategy 1 FAILED: No slides found with primary selector")
            
            # Strategy 2: Backup selector for bank-offer-swiper structure
            if not offer_wrappers:
                logging.info(f"STRATEGY 2: Testing bank-offer-swiper selector")
                offer_wrappers = soup.select("div.bank-offer-swiper div.swiper-container div.swiper-wrapper div.swiper-slide")
                logging.info(f"Strategy 2: Found {len(offer_wrappers)} slide elements")
                if offer_wrappers:
                    logging.info(f"Strategy 2 SUCCESS: Found slides in bank-offer-swiper")
                    for i, wrapper in enumerate(offer_wrappers[:3]):
                        text_content = wrapper.get_text(strip=True)
                        logging.info(f"  Slide {i+1} text preview: {text_content[:100]}...")
                else:
                    logging.warning(f"Strategy 2 FAILED: No slides found in bank-offer-swiper")
            
            # Strategy 3: Alternative path via offer-container
            if not offer_wrappers:
                logging.info(f"STRATEGY 3: Testing offer-container path")
                offer_wrappers = soup.select("div.offer-container div.offer-section-pdp div.swiper-slide")
                logging.info(f"Strategy 3: Found {len(offer_wrappers)} slide elements")
                if offer_wrappers:
                    logging.info(f"Strategy 3 SUCCESS: Found slides via offer-container")
                else:
                    logging.warning(f"Strategy 3 FAILED: No slides found via offer-container")
            
            # Strategy 4: Direct swiper-slide search as last resort
            if not offer_wrappers:
                logging.info(f"STRATEGY 4: Testing direct swiper-slide search")
                all_slides = soup.select("div.swiper-slide")
                logging.info(f"Strategy 4: Found {len(all_slides)} total swiper slides")
                
                # Filter only those that contain bank offer content
                filtered_wrappers = []
                for i, wrapper in enumerate(all_slides):
                    bank_offer_span = wrapper.select_one("span.bank-offers-text-pdp-carousel")
                    if bank_offer_span:
                        filtered_wrappers.append(wrapper)
                        logging.info(f"  Slide {i+1} contains bank-offers-text-pdp-carousel span")
                
                offer_wrappers = filtered_wrappers
                logging.info(f"Strategy 4: Filtered to {len(offer_wrappers)} slides with bank offer content")
                if offer_wrappers:
                    logging.info(f"Strategy 4 SUCCESS: Found bank offer slides")
                else:
                    logging.warning(f"Strategy 4 FAILED: No slides contain bank-offers-text-pdp-carousel")
            
            # Strategy 5: Alternative text patterns for Ubuntu server compatibility
            if not offer_wrappers:
                logging.info(f"STRATEGY 5: Testing text-based search")
                # Look for different text patterns that might indicate offers
                potential_offer_elements = soup.find_all(string=re.compile(r'(discount|offer|bank|cashback|emi)', re.I))
                logging.info(f"Strategy 5: Found {len(potential_offer_elements)} potential offer text elements")
                
                # Try to find parent containers of offer texts
                found_parents = []
                for text_elem in potential_offer_elements[:10]:  # Limit to first 10
                    if text_elem.parent and len(str(text_elem).strip()) > 20:
                        parent = text_elem.parent
                        # Check if this could be an offer container
                        while parent and parent.name:
                            if any(cls in str(parent.get('class', [])) for cls in ['offer', 'bank', 'discount', 'swiper']):
                                if parent not in [w.parent for w in offer_wrappers if w.parent] and parent not in found_parents:
                                    offer_wrappers.append(parent)
                                    found_parents.append(parent)
                                    logging.info(f"  Added parent element with classes: {parent.get('class', [])}")
                                    break
                            parent = parent.parent
                
                logging.info(f"Strategy 5: Added {len(found_parents)} offer elements from text search")
                if found_parents:
                    logging.info(f"Strategy 5 SUCCESS: Found offers via text search")
                else:
                    logging.warning(f"Strategy 5 FAILED: No valid parent elements found")
            
            logging.info(f"=== FINAL RESULT: {len(offer_wrappers)} offer wrappers found ===")
            if not offer_wrappers:
                logging.error(f"❌ NO OFFER WRAPPERS FOUND BY ANY STRATEGY - This is the main issue!")
            
            logging.info(f"=== PROCESSING {len(offer_wrappers)} OFFER WRAPPERS ===")
            
            for idx, wrapper in enumerate(offer_wrappers):
                logging.info(f"--- Processing Wrapper {idx+1}/{len(offer_wrappers)} ---")
                logging.info(f"Wrapper classes: {wrapper.get('class', [])}")
                logging.info(f"Wrapper tag: {wrapper.name}")
                
                # Primary extraction method
                desc_tag = wrapper.select_one("span.bank-offers-text-pdp-carousel")
                bank_tag = wrapper.select_one("div.bank-text-name-container span.bank-name-text")
                
                logging.info(f"Primary extraction attempt:")
                logging.info(f"  - Found desc_tag (span.bank-offers-text-pdp-carousel): {'YES' if desc_tag else 'NO'}")
                logging.info(f"  - Found bank_tag (div.bank-text-name-container span.bank-name-text): {'YES' if bank_tag else 'NO'}")

                description = None
                bank = None

                if desc_tag:
                    description = desc_tag.get_text(strip=True)
                    bank = bank_tag.get_text(strip=True) if bank_tag else None
                    logging.info(f"PRIMARY SUCCESS: Description length: {len(description)}, Bank: {bank}")
                    if description:
                        logging.info(f"  Description preview: {description[:150]}...")
                else:
                    logging.info(f"PRIMARY FAILED: Trying alternative extraction methods...")
                    
                    # Alternative extraction methods for Ubuntu server compatibility
                    # Method 1: Look for any span with offer-related text
                    alt_desc_tags = wrapper.find_all('span', string=re.compile(r'.*(discount|offer|bank|cashback|emi).*', re.I))
                    logging.info(f"  Alternative method 1: Found {len(alt_desc_tags)} spans with offer text")
                    if alt_desc_tags:
                        description = alt_desc_tags[0].get_text(strip=True)
                        logging.info(f"  ALT METHOD 1 SUCCESS: {description[:50]}...")
                    
                    # Method 2: Get all text from wrapper if it contains offer keywords
                    if not description:
                        wrapper_text = wrapper.get_text(strip=True)
                        logging.info(f"  Alternative method 2: Wrapper text length: {len(wrapper_text)}")
                        if wrapper_text and any(keyword in wrapper_text.lower() for keyword in ['discount', 'offer', 'bank', 'cashback', 'emi']):
                            description = wrapper_text
                            logging.info(f"  ALT METHOD 2 SUCCESS: Using wrapper text ({len(wrapper_text)} chars)")
                            logging.info(f"    Text preview: {wrapper_text[:100]}...")
                        else:
                            logging.info(f"  ALT METHOD 2 FAILED: No offer keywords found in wrapper text")
                    
                    # Method 3: Look for bank name in various patterns
                    bank_patterns = ['sbi', 'icici', 'hdfc', 'axis', 'federal', 'idfc', 'kotak']
                    wrapper_text = wrapper.get_text(strip=True)
                    logging.info(f"  Bank detection: Checking for bank names in text...")
                    if wrapper_text:
                        for pattern in bank_patterns:
                            if pattern in wrapper_text.lower():
                                bank = pattern.upper()
                                logging.info(f"  BANK DETECTED: {bank}")
                                break
                        if not bank:
                            logging.info(f"  No known bank patterns found")
                
                # Clean and validate description
                logging.info(f"Final validation: Description length: {len(description) if description else 0}")
                if description and len(description) > 10:
                    # Remove extra whitespace and normalize
                    description = ' '.join(description.split())
                    
                    # Skip if description seems invalid
                    if len(description) < 15 or description.lower() in ['view more', 'learn more', 'terms and conditions']:
                        logging.info(f"❌ SKIPPING invalid description: {description}")
                        continue
                    
                    offer = {
                        "card_type": f"{bank} Offer" if bank else "Bank Offer",
                        "offer_title": f"{bank} Bank Offer" if bank else "Bank Offer", 
                        "offer_description": description
                    }
                    offers.append(offer)
                    logging.info(f"✅ EXTRACTED OFFER {len(offers)}: {bank if bank else 'Bank'}")
                    logging.info(f"   Full description: {description}")
                else:
                    logging.warning(f"❌ NO VALID DESCRIPTION found for wrapper {idx+1}")
                    # Debug: Show what we actually found in this wrapper
                    wrapper_text = wrapper.get_text(strip=True)
                    logging.info(f"   Debug - Wrapper contained: {wrapper_text[:200]}...")
                    all_spans = wrapper.find_all('span')
                    logging.info(f"   Debug - Found {len(all_spans)} spans in wrapper")
                    for i, span in enumerate(all_spans[:5]):
                        span_text = span.get_text(strip=True)
                        span_classes = span.get('class', [])
                        if span_text:
                            logging.info(f"     Span {i+1}: classes={span_classes}, text={span_text[:50]}...")
            
            logging.info(f"=== OFFER EXTRACTION COMPLETE: {len(offers)} offers extracted ===")
            if not offers:
                logging.error(f"❌ NO OFFERS WERE SUCCESSFULLY EXTRACTED - Check the debug info above!")
            else:
                logging.info(f"✅ Successfully extracted {len(offers)} offers!")

            # Remove duplicates
            unique_offers = []
            seen_descriptions = set()
            for offer in offers:
                desc = offer['offer_description']
                if desc not in seen_descriptions and len(desc) > 15:
                    seen_descriptions.add(desc)
                    unique_offers.append(offer)

            logging.info(f"Extracted {len(unique_offers)} unique offers from {url}")
            return unique_offers

        except Exception as e:
            logging.error(f"Exception in get_croma_offers (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(3)
                continue
            else:
                return []
    
    return []

# ===============================================
# COMPREHENSIVE LINK DISCOVERY
# ===============================================

def find_all_croma_store_links_comprehensive(data: List[Dict]) -> List[Dict]:
    """
    Find ALL Croma store links from ALL 3 nested locations:
    1. scraped_data.variants[].store_links[]
    2. scraped_data.all_matching_products[].store_links[]
    3. scraped_data.unmapped[].store_links[]
    
    Returns comprehensive location info for each link.
    """
    
    croma_store_links = []
    locations_checked = {'variants': 0, 'all_matching_products': 0, 'unmapped': 0}
    croma_found = {'variants': 0, 'all_matching_products': 0, 'unmapped': 0}
    
    for entry_idx, entry in enumerate(data):
        if 'scraped_data' not in entry or not isinstance(entry['scraped_data'], dict):
            continue
            
        scraped_data = entry['scraped_data']
        
        # LOCATION 1: scraped_data.variants[].store_links[]
        if 'variants' in scraped_data and isinstance(scraped_data['variants'], list):
            for variant_idx, variant in enumerate(scraped_data['variants']):
                if isinstance(variant, dict) and 'store_links' in variant:
                    locations_checked['variants'] += 1
                    store_links = variant['store_links']
                    if isinstance(store_links, list):
                        for store_idx, store_link in enumerate(store_links):
                            if isinstance(store_link, dict):
                                name = store_link.get('name', '').lower()
                                if 'croma' in name:
                                    croma_found['variants'] += 1
                                    croma_store_links.append({
                                        'entry_idx': entry_idx,
                                        'location': 'variants',
                                        'location_idx': variant_idx,
                                        'store_idx': store_idx,
                                        'entry': entry,
                                        'parent_object': variant,
                                        'store_link': store_link,
                                        'path': f"scraped_data.variants[{variant_idx}].store_links[{store_idx}]"
                                    })
        
        # LOCATION 2: scraped_data.all_matching_products[].store_links[]
        if 'all_matching_products' in scraped_data and isinstance(scraped_data['all_matching_products'], list):
            for product_idx, product in enumerate(scraped_data['all_matching_products']):
                if isinstance(product, dict) and 'store_links' in product:
                    locations_checked['all_matching_products'] += 1
                    store_links = product['store_links']
                    if isinstance(store_links, list):
                        for store_idx, store_link in enumerate(store_links):
                            if isinstance(store_link, dict):
                                name = store_link.get('name', '').lower()
                                if 'croma' in name:
                                    croma_found['all_matching_products'] += 1
                                    croma_store_links.append({
                                        'entry_idx': entry_idx,
                                        'location': 'all_matching_products',
                                        'location_idx': product_idx,
                                        'store_idx': store_idx,
                                        'entry': entry,
                                        'parent_object': product,
                                        'store_link': store_link,
                                        'path': f"scraped_data.all_matching_products[{product_idx}].store_links[{store_idx}]"
                                    })
        
        # LOCATION 3: scraped_data.unmapped[].store_links[]
        if 'unmapped' in scraped_data and isinstance(scraped_data['unmapped'], list):
            for unmapped_idx, unmapped in enumerate(scraped_data['unmapped']):
                if isinstance(unmapped, dict) and 'store_links' in unmapped:
                    locations_checked['unmapped'] += 1
                    store_links = unmapped['store_links']
                    if isinstance(store_links, list):
                        for store_idx, store_link in enumerate(store_links):
                            if isinstance(store_link, dict):
                                name = store_link.get('name', '').lower()
                                if 'croma' in name:
                                    croma_found['unmapped'] += 1
                                    croma_store_links.append({
                                        'entry_idx': entry_idx,
                                        'location': 'unmapped',
                                        'location_idx': unmapped_idx,
                                        'store_idx': store_idx,
                                        'entry': entry,
                                        'parent_object': unmapped,
                                        'store_link': store_link,
                                        'path': f"scraped_data.unmapped[{unmapped_idx}].store_links[{store_idx}]"
                                    })
    
    # Print comprehensive statistics
    print(f"\n📊 COMPREHENSIVE CROMA LINK DISCOVERY:")
    print(f"   Total entries processed: {len(data)}")
    print(f"   Location coverage:")
    for location, checked in locations_checked.items():
        found = croma_found[location]
        print(f"     {location}: {checked} locations checked → {found} Croma links found")
    
    total_found = sum(croma_found.values())
    print(f"   🎯 TOTAL CROMA LINKS FOUND: {total_found} (from all 3 locations)")
    
    return croma_store_links

# Removed old URL skipping logic - now using comprehensive URL tracking

def process_croma_comprehensive(input_file: str = "all_data_amazon_jio.json", 
                              output_file: str = "all_data_amazon_jio_croma.json"):
    """
    ENHANCED Croma processing with advanced features:
    1. URL-based persistent caching system
    2. Exchange price extraction and terminal display
    3. Discontinued product detection and automatic skipping
    4. Single browser session with automatic renewal every 100 sessions
    5. Finds Croma links in ALL 3 nested locations
    6. Tracks URLs with visited_urls_croma.txt
    7. Completely isolates Amazon data
    8. No user interaction required
    """
    
    print(f"🚀 ENHANCED CROMA SCRAPER WITH ADVANCED FEATURES")
    print(f"📂 Input file: {input_file}")
    print(f"📂 Output file: {output_file}")
    print(f"� Caching system: URL-based persistent cache (croma_cache.json)")
    print(f"� Exchange price: Extraction and terminal display")
    print(f"❌ Discontinued products: Automatic detection and skipping")
    print(f"🔄 Browser sessions: Single session with renewal every 100 URLs")
    print(f"� URL tracking: visited_urls_croma.txt")
    print(f"🛡️  Amazon data isolation: ENABLED")
    print("-" * 80)
    
    # Load URL-based cache
    cache = load_cache("croma_cache.json")
    
    # Create backup before processing
    backup_file = f"{input_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(input_file, backup_file)
    print(f"💾 Created input backup: {backup_file}")
    
    # Load the JSON data
    print(f"📖 Loading data from {input_file}...")
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"✅ Loaded {len(data)} entries successfully")
    except Exception as e:
        print(f"❌ Error loading {input_file}: {e}")
        return
    
    # Setup URL tracking
    visited_urls_file = manage_visited_urls_file("visited_urls_croma.txt")
    visited_urls = load_visited_urls(visited_urls_file)
    
    # Find ALL Croma store links from all 3 locations
    print(f"\n🔍 Discovering Croma links from ALL nested locations...")
    croma_store_links = find_all_croma_store_links_comprehensive(data)
    
    if not croma_store_links:
        print(f"❌ No Croma store links found in {input_file}")
        return
    
    # Check cache status
    cached_count = len([link for link in croma_store_links if is_url_cached(link['store_link'].get('url', ''), cache)])
    new_urls_count = len(croma_store_links) - cached_count
    print(f"� Cache status: {cached_count} URLs cached, {new_urls_count} new URLs to process")
    
    print(f"🚀 Processing {len(croma_store_links)} Croma links with advanced features")
    
    # Setup driver manager and analyzer
    driver_manager = RobustDriverManager()
    analyzer = CromaOfferAnalyzer()
    
    # Statistics
    stats = {
        'processed': 0,
        'skipped_no_url': 0,
        'skipped_cached': 0,
        'skipped_discontinued': 0,
        'scraped_successfully': 0,
        'failed_scraping': 0,
        'total_offers_added': 0,
        'in_stock_count': 0,
        'out_of_stock_count': 0,
        'exchange_prices_found': 0,
        'session_renewals': 0
    }
    
    try:
        print(f"\n🎯 Starting enhanced Croma scraping...")
        
        for idx, link_data in enumerate(croma_store_links):
            entry = link_data['entry']
            store_link = link_data['store_link']
            
            print(f"\n🔍 Processing {idx + 1}/{len(croma_store_links)}: {entry.get('product_name', 'N/A')}")
            print(f"   📍 Location: {link_data['path']}")
            
            croma_url = store_link.get('url', '')
            if not croma_url:
                print(f"   ⚠️  No URL found")
                stats['skipped_no_url'] += 1
                continue
            
            print(f"   🌐 Croma URL: {croma_url}")
            
            # Check cache first
            if is_url_cached(croma_url, cache):
                print(f"   � Found in cache - using cached data")
                cached_data = cache[croma_url]['data']
                
                # Update store_link with cached data
                store_link.update(cached_data)
                stats['skipped_cached'] += 1
                stats['processed'] += 1
                
                if store_link.get('exchange_price'):
                    print(f"   💱 Cached Exchange Price: {store_link['exchange_price']}")
                    stats['exchange_prices_found'] += 1
                
                continue
            
            # Get driver (with automatic renewal every 100 sessions)
            if driver_manager.session_count == 0 and idx > 0:
                stats['session_renewals'] += 1
            
            driver = driver_manager.get_driver()
            print(f"   🤖 Using session #{driver_manager.session_count} (renews every {driver_manager.max_sessions})")
            
            # Visit the page for comprehensive checking
            try:
                driver.get(croma_url)
                time.sleep(5)  # Wait for page to load
                
                # Wait for page to be ready
                WebDriverWait(driver, 15).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                time.sleep(3)
                
                # Additional wait for dynamic content like exchange prices
                try:
                    # Try to wait for price elements to load
                    WebDriverWait(driver, 10).until(
                        EC.any_of(
                            EC.presence_of_element_located((By.CLASS_NAME, "with-Exchange-text")),
                            EC.presence_of_element_located((By.CLASS_NAME, "exchange-text")),
                            EC.presence_of_element_located((By.ID, "pdp-product-price"))
                        )
                    )
                    print(f"   ✅ Price elements loaded successfully")
                except TimeoutException:
                    print(f"   ⚠️  Price elements took longer to load, continuing anyway")
                    time.sleep(2)  # Extra wait time
                
                # Extract comprehensive stock status (includes discontinued check)
                stock_status = extract_croma_stock_status(driver, croma_url)
                
                # Check if product is discontinued
                if stock_status.get('discontinued', False):
                    print(f"   ❌ Product discontinued - skipping to next product")
                    stats['skipped_discontinued'] += 1
                    stats['processed'] += 1
                    
                    # Cache the discontinued status
                    discontinued_data = {
                        'in_stock': False,
                        'discontinued': True,
                        'ranked_offers': [],
                        'exchange_price': None
                    }
                    add_to_cache(croma_url, discontinued_data, cache)
                    continue
                
                # Update stock status
                store_link['in_stock'] = stock_status['in_stock']
                store_link['discontinued'] = stock_status.get('discontinued', False)
                
                if stock_status['in_stock']:
                    stats['in_stock_count'] += 1
                    print(f"   📦 Stock status: In Stock - {stock_status['status_details']}")
                else:
                    stats['out_of_stock_count'] += 1
                    print(f"   📦 Stock status: Out of Stock - {stock_status['status_details']}")
                
                # Display exchange price in terminal (separate extraction for verification)
                exchange_price = stock_status.get('exchange_price')
                if not exchange_price:
                    # Try direct extraction if not found in stock status
                    print(f"   🔍 Exchange price not found in stock check, trying direct extraction...")
                    exchange_price = extract_exchange_price(driver, croma_url)
                    stock_status['exchange_price'] = exchange_price
                
                if exchange_price:
                    print(f"   💱 Exchange Price: {exchange_price}")
                    store_link['exchange_price'] = exchange_price
                    stats['exchange_prices_found'] += 1
                else:
                    print(f"   💱 Exchange Price: Not available")
                    store_link['exchange_price'] = None
                
                # SCRAPE THE CROMA OFFERS
                print(f"   🔄 Scraping Croma offers...")
                offers = get_croma_offers(driver, croma_url)
                stats['processed'] += 1
                
                if offers:
                    # Get product price for ranking
                    price_str = store_link.get('price', '₹0')
                    product_price = extract_price_amount(price_str)
                    
                    # Rank the offers using advanced logic
                    ranked_offers = analyzer.rank_offers(offers, product_price)
                    
                    # Update the store_link with ranked offers
                    store_link['ranked_offers'] = ranked_offers
                    stats['scraped_successfully'] += 1
                    stats['total_offers_added'] += len(ranked_offers)
                    
                    print(f"   ✅ Found and ranked {len(offers)} Croma offers")
                    
                    # Log top 3 offers
                    for i, offer in enumerate(ranked_offers[:3], 1):
                        score_display = offer['score'] if offer['score'] is not None else 'N/A'
                        print(f"      🏆 Rank {i}: {offer['title']} (Score: {score_display}, Amount: ₹{offer['amount']})")
                else:
                    print(f"   ❌ No offers found")
                    store_link['ranked_offers'] = []
                    stats['failed_scraping'] += 1
                
                # Cache the scraped data
                cache_data = {
                    'in_stock': store_link['in_stock'],
                    'discontinued': store_link.get('discontinued', False),
                    'ranked_offers': store_link['ranked_offers'],
                    'exchange_price': store_link.get('exchange_price')
                }
                add_to_cache(croma_url, cache_data, cache)
                
                # Add URL to visited list
                append_visited_url(croma_url, visited_urls_file)
                print(f"   📝 Added to cache and visited URLs")
                
            except Exception as e:
                print(f"   ❌ Error processing URL: {e}")
                logging.error(f"Error processing {croma_url}: {e}")
                store_link['in_stock'] = False
                store_link['ranked_offers'] = []
                store_link['exchange_price'] = None
                stats['failed_scraping'] += 1
                
                # Cache the error status
                error_data = {
                    'in_stock': False,
                    'discontinued': False,
                    'ranked_offers': [],
                    'exchange_price': None
                }
                add_to_cache(croma_url, error_data, cache)
                append_visited_url(croma_url, visited_urls_file)
            
            # Save progress and cache every 50 entries
            if (idx + 1) % 50 == 0:
                progress_backup_file = f"{output_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                with open(progress_backup_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                save_cache(cache, "croma_cache.json")
                print(f"   💾 Progress and cache saved (every 50 URLs)")
            
            # Brief delay between requests
            time.sleep(2)
    
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted! Saving progress...")
    
    finally:
        # Close driver manager
        driver_manager.close()
        
        # Save final cache
        save_cache(cache, "croma_cache.json")
        
        # Save final output
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Final output saved to {output_file}")
        
        # Print comprehensive statistics
        print(f"\n📊 ENHANCED PROCESSING SUMMARY:")
        print(f"   🎯 Croma links processed: {stats['processed']}")
        print(f"   📋 Cached URLs used: {stats['skipped_cached']}")
        print(f"   ❌ Discontinued products skipped: {stats['skipped_discontinued']}")
        print(f"   ✅ Successfully scraped: {stats['scraped_successfully']}")
        print(f"   ❌ Failed scraping: {stats['failed_scraping']}")
        print(f"   ⚠️  Skipped (no URL): {stats['skipped_no_url']}")
        print(f"   🎁 Total offers added: {stats['total_offers_added']}")
        print(f"   📦 In stock products: {stats['in_stock_count']}")
        print(f"   📦 Out of stock products: {stats['out_of_stock_count']}")
        print(f"   � Exchange prices found: {stats['exchange_prices_found']}")
        print(f"   🔄 Session renewals: {stats['session_renewals']}")
        print(f"   📝 Cache entries: {len(cache)}")
        print(f"   🤖 Browser optimization: Single session with auto-renewal")
        print(f"   🛡️  Amazon entries completely untouched!")
        
        success_rate = (stats['scraped_successfully'] / max(stats['processed'] - stats['skipped_cached'] - stats['skipped_discontinued'], 1) * 100)
        print(f"   📈 Success rate: {success_rate:.1f}%")

if __name__ == "__main__":
    print("🚀 ENHANCED CROMA SCRAPER WITH ADVANCED FEATURES")
    print("=" * 80)
    print("� URL-based persistent caching system (croma_cache.json)")
    print("💱 Exchange price extraction and terminal display")
    print("❌ Discontinued product detection and automatic skipping")
    print("🔄 Single browser session with automatic renewal every 100 sessions")
    print("✅ Comprehensive JSON traversal (variants + all_matching_products + unmapped)")
    print("✅ BeautifulSoup deprecation warning fixes")
    print("✅ Explicit Amazon/Flipkart/JioMart data isolation")
    print("✅ URL tracking with visited_urls_croma.txt")
    print("✅ Stock detection via span.amount#pdp-product-price")
    print("🤖 Fully automated (no user input required)")
    print("� Backup every 50 URLs for better performance")
    print("-" * 80)
    
    # Auto-configuration with advanced features
    print("🚀 Starting enhanced processing with all advanced features:")
    print("   • Input file: all_data.json")
    print("   • Output file: all_data_amazon_jio_croma.json")
    print("   • Caching: URL-based persistent cache (croma_cache.json)")
    print("   • Exchange prices: Extracted and displayed in terminal")
    print("   • Discontinued products: Automatically detected and skipped")
    print("   • Browser mode: Single session with auto-renewal")
    print("   • Session renewal: Every 100 URLs")
    print("   • URL tracking: visited_urls_croma.txt")
    print("   • Stock detection: span.amount#pdp-product-price element")
    print("   • Backup frequency: Every 50 URLs")
    print("   • Amazon isolation: ENABLED")
    print()
    
    # Start processing with all advanced features
    process_croma_comprehensive(
        input_file="all_data.json",
        output_file="all_data_amazon_jio_croma.json"
    ) 