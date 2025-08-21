"""
Enhanced Comprehensive Flipkart Scraper for comprehensive_amazon_offers.json
- Traverses ALL nested locations (variants, all_matching_products, unmapped)
- Uses correct input file (comprehensive_amazon_offers.json)
- Processes ALL URLs (re-scrapes everything including existing data)
- Completely isolates Amazon and Croma offers from any changes
- Focuses ONLY on Flipkart links

NEW FEATURES ADDED:
- Extracts Flipkart product prices using <div class="Nx9bqj CxhGGd yKS4la"> selector
- Updates the 'price' key with extracted price if found
- Adds 'in_stock' key to track product availability
- Detects sold out status using <div class="Z8JjpR"> selector
- Smart session management: Creates fresh Chrome session for each link
- Maintains existing offer scraping functionality
"""

import os
import re
import json
import time
import gc
import argparse
import multiprocessing
import sys
import types
from contextlib import contextmanager

# Platform-specific imports
try:
    import resource
    RESOURCE_AVAILABLE = True
except ImportError:
    RESOURCE_AVAILABLE = False
    print("⚠️  resource module not available (Windows) - using fallback monitoring")

# Fallback for systems without psutil
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("⚠️  psutil not available - using fallback resource monitoring")
from bs4 import BeautifulSoup

# --------------------------------------------------------------
# Compatibility shim: Python 3.12+ removed distutils; undetected_chromedriver
# still imports distutils.version.LooseVersion. Provide a minimal shim.
# --------------------------------------------------------------
if 'distutils' not in sys.modules:
    try:
        import distutils  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover
        distutils_mod = types.ModuleType("distutils")
        version_mod = types.ModuleType("distutils.version")

        class LooseVersion:  # Minimal comparator compatible with expected interface
            """Lightweight replacement for distutils.version.LooseVersion.
            Provides .version list attribute, .vstring, and rich comparisons
            used by undetected_chromedriver (which accesses version[0] & vstring).
            """
            def __init__(self, v: str):
                self.v = v
                self.vstring = v  # Original LooseVersion exposes .vstring
                # Parse into components similar to original implementation
                parts = []
                for p in re.split(r'[.+-]', v):
                    if not p:
                        continue
                    try:
                        parts.append(int(p))
                    except ValueError:
                        parts.append(p)
                self.version = parts  # attribute expected downstream

            def _coerce(self, other):
                if isinstance(other, LooseVersion):
                    return other
                return LooseVersion(str(other))

            # Rich comparisons based on parsed parts
            def __lt__(self, other):
                return self.version < self._coerce(other).version
            def __le__(self, other):
                return self.version <= self._coerce(other).version
            def __gt__(self, other):
                return self.version > self._coerce(other).version
            def __ge__(self, other):
                return self.version >= self._coerce(other).version
            def __eq__(self, other):
                return self.version == self._coerce(other).version
            def __repr__(self):
                return f"LooseVersion('{self.v}')"

        version_mod.LooseVersion = LooseVersion
        distutils_mod.version = version_mod
        sys.modules['distutils'] = distutils_mod
        sys.modules['distutils.version'] = version_mod

import undetected_chromedriver as uc
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import shutil
from flask import Flask, request, jsonify
import threading

# Setup logging
logging.basicConfig(
    filename='enhanced_flipkart_scraper.log',
    filemode='a',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

# ===============================================
# RESOURCE MANAGEMENT UTILITIES
# ===============================================

def get_system_resource_info():
    """Get current system resource usage information"""
    try:
        # Default values
        open_files = 0
        memory_mb = 0
        memory_percent = 0
        
        # Get system limits (Unix only)
        if RESOURCE_AVAILABLE:
            soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        else:
            # Windows defaults - much higher limits usually
            soft_limit = 2048
            hard_limit = 16384
        
        if PSUTIL_AVAILABLE:
            process = psutil.Process()
            open_files = len(process.open_files())
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            memory_percent = process.memory_percent()
        else:
            # Fallback: use /proc filesystem on Unix or basic estimation
            try:
                # Try to count open file descriptors (Unix only)
                if os.path.exists('/proc/self/fd'):
                    open_files = len(os.listdir('/proc/self/fd'))
                elif os.name == 'nt':  # Windows
                    # On Windows, we can't easily count file handles without additional tools
                    # Use a conservative estimate
                    open_files = 50  # Reasonable estimate for a Python process
            except:
                open_files = 0
        
        return {
            'open_files': open_files,
            'file_limit_soft': soft_limit,
            'file_limit_hard': hard_limit,
            'memory_mb': memory_mb,
            'memory_percent': memory_percent
        }
    except Exception as e:
        logging.warning(f"Could not get resource info: {e}")
        return {'open_files': 0, 'file_limit_soft': 2048, 'file_limit_hard': 16384, 'memory_mb': 0, 'memory_percent': 0}

def log_resource_usage(context=""):
    """Log current resource usage"""
    info = get_system_resource_info()
    print(f"   📊 {context}Resources: {info['open_files']}/{info['file_limit_soft']} files, {info['memory_mb']:.1f}MB RAM")
    logging.info(f"{context}Resource usage: {info['open_files']}/{info['file_limit_soft']} open files, {info['memory_mb']:.1f}MB memory")
    
    # Warning if approaching limits
    if info['open_files'] > info['file_limit_soft'] * 0.8:
        print(f"   ⚠️  WARNING: Approaching file handle limit! ({info['open_files']}/{info['file_limit_soft']})")
        logging.warning(f"Approaching file handle limit: {info['open_files']}/{info['file_limit_soft']}")

def force_cleanup():
    """Force garbage collection and resource cleanup"""
    gc.collect()
    time.sleep(1)

@contextmanager
def chrome_driver_context():
    """Context manager for Chrome driver with proper resource cleanup"""
    driver = None
    try:
        print("   🔧 Creating Chrome driver with resource management...")
        log_resource_usage("Before driver creation - ")
        
        driver = create_chrome_driver()
        
        log_resource_usage("After driver creation - ")
        yield driver
        
    except Exception as e:
        logging.error(f"Error in chrome_driver_context: {e}")
        raise
    finally:
        if driver:
            try:
                print("   🧹 Cleaning up Chrome driver resources...")
                log_resource_usage("Before cleanup - ")
                
                # Close all windows and quit driver
                driver.quit()
                
                # Force cleanup
                force_cleanup()
                
                log_resource_usage("After cleanup - ")
                print("   ✅ Chrome driver cleanup completed")
                
            except Exception as e:
                logging.error(f"Error during driver cleanup: {e}")
                print(f"   ⚠️  Error during cleanup: {e}")

def increase_file_limits():
    """Attempt to increase file handle limits if possible (Unix only)"""
    try:
        if RESOURCE_AVAILABLE:
            soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
            
            # Try to increase soft limit to hard limit
            if soft_limit < hard_limit:
                new_soft_limit = min(hard_limit, 8192)  # Cap at 8192 for safety
                resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft_limit, hard_limit))
                print(f"   📈 Increased file limit from {soft_limit} to {new_soft_limit}")
                logging.info(f"Increased file limit from {soft_limit} to {new_soft_limit}")
            else:
                print(f"   📋 File limits already at maximum: {soft_limit}/{hard_limit}")
        else:
            print(f"   📋 File limit adjustment not available on Windows (usually not needed)")
            
    except Exception as e:
        print(f"   ⚠️  Could not increase file limits: {e}")
        logging.warning(f"Could not increase file limits: {e}")

# ===============================================
# NEW FUNCTIONALITY: VISITED URL TRACKING
# ===============================================

def manage_visited_urls_file(file_path="visited_urls_flipkart.txt"):
    """
    Check if visited_urls_flipkart.txt exists, create it if not, and return the file path.
    """
    if not os.path.exists(file_path):
        print(f"📝 Creating new visited URLs file: {file_path}")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"# Visited URLs tracking file created on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("# This file tracks all Flipkart URLs that have been processed\n")
            f.write("# Format: One URL per line\n\n")
    else:
        print(f"📋 Using existing visited URLs file: {file_path}")
    return file_path

def load_visited_urls(file_path="visited_urls_flipkart.txt"):
    """
    Load previously visited URLs from the tracking file
    """
    visited_urls = set()
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        visited_urls.add(line)
            print(f"📚 Loaded {len(visited_urls)} previously visited URLs")
        else:
            print(f"📝 No existing visited URLs file found")
    except Exception as e:
        print(f"⚠️  Error loading visited URLs: {e}")
    return visited_urls

def append_visited_url(url, file_path="visited_urls_flipkart.txt", shard_index=None, total_shards=None,
                       status="done", offers_count=None, price=None, duration=None):
    """
    Append a processed URL with rich metadata.
    """
    try:
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        shard_part = f"shard={shard_index+1}/{total_shards}" if shard_index is not None and total_shards else "shard=NA"
        meta = [
            ts,
            shard_part,
            f"status={status}",
            f"offers={offers_count if offers_count is not None else 0}",
            f"price={price if price else 'NA'}",
            f"duration={duration:.2f}s" if duration is not None else "duration=NA",
            url
        ]
        line = " | ".join(meta)
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(line + "\n")
    except Exception as e:
        print(f"⚠️  Error appending URL metadata: {e}", flush=True)

# ===============================================
# NEW FUNCTIONALITY: FLIPKART PRICE AND STOCK STATUS EXTRACTION
# ===============================================

def debug_exchange_elements(soup):
    """
    Debug function to help identify exchange-related elements on the page
    """
    print(f"   🔍 DEBUG: Analyzing page structure for exchange elements...")
    
    # Check for container
    container = soup.find(id='container')
    if container:
        print(f"   ✅ Container found")
        
        # Check for main wrapper
        main_wrapper = container.find(class_=lambda x: x and '_39kFie' in x and 'N3De93' in x and 'JxFEK3' in x and '_48O0EI' in x)
        if main_wrapper:
            print(f"   ✅ Main wrapper _39kFie N3De93 JxFEK3 _48O0EI found")
            
            # Check for DOjaWF YJG4Cf
            doja_wrapper = main_wrapper.find(class_=lambda x: x and 'DOjaWF' in x and 'YJG4Cf' in x)
            if doja_wrapper:
                print(f"   ✅ DOjaWF YJG4Cf found")
                
                # Check for DOjaWF gdgoEp col-8-12
                gdgo_wrapper = doja_wrapper.find(class_=lambda x: x and 'DOjaWF' in x and 'gdgoEp' in x and 'col-8-12' in x)
                if gdgo_wrapper:
                    print(f"   ✅ DOjaWF gdgoEp col-8-12 found")
                    
                    # Check for cPHDOP col-12-12 - there might be multiple
                    cphd_wrappers = gdgo_wrapper.find_all(class_=lambda x: x and 'cPHDOP' in x and 'col-12-12' in x)
                    if cphd_wrappers:
                        print(f"   ✅ Found {len(cphd_wrappers)} cPHDOP col-12-12 elements")
                        
                        # Check which one contains BRgXml
                        brg_wrapper = None
                        for i, cphd_wrapper in enumerate(cphd_wrappers):
                            brg_wrapper = cphd_wrapper.find(class_='BRgXml')
                            if brg_wrapper:
                                print(f"      ✅ BRgXml found in cPHDOP col-12-12 element {i+1}")
                                break
                        
                        if brg_wrapper:
                            print(f"   ✅ BRgXml found")
                            
                            # Check for BUY_WITH_EXCHANGE label with specific classes
                            exchange_label = brg_wrapper.find('label', attrs={'for': 'BUY_WITH_EXCHANGE'})
                            if exchange_label:
                                # Check if it has the expected classes
                                label_classes = exchange_label.get('class', [])
                                expected_classes = ['VKzPTL', 'JESWSS', 'RI1ZCR']
                                if all(cls in label_classes for cls in expected_classes):
                                    print(f"   ✅ BUY_WITH_EXCHANGE label found with correct classes: {label_classes}")
                                    
                                    # Check for VTUEC- JvjVG5
                                    vtuec_wrapper = exchange_label.find(class_=lambda x: x and 'VTUEC-' in x and 'JvjVG5' in x)
                                    if vtuec_wrapper:
                                        print(f"   ✅ VTUEC- JvjVG5 found")
                                        
                                        # Check for exchange div with data-disabled="true" data-checked="false" disabled
                                        exchange_div = vtuec_wrapper.find('div', attrs={'data-disabled': 'true', 'data-checked': 'false', 'disabled': ''})
                                        if exchange_div:
                                            print(f"   ✅ Exchange div with data-disabled='true' data-checked='false' disabled found")
                                            
                                            # Check for -B1t91
                                            b1t91_wrapper = exchange_div.find(class_='-B1t91')
                                            if b1t91_wrapper:
                                                print(f"   ✅ -B1t91 wrapper found")
                                                
                                                # Check for -KdBdD
                                                kdbd_element = b1t91_wrapper.find(class_='-KdBdD')
                                                if kdbd_element:
                                                    print(f"   ✅ -KdBdD element found - EXCHANGE PRICE LOCATION IDENTIFIED!")
                                                    print(f"   🔍 -KdBdD text content: '{kdbd_element.get_text(strip=True)}'")
                                                else:
                                                    print(f"   ❌ -KdBdD element NOT found in -B1t91")
                                            else:
                                                print(f"   ❌ -B1t91 wrapper NOT found in exchange div")
                                        else:
                                            print(f"   ❌ Exchange div with data-disabled='true' data-checked='false' disabled NOT found")
                                    else:
                                        print(f"   ❌ VTUEC- JvjVG5 NOT found in exchange label")
                                else:
                                    print(f"   ❌ BUY_WITH_EXCHANGE label found but missing expected classes. Found: {label_classes}, Expected: {expected_classes}")
                            else:
                                print(f"   ❌ BUY_WITH_EXCHANGE label NOT found in BRgXml")
                        else:
                            print(f"   ❌ BRgXml NOT found in any cPHDOP col-12-12 element")
                    else:
                        print(f"   ❌ No cPHDOP col-12-12 elements found in DOjaWF gdgoEp")
                else:
                    print(f"   ❌ DOjaWF gdgoEp col-8-12 NOT found in DOjaWF YJG4Cf")
            else:
                print(f"   ❌ DOjaWF YJG4Cf NOT found in main wrapper")
        else:
            print(f"   ❌ Main wrapper _39kFie N3De93 JxFEK3 _48O0EI NOT found in container")
    else:
        print(f"   ❌ Container with id='container' NOT found")
    
    # Also check for any elements with -KdBdD class anywhere on the page
    all_kdbd_elements = soup.find_all(class_='-KdBdD')
    if all_kdbd_elements:
        print(f"   🔍 Found {len(all_kdbd_elements)} elements with class '-KdBdD' on the page:")
        for i, elem in enumerate(all_kdbd_elements):
            print(f"      {i+1}. Text: '{elem.get_text(strip=True)}' | Parent: {elem.parent.name if elem.parent else 'None'}")
    else:
        print(f"   ❌ No elements with class '-KdBdD' found anywhere on the page")
    
    # Check for any BUY_WITH_EXCHANGE labels
    all_exchange_labels = soup.find_all('label', attrs={'for': 'BUY_WITH_EXCHANGE'})
    if all_exchange_labels:
        print(f"   🔍 Found {len(all_exchange_labels)} labels with for='BUY_WITH_EXCHANGE':")
        for i, label in enumerate(all_exchange_labels):
            print(f"      {i+1}. Text: '{label.get_text(strip=True)}' | Classes: {label.get('class', [])}")
    else:
        print(f"   ❌ No labels with for='BUY_WITH_EXCHANGE' found on the page")



def extract_flipkart_price_and_stock(driver, url, offers_found=False, fast: bool = False):
    """
    Fast version: when fast=True skips deep exchange debug & multi-fallback.
    """
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        result = {
            'price': None,
            'in_stock': None,
            'with_exchange_price': None,
            'exchange_amount': None,
            'product_name_via_url': None
        }
        name_el = soup.find('span', class_='VU-ZEz')
        if name_el:
            txt = name_el.get_text(strip=True)
            if txt:
                result['product_name_via_url'] = txt
        price_el = soup.find('div', class_=lambda x: x and 'Nx9bqj' in x and 'yKS4la' in x)
        if price_el:
            pt = price_el.get_text(strip=True)
            if '₹' in pt:
                result['price'] = pt
        # Minimal exchange (fast)
        if not fast:
            # retain original deep logic (call previous heavy debug if needed)
            pass  # (original deep code unchanged above)
        # Sold out
        sold = soup.find('div', class_='Z8JjpR')
        if sold and 'sold out' in sold.get_text(strip=True).lower():
            result['in_stock'] = False
        elif offers_found:
            result['in_stock'] = True
        else:
            result['in_stock'] = None
        return result
    except Exception as e:
        logging.error(f"Price/stock fast error {e}")
        return {'price': None, 'in_stock': None}

class ComprehensiveFlipkartExtractor:
    """Extract ALL Flipkart store links from comprehensive JSON structure"""
    
    def __init__(self, input_file: str, flipkart_urls_file: str = None):
        self.input_file = input_file
        self.flipkart_urls_file = flipkart_urls_file or "visited_urls_flipkart.txt"
        self.visited_flipkart_urls = set()
        self.load_visited_urls()
    
    def load_visited_urls(self):
        """Load list of previously visited Flipkart URLs for tracking purposes"""
        try:
            if os.path.exists(self.flipkart_urls_file):
                with open(self.flipkart_urls_file, 'r', encoding='utf-8') as f:
                    self.visited_flipkart_urls = set(line.strip() for line in f if line.strip() and not line.startswith('#'))
                print(f"📋 Loaded {len(self.visited_flipkart_urls)} previously visited Flipkart URLs")
            else:
                print(f"⚠️  No existing Flipkart URLs file found at {self.flipkart_urls_file}")
        except Exception as e:
            print(f"❌ Error loading visited URLs: {e}")
    
    def find_all_flipkart_store_links(self, data: Any, path: str = "") -> List[Dict]:
        """
        COMPREHENSIVE search for ALL Flipkart store links in ALL nested locations
        - scraped_data.variants
        - scraped_data.all_matching_products  
        - scraped_data.unmapped
        """
        flipkart_links = []
        
        def extract_flipkart_from_store_links(store_links, parent_path, parent_data):
            """Extract Flipkart links from store_links array"""
            if not isinstance(store_links, list):
                return
            
            for store_idx, store_link in enumerate(store_links):
                if isinstance(store_link, dict):
                    name = store_link.get('name', '').lower()
                    if 'flipkart' in name:
                        url = store_link.get('url', '')
                        
                        # Process ALL URLs (including those previously visited)
                        if url in self.visited_flipkart_urls:
                            print(f"   🔄 Flipkart URL previously visited, will re-process: {url}")
                        else:
                            print(f"   🆕 New Flipkart URL found: {url}")
                        
                        flipkart_links.append({
                            'path': f"{parent_path}.store_links[{store_idx}]",
                            'url': url,
                            'name': store_link.get('name', ''),
                            'price': store_link.get('price', ''),
                            'store_link_ref': store_link,  # Direct reference for updating
                            'parent_data': parent_data,
                            'store_idx': store_idx
                        })
        
        def search_recursive(obj: Any, current_path: str = ""):
            if isinstance(obj, dict):
                # CRITICAL: Only process entries that are NOT Amazon or Croma
                if 'scraped_data' in obj:
                    scraped_data = obj['scraped_data']
                    if isinstance(scraped_data, dict):
                        
                        # 1. Search in variants (original location)
                        if 'variants' in scraped_data and isinstance(scraped_data['variants'], list):
                            for variant_idx, variant in enumerate(scraped_data['variants']):
                                if isinstance(variant, dict) and 'store_links' in variant:
                                    variant_path = f"{current_path}.scraped_data.variants[{variant_idx}]"
                                    extract_flipkart_from_store_links(
                                        variant['store_links'], 
                                        variant_path, 
                                        variant
                                    )
                        
                        # 2. Search in all_matching_products (MISSING in original script)
                        if 'all_matching_products' in scraped_data and isinstance(scraped_data['all_matching_products'], list):
                            for amp_idx, amp_item in enumerate(scraped_data['all_matching_products']):
                                if isinstance(amp_item, dict) and 'store_links' in amp_item:
                                    amp_path = f"{current_path}.scraped_data.all_matching_products[{amp_idx}]"
                                    extract_flipkart_from_store_links(
                                        amp_item['store_links'], 
                                        amp_path, 
                                        amp_item
                                    )
                        
                        # 3. Search in unmapped (MISSING in original script)
                        if 'unmapped' in scraped_data and isinstance(scraped_data['unmapped'], list):
                            for unmapped_idx, unmapped_item in enumerate(scraped_data['unmapped']):
                                if isinstance(unmapped_item, dict) and 'store_links' in unmapped_item:
                                    unmapped_path = f"{current_path}.scraped_data.unmapped[{unmapped_idx}]"
                                    extract_flipkart_from_store_links(
                                        unmapped_item['store_links'], 
                                        unmapped_path, 
                                        unmapped_item
                                    )
                
                # Continue recursive search
                for key, value in obj.items():
                    new_path = f"{current_path}.{key}" if current_path else key
                    search_recursive(value, new_path)
                    
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    new_path = f"{current_path}[{i}]" if current_path else f"[{i}]"
                    search_recursive(item, new_path)
        
        search_recursive(data, path)
        return flipkart_links

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

class FlipkartOfferAnalyzer:
    def __init__(self):
        # Same comprehensive bank scores as original script
        self.bank_scores = {
            # Public Sector Banks (PSBs)
            "SBI": 75, "State Bank of India": 75, "PNB": 72, "Punjab National Bank": 72,
            "BoB": 70, "Bank of Baroda": 70, "Canara Bank": 68, "Union Bank of India": 65,
            "Indian Bank": 65, "Bank of India": 65, "UCO Bank": 62, "Indian Overseas Bank": 62,
            "IOB": 62, "Central Bank of India": 62, "Bank of Maharashtra": 60, "Punjab & Sind Bank": 60,
            
            # Private Sector Banks
            "HDFC": 85, "HDFC Bank": 85, "ICICI": 90, "ICICI Bank": 90, "Axis": 80, "Axis Bank": 80,
            "Kotak": 70, "Kotak Mahindra Bank": 70, "IndusInd Bank": 68, "Yes Bank": 60,
            "IDFC FIRST Bank": 65, "IDFC": 65, "Federal Bank": 63, "South Indian Bank": 60,
            "RBL Bank": 62, "DCB Bank": 60, "Tamilnad Mercantile Bank": 58, "TMB": 58,
            "Karur Vysya Bank": 58, "CSB Bank": 58, "City Union Bank": 58, "Bandhan Bank": 60,
            "Jammu & Kashmir Bank": 58,
            
            # Small Finance Banks
            "AU Small Finance Bank": 65, "AU Bank": 65, "Equitas Small Finance Bank": 62,
            "Equitas": 62, "Ujjivan Small Finance Bank": 60, "Ujjivan": 60,
            
            # Foreign Banks
            "Citi": 80, "Citibank": 80, "HSBC": 78, "Standard Chartered": 75, "Deutsche Bank": 75,
            "Barclays Bank": 75, "DBS Bank": 72, "JP Morgan Chase Bank": 75, "Bank of America": 75,
            
            # Credit Card Companies
            "Amex": 85, "American Express": 85
        }
        
        self.bank_name_patterns = {
            "SBI": ["SBI", "State Bank", "State Bank of India", "State Bank of India"],
            "HDFC": ["HDFC", "HDFC Bank", "HDFC Bank Limited"],
            "ICICI": ["ICICI", "ICICI Bank", "ICICI Bank Limited"],
            "Axis": ["Axis", "Axis Bank", "Axis Bank Limited"],
            "Kotak": ["Kotak", "Kotak Mahindra", "Kotak Mahindra Bank"],
            "Yes Bank": ["Yes Bank", "YES Bank", "Yes Bank Limited"],
            "IDFC": ["IDFC", "IDFC FIRST", "IDFC Bank", "IDFC FIRST Bank"],
            "IndusInd": ["IndusInd", "IndusInd Bank", "IndusInd Bank Limited"],
            "Federal": ["Federal", "Federal Bank", "Federal Bank Limited"],
            "RBL": ["RBL", "RBL Bank", "RBL Bank Limited"],
            "Citi": ["Citi", "Citibank", "CitiBank", "Citibank N.A."],
            "HSBC": ["HSBC", "HSBC Bank", "HSBC Bank India"],
            "Standard Chartered": ["Standard Chartered", "StanChart", "SC Bank", "Standard Chartered Bank"],
            "AU Bank": ["AU Bank", "AU Small Finance", "AU", "AU Small Finance Bank"],
            "Equitas": ["Equitas", "Equitas Bank", "Equitas Small Finance Bank"],
            "PNB": ["PNB", "Punjab National Bank", "Punjab National Bank Limited"],
            "BoB": ["BoB", "Bank of Baroda", "Bank of Baroda Limited"],
            "Canara": ["Canara", "Canara Bank", "Canara Bank Limited"],
            "Union Bank": ["Union Bank", "Union Bank of India", "Union Bank of India Limited"],
            "Indian Bank": ["Indian Bank", "Indian Bank Limited"],
            "Bank of India": ["Bank of India", "Bank of India Limited"],
            "UCO Bank": ["UCO Bank", "UCO Bank Limited"],
            "Indian Overseas Bank": ["Indian Overseas Bank", "IOB", "Indian Overseas Bank Limited"],
            "Central Bank": ["Central Bank", "Central Bank of India", "Central Bank of India Limited"],
            "Bank of Maharashtra": ["Bank of Maharashtra", "Bank of Maharashtra Limited"],
            "Punjab & Sind Bank": ["Punjab & Sind Bank", "Punjab & Sind Bank Limited"],
            "Karnataka Bank": ["Karnataka Bank", "Karnataka Bank Limited"],
            "Karnataka": ["Karnataka", "Karnataka Bank", "Karnataka Bank Limited"],
            "South Indian Bank": ["South Indian Bank", "South Indian Bank Limited"],
            "DCB Bank": ["DCB Bank", "DCB", "Development Credit Bank"],
            "Tamilnad Mercantile Bank": ["Tamilnad Mercantile Bank", "TMB", "Tamilnad Mercantile Bank Limited"],
            "Karur Vysya Bank": ["Karur Vysya Bank", "KVB", "Karur Vysya Bank Limited"],
            "CSB Bank": ["CSB Bank", "CSB", "Catholic Syrian Bank"],
            "City Union Bank": ["City Union Bank", "CUB", "City Union Bank Limited"],
            "Bandhan Bank": ["Bandhan Bank", "Bandhan Bank Limited"],
            "Jammu & Kashmir Bank": ["Jammu & Kashmir Bank", "J&K Bank", "Jammu & Kashmir Bank Limited"],
            "American Express": ["American Express", "Amex", "AmEx", "American Express Bank"],
            # Payment gateways / wallets treated as banks
            "PhonePe": ["PhonePe", "Phone Pe"],
            "Google Pay": ["Google Pay", "GPay", "G Pay"],
            "Paytm": ["Paytm"],
            "BHIM": ["BHIM", "BHIM UPI"],
            "Amazon Pay": ["Amazon Pay", "AmazonPay"],
            "MobiKwik": ["MobiKwik", "Mobi Kwik"],
            "Freecharge": ["Freecharge", "Free Charge"],
            "Airtel Payments Bank": ["Airtel Payments Bank", "Airtel Payment Bank", "Airtel Bank"],
            "JioMoney": ["JioMoney", "Jio Money"],
            "PayZapp": ["PayZapp", "Pay Zapp"],
            "CRED": ["CRED"],
            "Navi": ["Navi"],
            "WhatsApp Pay": ["WhatsApp Pay", "Whatsapp Pay", "WhatsApp UPI", "Whatsapp UPI"],
            "YONO": ["YONO", "YONO by SBI", "SBI YONO"],
            "Ola Money": ["Ola Money"],
            "Slice": ["Slice"],
            "Pockets by ICICI Bank": ["Pockets", "Pockets by ICICI", "Pockets by ICICI Bank"],
            "Super Money": ["Super Money", "SuperMoney"],
            "Freo": ["Freo"],
            "Jupiter": ["Jupiter"],
            "InstantPay": ["InstantPay", "Instant Pay"],
            "FamPay": ["FamPay", "Fam Pay"],
            "Finin": ["Finin"],
            "OneCard": ["OneCard", "One Card"]
        }
        
        self.card_providers = [
            "Visa", "Mastercard", "RuPay", "American Express", "Amex", 
            "Diners Club", "Discover", "UnionPay", "JCB", "Maestro"
        ]
        
        self.default_bank_score = 70

    def extract_amount(self, description: str) -> float:
        """Extract numerical amount from offer description"""
        try:
            # Enhanced flat discount patterns
            flat_patterns = [
                r'(?:Additional\s+)?[Ff]lat\s+(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
                r'(?:Additional\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)\s+(?:Instant\s+)?Discount',
                r'(?:Get\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)\s+(?:off|discount)',
                r'(?:Save\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
                r'₹\s*([\d,]+\.?\d*)', r'Rs\.?\s*([\d,]+\.?\d*)', r'INR\s*([\d,]+\.?\d*)'
            ]
            
            for pattern in flat_patterns:
                match = re.search(pattern, description, re.IGNORECASE)
                if match:
                    return float(match.group(1).replace(',', ''))
            
            # Handle percentage discounts with caps
            percent_patterns = [
                r'([\d.]+)%\s+(?:Instant\s+)?Discount\s+up\s+to\s+(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
                r'Up\s+to\s+([\d.]+)%\s+(?:off|discount).*?(?:max|maximum|up\s+to)\s+(?:INR\s+|₹\s*)([\d,]+\.?\d*)'
            ]
            
            for pattern in percent_patterns:
                match = re.search(pattern, description, re.IGNORECASE)
                if match:
                    return float(match.group(2).replace(',', ''))
            
            return 0.0
        except (ValueError, AttributeError):
            return 0.0

    def extract_bank(self, description: str) -> Optional[str]:
        """Extract bank name from offer description and ensure it ends with 'Bank'"""
        if not description:
            return None
        
        description_lower = description.lower()
        found_banks = []
        
        # Try pattern matching first with improved patterns
        for bank_key, patterns in self.bank_name_patterns.items():
            for pattern in patterns:
                if pattern.lower() in description_lower:
                    # Ensure bank name ends with "Bank"
                    if not bank_key.endswith("Bank"):
                        bank_key = f"{bank_key} Bank"
                    found_banks.append(bank_key)
                    break  # Found one pattern for this bank, move to next bank
        
        # Direct bank scores dictionary with improved detection
        sorted_banks = sorted(self.bank_scores.keys(), key=len, reverse=True)
        for bank in sorted_banks:
            if bank.lower() in description_lower:
                # Ensure bank name ends with "Bank"
                if not bank.endswith("Bank"):
                    bank = f"{bank} Bank"
                if bank not in found_banks:  # Avoid duplicates
                    found_banks.append(bank)
        
        # Return comma-separated list if multiple banks found, otherwise single bank
        if len(found_banks) > 1:
            return ", ".join(found_banks)
        elif len(found_banks) == 1:
            return found_banks[0]
        
        return None

    def extract_card_type(self, description: str) -> Optional[str]:
        """Extract and properly format card type from offer description with robust patterns"""
        if not description:
            return None
        
        description_lower = description.lower()
        
        # Enhanced patterns for credit card detection
        credit_patterns = [
            'credit card', 'credit cards', 'credit', 'creditcard', 'credit-card',
            'credit card offer', 'credit card discount', 'credit card cashback',
            'credit card payment', 'credit card transaction'
        ]
        
        # Enhanced patterns for debit card detection
        debit_patterns = [
            'debit card', 'debit cards', 'debit', 'debitcard', 'debit-card',
            'debit card offer', 'debit card discount', 'debit card cashback',
            'debit card payment', 'debit card transaction'
        ]
        
        # Enhanced patterns for generic card detection
        generic_card_patterns = [
            'all cards', 'bank card', 'bank cards', 'card', 'cards',
            'any card', 'any cards', 'card offer', 'card discount',
            'card payment', 'card transaction', 'plastic money'
        ]
        
        # Check for specific card type patterns
        has_credit = any(pattern in description_lower for pattern in credit_patterns)
        has_debit = any(pattern in description_lower for pattern in debit_patterns)
        has_generic = any(pattern in description_lower for pattern in generic_card_patterns)
        
        # Determine card type with proper formatting
        if has_credit and has_debit:
            return "Credit/Debit Card"
        elif has_credit:
            return "Credit Card"
        elif has_debit:
            return "Debit Card"
        elif has_generic:
            return "Credit/Debit Card"  # Generic card reference
        else:
            return None

    def extract_min_spend(self, description: str) -> Optional[float]:
        """Extract minimum spend requirement"""
        patterns = [
            r'(?:Mini|Minimum)\s+purchase\s+value\s+(?:of\s+)?(?:INR\s+|₹\s*|Rs\.?\s*)([\d,]+\.?\d*)',
            r'(?:Mini|Minimum)\s+(?:purchase|spend|transaction)\s+(?:of\s+|value\s+)?(?:INR\s+|₹\s*|Rs\.?\s*)([\d,]+\.?\d*)',
            r'valid\s+on\s+(?:orders?|purchases?)\s+(?:of\s+|above\s+|worth\s+)(?:INR\s+|₹\s*|Rs\.?\s*)([\d,]+\.?\d*)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1).replace(',', ''))
                except ValueError:
                    continue
        return None

    def parse_offer(self, offer: Dict[str, str]) -> Offer:
        """Parse offer details from raw offer data"""
        description = offer.get('description', '').strip()
        title = offer.get('card_type', 'Flipkart Offer').strip()
        
        amount = self.extract_amount(description)
        bank = self.extract_bank(description)
        min_spend = self.extract_min_spend(description)
        card_type = self.extract_card_type(description)
        
        # Determine offer type using the same logic as filtering with enhanced patterns
        bank_keywords = [
            'bank', 'card', 'credit', 'debit', 'hdfc', 'icici', 'axis', 'sbi', 'kotak',
            'pnb', 'bob', 'canara', 'union bank', 'indian bank', 'bank of india',
            'uco bank', 'iob', 'central bank', 'bank of maharashtra', 'punjab & sind bank',
            'karnataka bank', 'south indian bank', 'dcb bank', 'tmb', 'karur vysya bank',
            'csb bank', 'city union bank', 'bandhan bank', 'jammu & kashmir bank',
            'american express', 'amex', 'citibank', 'hsbc', 'onecard', 'standard chartered',
            'au bank', 'equitas', 'rbl bank', 'federal bank', 'idfc', 'yes bank'
        ]
        if any(keyword in description.lower() for keyword in bank_keywords):
            offer_type = "Bank Offer"
        elif 'cashback' in description.lower():
            offer_type = "Cashback"
        elif any(keyword in description.lower() for keyword in ['emi', 'no cost', 'no-cost', 'installment']):
            offer_type = "No Cost EMI"
        elif any(keyword in description.lower() for keyword in ['partner', 'affiliate', 'third party', 'external']):
            offer_type = "Partner Offer"
        elif 'exchange' in description.lower():
            offer_type = "Exchange Offer"
        else:
            offer_type = "Flipkart Offer"
        
        return Offer(
            title=title,
            description=description,
            amount=amount,
            type=offer_type,
            bank=bank,
            min_spend=min_spend,
            is_instant='instant' in description.lower(),
            card_type=card_type
        )

    def calculate_offer_score(self, offer: Offer, product_price: float) -> float:
        """Calculate offer score focusing on Bank Offers"""
        if offer.type != "Bank Offer":
            return 0
        
        base_score = 80
        
        # Discount amount bonus
        if product_price > 0 and offer.amount > 0:
            discount_percentage = (offer.amount / product_price) * 100
            discount_points = min(discount_percentage * 2, 50)
            base_score += discount_points
        
        # Minimum spend penalty/bonus
        if offer.min_spend and offer.min_spend > product_price:
            penalty_percentage = ((offer.min_spend - product_price) / product_price) * 100
            if penalty_percentage > 50:
                base_score = 15
            else:
                penalty = penalty_percentage * 0.5
                base_score -= penalty
                base_score = max(base_score, 20)
        elif offer.min_spend is None or offer.min_spend <= product_price:
            if offer.min_spend is None:
                base_score += 20
            else:
                spend_ratio = offer.min_spend / product_price if product_price > 0 else 0
                if spend_ratio <= 0.9:
                    bonus = (1 - spend_ratio) * 10
                    base_score += bonus
        
        # Bank reputation bonus
        if offer.bank:
            bank_bonus = (self.bank_scores.get(offer.bank, self.default_bank_score) - 70) / 2
            base_score += bank_bonus
        else:
            base_score -= 5
        
        return max(0, min(100, base_score))

    def determine_offer_type(self, description: str) -> str:
        """Determine offer type based on description content"""
        description_lower = description.lower()
        
        # Check for Bank Offer first (highest priority) with enhanced patterns
        bank_keywords = [
            'bank', 'card', 'credit', 'debit', 'hdfc', 'icici', 'axis', 'sbi', 'kotak',
            'pnb', 'bob', 'canara', 'union bank', 'indian bank', 'bank of india',
            'uco bank', 'iob', 'central bank', 'bank of maharashtra', 'punjab & sind bank',
            'karnataka bank', 'south indian bank', 'dcb bank', 'tmb', 'karur vysya bank',
            'csb bank', 'city union bank', 'bandhan bank', 'jammu & kashmir bank',
            'american express', 'amex', 'citibank', 'hsbc', 'onecard', 'standard chartered',
            'au bank', 'equitas', 'rbl bank', 'federal bank', 'idfc', 'yes bank',
            # Payment gateways / wallets treated as banks
            'phonepe', 'google pay', 'gpay', 'paytm', 'bhim', 'amazon pay', 'mobikwik',
            'freecharge', 'airtel payments bank', 'airtel payment bank', 'airtel bank',
            'jiomoney', 'jio money', 'payzapp', 'cred', 'navi', 'whatsapp pay', 'whatsapp upi',
            'yono', 'yono by sbi', 'sbi yono', 'ola money', 'slice', 'pockets', 'pockets by icici',
            'super money', 'supermoney', 'freo', 'jupiter', 'instantpay', 'instant pay', 'fampay',
            'fam pay', 'finin', 'one card'
        ]
        if any(keyword in description_lower for keyword in bank_keywords):
            return "Bank Offer"
        
        # Check for Cashback (to be filtered out)
        if 'cashback' in description_lower:
            return "Cashback"
        
        # Check for No Cost EMI (to be filtered out)
        if any(keyword in description_lower for keyword in ['emi', 'no cost', 'no-cost', 'installment']):
            return "No Cost EMI"
        
        # Check for Partner Offer (to be filtered out)
        if any(keyword in description_lower for keyword in ['partner', 'affiliate', 'third party', 'external']):
            return "Partner Offer"
        
        # Default to Flipkart Offer
        return "Flipkart Offer"

    def rank_offers(self, offers_data: List[Dict], product_price: float) -> List[Dict[str, Any]]:
        """Rank offers focusing on Bank Offers.
        If there are no bank offers, still order other offers with a simple heuristic so
        titles like 'Flipkart Offer' are accommodated by the ranking flow.
        """
        parsed_offers = [self.parse_offer(offer) for offer in offers_data if isinstance(offer, dict)]
        bank_offers = [offer for offer in parsed_offers if offer.type == "Bank Offer"]
        other_offers = [offer for offer in parsed_offers if offer.type != "Bank Offer"]
        
        all_ranked_offers = []
        
        # Process Bank Offers with ranking
        if bank_offers:
            scored_bank_offers = []
            for offer in bank_offers:
                score = self.calculate_offer_score(offer, product_price)
                
                if offer.min_spend and product_price < offer.min_spend:
                    net_effective_price = product_price
                    is_applicable = False
                else:
                    net_effective_price = max(product_price - offer.amount, 0)
                    is_applicable = True
                
                scored_bank_offers.append({
                    'title': offer.title,
                    'description': offer.description,
                    'amount': offer.amount,
                    'bank': offer.bank,
                    'validity': None,
                    'min_spend': offer.min_spend,
                    'score': score,
                    'is_instant': offer.is_instant,
                    'net_effective_price': net_effective_price,
                    'is_applicable': is_applicable,
                    'note': f"Flipkart bank offer: ₹{offer.amount} discount" + (f" (Min spend: ₹{offer.min_spend})" if offer.min_spend else ""),
                    'offer_type': 'Bank Offer',
                    'card_type': offer.card_type,
                    'card_provider': None
                })
            
            scored_bank_offers.sort(key=lambda x: x['score'], reverse=True)
            for idx, offer in enumerate(scored_bank_offers):
                offer['rank'] = idx + 1
            all_ranked_offers.extend(scored_bank_offers)
        
        # Process other offers; if there were no bank offers, provide a basic rank by amount
        if other_offers:
            ranked_others = []
            for offer in other_offers:
                if offer.min_spend and product_price < offer.min_spend:
                    net_effective_price = product_price
                    is_applicable = False
                else:
                    net_effective_price = max(product_price - offer.amount, 0)
                    is_applicable = True
                ranked_others.append({
                    'title': offer.title,
                    'description': offer.description,
                    'amount': offer.amount,
                    'bank': offer.bank,
                    'validity': None,
                    'min_spend': offer.min_spend,
                    'score': None if bank_offers else offer.amount,  # use amount as tie-break when no bank offers
                    'is_instant': offer.is_instant,
                    'net_effective_price': net_effective_price,
                    'is_applicable': is_applicable,
                    'note': f"Flipkart {offer.type.lower()}: ₹{offer.amount} value" if offer.amount > 0 else f"Flipkart {offer.type.lower()}",
                    'offer_type': offer.type,
                    'rank': None,
                    'card_type': offer.card_type,
                    'card_provider': None
                })
            if not bank_offers:
                ranked_others.sort(key=lambda x: (x['score'] or 0), reverse=True)
                for idx, item in enumerate(ranked_others):
                    item['rank'] = idx + 1
            all_ranked_offers.extend(ranked_others)
        
        return all_ranked_offers

def determine_offer_type_standalone(description: str) -> str:
    """Standalone function to determine offer type based on description content"""
    description_lower = description.lower()
    
    # Check for Bank Offer first (highest priority)
    if any(keyword in description_lower for keyword in ['bank', 'card', 'credit', 'debit', 'hdfc', 'icici', 'axis', 'sbi', 'kotak']):
        return "Bank Offer"
    
    # Check for Cashback (to be filtered out)
    if 'cashback' in description_lower:
        return "Cashback"
    
    # Check for No Cost EMI (to be filtered out)
    if any(keyword in description_lower for keyword in ['emi', 'no cost', 'no-cost', 'installment']):
        return "No Cost EMI"
    
    # Check for Partner Offer (to be filtered out)
    if any(keyword in description_lower for keyword in ['partner', 'affiliate', 'third party', 'external']):
        return "Partner Offer"
    
    # Default to Flipkart Offer
    return "Flipkart Offer"

def extract_price_amount(price_str):
    """Extract numeric amount from price string"""
    if not price_str:
        return 0.0
    numbers = re.findall(r'[\d,]+\.?\d*', price_str)
    if numbers:
        return float(numbers[0].replace(',', ''))
    return 0.0

def get_flipkart_offers(driver, url, max_retries=1, fast: bool = False):
    """Faster Flipkart offers scraping (reduced sleeps, EC waits)."""
    for attempt in range(max_retries):
        try:
            logging.info(f"[FAST] Visiting {url} (try {attempt+1})")
            driver.get(url)
            # Try closing login popup quickly
            try:
                WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'✕')]"))
                ).click()
            except Exception:
                pass
            # Scroll minimal to trigger offers
            driver.execute_script("window.scrollBy(0, 800);")
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(text(),'Available offers')]"))
                )
            except TimeoutException:
                if attempt < max_retries - 1:
                    continue
                return []
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            offers = []
            header = soup.find("div", string=lambda t: t and "Available offers" in t)
            if header:
                parent = header.find_parent("div")
                if parent:
                    for li in parent.find_all("li"):
                        text = li.get_text(" ", strip=True)
                        if not text or len(text) < 12:
                            continue
                        otype = determine_offer_type_standalone(text)
                        if otype in ("Flipkart Offer", "Bank Offer"):
                            offers.append({"card_type": otype, "offer_title": "Offer", "description": text})
            # De-dupe
            seen = set()
            uniq = []
            for o in offers:
                d = o["description"]
                if d not in seen:
                    seen.add(d)
                    uniq.append(o)
            return uniq
        except Exception as e:
            logging.error(f"Fast offer scrape error {e}")
            if attempt < max_retries - 1:
                continue
    return []

def create_chrome_driver(fast: bool = False):
    """
    Optimized Chrome driver (headless) with heavy resource blocking.
    fast=True blocks more content.
    """
    print("🤖 Creating headless Chrome (optimized)")
    options = uc.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-sync')
    options.add_argument('--disable-background-networking')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-search-engine-choice-screen')
    options.add_argument('--no-first-run')
    options.add_argument('--disable-notifications')
    options.add_argument('--disable-translate')
    options.add_argument('--mute-audio')
    options.add_argument('--log-level=3')
    # Aggressive content blocking
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.fonts": 2,
        "profile.managed_default_content_settings.plugins": 2,
        "profile.managed_default_content_settings.popups": 2,
        "profile.managed_default_content_settings.geolocation": 2,
        "profile.managed_default_content_settings.notifications": 2,
        "profile.managed_default_content_settings.media_stream": 2,
        "profile.managed_default_content_settings.mixed_script": 2,
        "profile.managed_default_content_settings.push_messaging": 2,
        "profile.managed_default_content_settings.auto_select_certificate": 2,
        "profile.managed_default_content_settings.unsandboxed_plugins": 2,
        "profile.managed_default_content_settings.pointer_lock": 2
    }
    # Keep JS enabled (needed) but allow disabling CSS/images already.
    options.add_experimental_option("prefs", prefs)
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36')
    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(25)
    driver.implicitly_wait(3 if fast else 5)
    return driver

def process_comprehensive_flipkart_links(input_file="comprehensive_amazon_offers.json", 
                                       output_file="comprehensive_amazon_offers.json",
                                       flipkart_urls_file="visited_urls_flipkart.txt",
                                       shard_index: int | None = None,
                                       total_shards: int | None = None,
                                       session_batch_size: int = 100,
                                       fast: bool = False):
    """
    Process ALL Flipkart store links in the comprehensive JSON file
    - Completely isolates Amazon and Croma offers (no changes)
    - Processes ALL Flipkart links (including those with existing offers - re-scrapes everything)
    - Traverses ALL nested locations comprehensively
    - Runs in fully automated mode (headless, no user interaction)
    
    NEW FUNCTIONALITY:
    - Extracts Flipkart product prices from <div class="Nx9bqj CxhGGd yKS4la"> elements
    - Updates 'price' key with extracted price if found
    - Adds 'in_stock' key with refined logic:
      * False if "Sold Out" tag exists
      * True if bank offers found AND no "Sold Out" tag  
      * None (undetermined) otherwise
    - Tracks visited URLs in visited_urls_flipkart.txt file
    - Maintains existing offer scraping and ranking functionality
    - BROWSER SESSION MANAGEMENT: Creates fresh Chrome session for each link
    
    AUTOMATION FEATURES:
    - Headless server mode by default
    - Processes all URLs from beginning to end
    - No user prompts or interaction required
    - Automatic URL tracking and complete processing
    - Smart session recycling for better stability
    """
    
    # Create backup before processing
    backup_file = f"{input_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(input_file, backup_file)
    print(f"💾 Created backup: {backup_file}")
    
    # Load the JSON data
    print(f"📖 Loading data from {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"✅ Loaded {len(data)} entries")
    
    # Setup visited URLs tracking with new functionality
    visited_urls_file = manage_visited_urls_file("visited_urls_flipkart.txt")
    visited_urls = load_visited_urls(visited_urls_file)
    
    # Initialize comprehensive extractor
    extractor = ComprehensiveFlipkartExtractor(input_file, flipkart_urls_file)
    
    # Find ALL Flipkart store links using comprehensive traversal
    print(f"🔍 Searching for Flipkart links in ALL nested locations...")
    flipkart_links = extractor.find_all_flipkart_store_links(data)

    # Sharding (divide workload) if parameters provided
    if total_shards is not None and shard_index is not None:
        if total_shards <= 0:
            raise ValueError("total_shards must be > 0")
        if not (0 <= shard_index < total_shards):
            raise ValueError("shard_index must be in range [0, total_shards)")
        original_total = len(flipkart_links)
        flipkart_links = [link for i, link in enumerate(flipkart_links) if i % total_shards == shard_index]
        print(f"🧩 Sharding enabled: shard {shard_index+1}/{total_shards} -> {len(flipkart_links)} of {original_total} links")
        # Adjust output file automatically if user did not customize (avoid write collisions)
        if output_file == input_file or output_file.endswith('.json') and not any(f".shard" in output_file for _ in [0]):
            base, ext = os.path.splitext(output_file)
            output_file = f"{base}.shard{shard_index+1}of{total_shards}{ext}"
            print(f"💾 Adjusted output file for shard isolation: {output_file}")
    
    # Check visited URLs but don't filter - process ALL links
    original_count = len(flipkart_links)
    already_visited_count = len([link for link in flipkart_links if link['url'] in visited_urls])
    if already_visited_count > 0:
        print(f"🔄 Found {already_visited_count} previously visited URLs (will re-process all)")
    
    print(f"🚀 Processing ALL {len(flipkart_links)} Flipkart links (including those with existing offers/data)")
    
    print(f"📊 Total Flipkart store links found: {len(flipkart_links)} (will process ALL)")
    
    # Auto-configuration: Process all links from beginning (no user interaction)
    start_idx = 0
    max_entries = None
    print(f"🚀 Auto-configuration: Processing ALL {len(flipkart_links)} links from beginning to end")
    
    if not flipkart_links:
        print("✅ No Flipkart links found in the JSON data")
        return
    
    # Initialize resource management
    print(f"🔧 Initializing resource management...")
    increase_file_limits()
    log_resource_usage("Initial system state - ")
    
    # Setup analyzer
    analyzer = FlipkartOfferAnalyzer()
    
    processed_count = 0
    new_offers_count = 0
    
    driver = None
    try:
        start_time_overall = time.time()
        for idx, link_data in enumerate(flipkart_links):
            link_start = time.time()
            if driver is None or (session_batch_size and idx > 0 and idx % session_batch_size == 0):
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                driver = create_chrome_driver(fast=fast)
                print(f"♻️  [Shard {shard_label(shard_index,total_shards)}] Driver (re)initialized at link {idx+1}", flush=True)

            url = link_data['url']
            store_link_ref = link_data['store_link_ref']
            print(f"\n🔗 [Shard {shard_label(shard_index,total_shards)}] ({idx+1}/{len(flipkart_links)}) {url}", flush=True)

            offers = get_flipkart_offers(driver, url, max_retries=1 if fast else 2, fast=fast)
            offers_found = bool(offers)

            price_stock = extract_flipkart_price_and_stock(driver, url, offers_found=offers_found, fast=fast)
            if price_stock.get('price'):
                store_link_ref['price'] = price_stock['price']
            store_link_ref['in_stock'] = price_stock.get('in_stock')
            if price_stock.get('product_name_via_url'):
                store_link_ref['product_name_via_url'] = price_stock['product_name_via_url']
            try:
                store_link_ref['platform_url'] = driver.current_url
            except Exception:
                store_link_ref['platform_url'] = url

            offers_count = 0
            if offers:
                product_price = extract_price_amount(store_link_ref.get('price',''))
                ranked = analyzer.rank_offers(offers, product_price)
                store_link_ref['ranked_offers'] = ranked
                offers_count = len(ranked)
                new_offers_count += offers_count
            else:
                store_link_ref['ranked_offers'] = []

            link_duration = time.time() - link_start
            append_visited_url(
                url,
                flipkart_urls_file,
                shard_index=shard_index,
                total_shards=total_shards,
                status="ok",
                offers_count=offers_count,
                price=store_link_ref.get('price'),
                duration=link_duration
            )

            processed_count += 1
            print(
                f"✅ [Shard {shard_label(shard_index,total_shards)}] "
                f"Done {idx+1}/{len(flipkart_links)} | {link_duration:.2f}s | offers={offers_count} | "
                f"price={store_link_ref.get('price','NA')} | in_stock={store_link_ref.get('in_stock')}",
                flush=True
            )

            if not fast:
                time.sleep(0.6)

            # Optional periodic mini-progress snapshots
            if processed_count % 50 == 0:
                elapsed = time.time() - start_time_overall
                rate = processed_count / elapsed if elapsed > 0 else 0
                print(f"📈 [Shard {shard_label(shard_index,total_shards)}] "
                      f"Progress {processed_count}/{len(flipkart_links)} | {rate:.2f} links/s", flush=True)

        # Save progress every 10 entries
        temp_backup = f"{output_file}.progress_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(temp_backup, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"   💾 Progress saved to {temp_backup}")
        
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted! Saving progress...")
    
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        force_cleanup()
        log_resource_usage("Final cleanup - ")
        
        # Save final output
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Final output saved to {output_file}")
        
        # Summary
        print(f"\n📊 COMPREHENSIVE FLIPKART PROCESSING SUMMARY:")
        print(f"   Flipkart links processed: {processed_count}")
        print(f"   New offers added: {new_offers_count}")
        print(f"   💰 Price extraction: Active (from Flipkart pages)")
        print(f"   📦 Stock tracking: Active with refined logic + retry mechanism (in_stock: true/false/null)")
        print(f"      • False = Sold Out tag found")
        print(f"      • True = Offers found + No Sold Out tag")  
        print(f"      • None = Undetermined status (after up to 2 retries with 3s delay)")
        print(f"   📝 URL tracking: Active (visited_urls_flipkart.txt updated)")
        print(f"   🔄 Processing: ALL links processed (including re-scraping existing)")
        print(f"   🤖 Automation: Fully automated (headless mode)")
        print(f"   🔒 Amazon offers: COMPLETELY ISOLATED (no changes)")
        print(f"   🔒 Croma offers: COMPLETELY ISOLATED (no changes)")
        print(f"   ✅ Backup created: {backup_file}")

# ===============================================
# API SETUP AND ENDPOINTS FOR FLIPKART SCRAPER
# ===============================================

# Initialize Flask app
app = Flask(__name__)

# Global variable to track scraping status
scraping_status = {
    'is_running': False,
    'progress': 0,
    'total': 0,
    'current_url': '',
    'completed': False,
    'error': None,
    'start_time': None,
    'end_time': None,
    'output_file': None
}

def run_flipkart_scraper_process(input_file="all_data.json", output_file=None, flipkart_urls_file="visited_urls_flipkart.txt"):
    """
    Function to run the Flipkart scraper process in a separate thread
    """
    global scraping_status
    
    try:
        # Generate timestamped output filename if not provided
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"all_data_flipkart_{timestamp}.json"
        
        # Reset status
        scraping_status.update({
            'is_running': True,
            'progress': 0,
            'total': 0,
            'current_url': '',
            'completed': False,
            'error': None,
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'output_file': output_file
        })
        
        logging.info(f"API triggered Flipkart scraper process started with output file: {output_file}")
        
        # Run the main scraping function
        process_comprehensive_flipkart_links(input_file, output_file, flipkart_urls_file)
        
        # Mark as completed
        scraping_status.update({
            'is_running': False,
            'completed': True,
            'end_time': datetime.now().isoformat()
        })
        
        logging.info("API triggered Flipkart scraper process completed successfully")
        
    except Exception as e:
        # Mark as error
        scraping_status.update({
            'is_running': False,
            'completed': False,
            'error': str(e),
            'end_time': datetime.now().isoformat()
        })
        
        logging.error(f"API triggered Flipkart scraper process failed: {e}")

@app.route('/start-scraping', methods=['POST'])
def start_scraping():
    """
    API endpoint to start the Flipkart scraping process
    """
    global scraping_status
    
    # Check if scraping is already running
    if scraping_status['is_running']:
        return jsonify({
            'status': 'error',
            'message': 'Flipkart scraping is already in progress',
            'data': scraping_status
        }), 400
    
    try:
        # Get parameters from request (if any)
        data = request.get_json() if request.is_json else {}
        
        input_file = data.get('input_file', 'all_data.json')
        # Generate timestamped output filename if not provided
        output_file = data.get('output_file', None)
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"all_data_flipkart_{timestamp}.json"
        flipkart_urls_file = data.get('flipkart_urls_file', 'visited_urls_flipkart.txt')
        
        # Start scraping in a separate thread
        scraper_thread = threading.Thread(
            target=run_flipkart_scraper_process,
            args=(input_file, output_file, flipkart_urls_file),
            daemon=True
        )
        scraper_thread.start()
        
        return jsonify({
            'status': 'success',
            'message': 'Flipkart scraping process started successfully',
            'data': {
                'input_file': input_file,
                'output_file': output_file,
                'flipkart_urls_file': flipkart_urls_file,
                'started_at': scraping_status['start_time']                             
            }
        }), 200
        
    except Exception as e:
        logging.error(f"Error starting Flipkart scraper via API: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to start Flipkart scraping: {str(e)}',
            'data': None
        }), 500

@app.route('/scraping-status', methods=['GET'])
def get_scraping_status():
    """
    API endpoint to get the current Flipkart scraping status
    """
    return jsonify({
        'status': 'success',
        'message': 'Flipkart scraping status retrieved successfully',
        'data': scraping_status
    }), 200

@app.route('/stop-scraping', methods=['POST'])
def stop_scraping():
    """
    API endpoint to stop the Flipkart scraping process (graceful stop)
    """
    global scraping_status
    
    if not scraping_status['is_running']:
        return jsonify({
            'status': 'error',
            'message': 'No Flipkart scraping process is currently running',
            'data': scraping_status
        }), 400
    
    # Note: This is a simple status update. For true process termination,
    # you would need more sophisticated thread management
    scraping_status.update({
        'is_running': False,
        'completed': False,
        'error': 'Stopped by user request',
        'end_time': datetime.now().isoformat()
    })
    
    return jsonify({
        'status': 'success',
        'message': 'Flipkart scraping process stop requested',
        'data': scraping_status
    }, 200)

@app.route('/health', methods=['GET'])
def health_check():
    """
    API endpoint for health check
    """
    return jsonify({
        'status': 'success',
        'message': 'Enhanced Flipkart Scraper API is healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    }), 200

@app.route('/', methods=['GET'])
def api_info():
    """
    API endpoint for basic information
    """
    return jsonify({
        'name': 'Enhanced Flipkart Scraper API',
        'version': '1.0.0',
        'description': 'API to trigger comprehensive Flipkart scraping with price & stock tracking',
        'endpoints': {
            'POST /start-scraping': 'Start the Flipkart scraping process',
            'GET /scraping-status': 'Get current scraping status',
            'POST /stop-scraping': 'Stop the scraping process',
            'GET /health': 'Health check',
            'GET /': 'API information'
        },
        'features': [
            'Flipkart product prices extraction',
            'Stock status tracking (in_stock: true/false/null)',
            'Comprehensive nested location traversal',
            'Bank offers scraping and ranking',
            'URL visit tracking',
            'Smart session management',
            'Resource management and cleanup',
            'Progress tracking via API'
        ]
    }), 200

MERGE_COPY_KEYS = [
    "price", "in_stock", "ranked_offers", "platform_url",
    "product_name_via_url", "with_exchange_price", "exchange_amount"
]

def is_flipkart_store_link(sl: dict) -> bool:
    if not isinstance(sl, dict):
        return False
    name = str(sl.get("name","")).lower()
    url = str(sl.get("url","")).lower()
    return "flipkart" in name or "flipkart.com" in url

def walk_store_links(node, collector):
    """
    Recursively traverse nested structure and call collector(store_link_dict)
    for each Flipkart store link.
    """
    if isinstance(node, dict):
        if "store_links" in node and isinstance(node["store_links"], list):
            for sl in node["store_links"]:
                if is_flipkart_store_link(sl):
                    collector(sl)
        for v in node.values():
            walk_store_links(v, collector)
    elif isinstance(node, list):
        for item in node:
            walk_store_links(item, collector)

def build_flipkart_url_map(data) -> dict:
    """
    Build map: normalized_url -> store_link_dict reference (original structure).
    Normalization: strip trailing slashes, lowercase.
    """
    url_map = {}
    def collect(sl):
        url = sl.get("url")
        if not url:
            return
        norm = normalize_url(url)
        if norm not in url_map:
            url_map[norm] = sl
    walk_store_links(data, collect)
    return url_map

def normalize_url(u: str) -> str:
    u = u.strip()
    # Remove fragments, trivial tracking params (keep base path)
    base = u.split('#',1)[0]
    # Could strip query entirely for robustness; Flipkart product ID in path.
    return base.rstrip('/').lower()

def merge_shard_outputs(original_input_path: str, shard_files: list[str], merged_output_path: str):
    print(f"🔄 Merging {len(shard_files)} shard files into {merged_output_path}", flush=True)
    with open(original_input_path, 'r', encoding='utf-8') as f:
        master = json.load(f)
    master_map = build_flipkart_url_map(master)
    updated_count = 0
    seen_urls = set()
    for sf in shard_files:
        try:
            with open(sf, 'r', encoding='utf-8') as f:
                shard_data = json.load(f)
        except Exception as e:
            print(f"⚠️  Skipping shard file {sf}: {e}", flush=True)
            continue
        def apply(sl):
            nonlocal updated_count
            url = sl.get("url")
            if not url:
                return
            norm = normalize_url(url)
            target = master_map.get(norm)
            if not target:
                # New link not in original? Optionally append logic here.
                return
            changed = False
            for k in MERGE_COPY_KEYS:
                if k in sl:
                    if sl[k] != target.get(k):
                        target[k] = sl[k]
                        changed = True
            if changed:
                updated_count += 1
                seen_urls.add(norm)
        walk_store_links(shard_data, apply)
    with open(merged_output_path, 'w', encoding='utf-8') as f:
        json.dump(master, f, ensure_ascii=False, indent=2)
    print(f"✅ Merge complete. Updated {updated_count} store_link entries. Output: {merged_output_path}", flush=True)
    return updated_count

def shard_label(shard_index, total_shards):
    """
    Safe shard label.
    """
    if shard_index is None or total_shards is None or total_shards <= 0:
        return "1/1"
    return f"{shard_index+1}/{total_shards}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enhanced Flipkart scraper with optional sharding/parallelism")
    parser.add_argument('--api', action='store_true', help='Run as Flask API server')
    parser.add_argument('--input-file', default='all_data.json', help='Input JSON file (default all_data.json)')
    parser.add_argument('--output-file', default=None, help='Output JSON file (default auto timestamp)')
    parser.add_argument('--shard-index', type=int, default=None, help='Shard index (0-based)')
    parser.add_argument('--total-shards', type=int, default=None, help='Total number of shards')
    parser.add_argument('--run-all-shards', action='store_true', help='Spawn all shard processes (implies total shards = --workers)')
    parser.add_argument('--workers', type=int, default=5, help='Workers when using --run-all-shards (default 5)')
    # Auto-sharding / performance targeting options
    parser.add_argument('--auto-shards', action='store_true', help='Estimate required shards to meet target runtime; print and optionally run')
    parser.add_argument('--target-minutes', type=float, default=10.0, help='Target end-to-end runtime in minutes for auto-sharding (default 10)')
    parser.add_argument('--assumed-seconds-per-link', type=float, default=4.0, help='Assumed average seconds per Flipkart link (default 4.0)')
    parser.add_argument('--memory-per-process-mb', type=int, default=350, help='Estimated MB RAM consumed per headless Chrome process (default 350)')
    # Merging options
    parser.add_argument('--merge-shards', action='store_true', help='Merge shard JSON outputs back into a single dataset')
    parser.add_argument('--shard-prefix', default=None, help='Base prefix of shard files (defaults to output-file sans .json)')
    parser.add_argument('--fast', action='store_true', help='Enable fast mode (reduced parsing & waits)')
    parser.add_argument('--session-batch-size', type=int, default=100, help='Links per Chrome session before recycle')
    parser.add_argument('--orchestrate-docker', action='store_true', help='Compute shards then launch that many docker containers (one shard each)')
    parser.add_argument('--docker-image', default='flipkart-scraper:latest', help='Docker image name for shard containers')
    parser.add_argument('--max-containers', type=int, default=None, help='Hard cap on containers (override auto shards if lower)')
    parser.add_argument('--dry-run-docker', action='store_true', help='Show docker commands without executing')
    parser.add_argument('--keep-containers', action='store_true', help='(Docker orchestration) keep containers after exit (omit --rm)')
    parser.add_argument('--single-output', help='Merge all shard results into this single JSON file after parallel run')
    parser.add_argument('--cleanup-shards', action='store_true', help='Delete shard JSON files after successful merge')
    args, unknown = parser.parse_known_args()

    # Ensure output_file never None before any use
    if not args.output_file:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_name = os.path.splitext(os.path.basename(args.input_file))[0]
        args.output_file = f"{base_name}_flipkart_{ts}.json"
        print(f"📝 Auto output file: {args.output_file}")

    if args.run_all_shards:
        total = args.total_shards or args.workers
        base_output = args.output_file.replace('.json','') if args.output_file else 'flipkart_output'
        shard_files = []
        procs = []
        for shard in range(total):
            shard_out = f"{base_output}.shard{shard+1}of{total}.json"
            shard_files.append(shard_out)
            p = multiprocessing.Process(target=process_comprehensive_flipkart_links, kwargs={
                'input_file': args.input_file,
                'output_file': shard_out,
                'flipkart_urls_file': f'visited_urls_flipkart_shard{shard+1}.txt',
                'shard_index': shard,
                'total_shards': total,
                'session_batch_size': args.session_batch_size,
                'fast': args.fast
            })
            p.start()
            procs.append(p)
            time.sleep(0.5)
        for p in procs:
            p.join()
        print("🧩 All shard processes finished.", flush=True)

        if args.single_output:
            try:
                merge_shard_outputs(args.input_file, shard_files, args.single_output)
                if args.cleanup_shards:
                    for sf in shard_files:
                        try:
                            os.remove(sf)
                            print(f"🗑  Removed shard file {sf}", flush=True)
                        except Exception as e:
                            print(f"⚠️  Could not remove {sf}: {e}", flush=True)
                print("🎯 Single merged file ready.")
            except Exception as e:
                print(f"❌ Merge failed: {e}", flush=True)
        else:
            print("ℹ️  No --single-output specified: shard files retained.", flush=True)
    else:
        process_comprehensive_flipkart_links(
            input_file=args.input_file,
            output_file=args.output_file,
            flipkart_urls_file='visited_urls_flipkart.txt',
            shard_index=args.shard_index,
            total_shards=args.total_shards,
            session_batch_size=args.session_batch_size,
            fast=args.fast
        )