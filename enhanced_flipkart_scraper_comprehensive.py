
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
import undetected_chromedriver as uc
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, InvalidSessionIdException, WebDriverException
import shutil
from flask import Flask, request, jsonify
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import multiprocessing
import random
import math

# Setup logging
logging.basicConfig(
    filename='enhanced_flipkart_scraper.log',
    filemode='a',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

# Threading locks for safe concurrent operations
VISITED_LOCK = threading.Lock()
FILE_SAVE_LOCK = threading.Lock()
STATUS_LOCK = threading.Lock()

# Speed toggles
# Set FAST_MODE=0 in environment to disable fast-path shortcuts and extra blocking.
FAST_MODE = os.getenv("FAST_MODE", "1") != "0"

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

def append_visited_url(url, file_path="visited_urls_flipkart.txt"):
    """
    Append a newly processed URL to the tracking file
    """
    try:
        with VISITED_LOCK:
            with open(file_path, 'a', encoding='utf-8') as f:
                f.write(f"{url}\n")
    except Exception as e:
        print(f"⚠️  Error appending URL to visited file: {e}")

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



def extract_flipkart_price_and_stock(driver, url, offers_found=False):
    """
    Extract price, stock status, and product name from Flipkart product page
    
    REFINED LOGIC for in_stock flag:
    1. If "Sold Out" tag exists → in_stock = False
    2. If bank offers found AND no "Sold Out" tag → in_stock = True  
    3. Otherwise → in_stock = None (undetermined)
    
    Args:
        driver: Selenium WebDriver instance
        url: Flipkart product URL
        offers_found: bool - Whether bank offers were found on the page
    
    Returns:
    dict: {
        'price': str (extracted price or None),
        'in_stock': bool/None (True/False/None based on refined logic),
        'with_exchange_price': Optional[str] (computed if exchange discount detected),
        'exchange_amount': Optional[float] (numeric discount for exchange if found),
        'product_name_via_url': str (extracted product name from <span class="VU-ZEz"> or None)
    }
    """
    try:
        # Get page source for parsing
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        result = {
            'price': None,
            'in_stock': None,  # Will be determined by refined logic
            'with_exchange_price': None,
            'exchange_amount': None,
            'product_name_via_url': None
        }
        
        # 0. Extract product name from <span class="VU-ZEz"> element
        product_name_element = soup.find('span', class_='VU-ZEz')
        if product_name_element:
            product_name = product_name_element.get_text(strip=True)
            if product_name:
                result['product_name_via_url'] = product_name
                print(f"   📱 Found product name: {product_name}")
            else:
                print(f"   📱 Product name element found but no text content")
        else:
            print(f"   📱 Product name element not found")
        
        # 1. Check for Flipkart price element: <div class="Nx9bqj CxhGGd yKS4la">₹52,999</div>
        price_element = soup.find('div', class_=lambda x: x and 'Nx9bqj' in x and 'CxhGGd' in x and 'yKS4la' in x)
        if price_element:
            price_text = price_element.get_text(strip=True)
            if '₹' in price_text:
                result['price'] = price_text
                print(f"   💰 Found Flipkart price: {price_text}")
        
        # 2. Extract exchange offer text using the exact path specified (skippable in FAST_MODE)
        try:
            if FAST_MODE:
                # Quick path: a light heuristic scan only
                exchange_label = soup.find('label', attrs={'for': 'BUY_WITH_EXCHANGE'})
                if exchange_label:
                    kdbd = exchange_label.find(class_='-KdBdD')
                    if kdbd:
                        txt = kdbd.get_text(strip=True)
                        if txt:
                            result['with_exchange_price'] = txt
                # Skip heavy traversal when fast
                if result['with_exchange_price']:
                    pass
                else:
                    # Fallback tiny heuristic
                    any_kdbd = soup.find(class_='-KdBdD')
                    if any_kdbd:
                        t = any_kdbd.get_text(strip=True)
                        if t and ('₹' in t or any(ch.isdigit() for ch in t)):
                            result['with_exchange_price'] = t
            else:
                # First, run debug analysis to see what's available on the page
                debug_exchange_elements(soup)
                
                # Follow the exact path: container -> _39kFie N3De93 JxFEK3 _48O0EI -> DOjaWF YJG4Cf -> DOjaWF gdgoEp col-8-12 -> cPHDOP col-12-12 -> BRgXml -> label for="BUY_WITH_EXCHANGE" -> VTUEC- JvjVG5 -> div data-disabled="false" data-checked="false" -> -B1t91 -> -KdBdD
                
                # Start from the container
                container = soup.find(id='container')
                if container:
                    # Find the main wrapper with class "_39kFie N3De93 JxFEK3 _48O0EI"
                    main_wrapper = container.find(class_=lambda x: x and '_39kFie' in x and 'N3De93' in x and 'JxFEK3' in x and '_48O0EI' in x)
                    if main_wrapper:
                        # Find DOjaWF YJG4Cf
                        doja_wrapper = main_wrapper.find(class_=lambda x: x and 'DOjaWF' in x and 'YJG4Cf' in x)
                        if doja_wrapper:
                            # Find DOjaWF gdgoEp col-8-12
                            gdgo_wrapper = doja_wrapper.find(class_=lambda x: x and 'DOjaWF' in x and 'gdgoEp' in x and 'col-8-12' in x)
                            if gdgo_wrapper:
                                # Find cPHDOP col-12-12 - there might be multiple, so iterate through them
                                cphd_wrappers = gdgo_wrapper.find_all(class_=lambda x: x and 'cPHDOP' in x and 'col-12-12' in x)
                                if cphd_wrappers:
                                    print(f"   🔍 Found {len(cphd_wrappers)} cPHDOP col-12-12 elements")
                                    
                                    # Iterate through all cPHDOP col-12-12 elements to find the one with BRgXml
                                    brg_wrapper = None
                                    cphd_wrapper_with_brg = None
                                    
                                    for i, cphd_wrapper in enumerate(cphd_wrappers):
                                        print(f"      🔍 Checking cPHDOP col-12-12 element {i+1}/{len(cphd_wrappers)}")
                                        
                                        # Find BRgXml in this specific cPHDOP element
                                        brg_wrapper = cphd_wrapper.find(class_='BRgXml')
                                        if brg_wrapper:
                                            print(f"      ✅ BRgXml found in cPHDOP col-12-12 element {i+1}")
                                            cphd_wrapper_with_brg = cphd_wrapper
                                            break
                                        else:
                                            print(f"      ❌ BRgXml NOT found in cPHDOP col-12-12 element {i+1}")
                                    
                                    if brg_wrapper and cphd_wrapper_with_brg:
                                        # Find label with for="BUY_WITH_EXCHANGE" and specific classes
                                        exchange_label = brg_wrapper.find('label', attrs={'for': 'BUY_WITH_EXCHANGE'})
                                        if exchange_label:
                                            # Check if it has the expected classes
                                            label_classes = exchange_label.get('class', [])
                                            expected_classes = ['VKzPTL', 'JESWSS', 'RI1ZCR']
                                            if all(cls in label_classes for cls in expected_classes):
                                                print(f"   ✅ BUY_WITH_EXCHANGE label found with correct classes: {label_classes}")
                                                
                                                # Find VTUEC- JvjVG5
                                                vtuec_wrapper = exchange_label.find(class_=lambda x: x and 'VTUEC-' in x and 'JvjVG5' in x)
                                                if vtuec_wrapper:
                                                    print(f"   ✅ VTUEC- JvjVG5 wrapper found")
                                                    
                                                    # Find div with data-disabled="true" data-checked="false" disabled
                                                    exchange_div = vtuec_wrapper.find('div', attrs={'data-disabled': 'true', 'data-checked': 'false', 'disabled': ''})
                                                    if exchange_div:
                                                        print(f"   ✅ Exchange div with data-disabled='true' data-checked='false' disabled found")
                                                        
                                                        # Find -B1t91
                                                        b1t91_wrapper = exchange_div.find(class_='-B1t91')
                                                        if b1t91_wrapper:
                                                            print(f"   ✅ -B1t91 wrapper found")
                                                            
                                                            # Finally find -KdBdD which contains the exchange price
                                                            exchange_text_element = b1t91_wrapper.find(class_='-KdBdD')
                                                            if exchange_text_element:
                                                                exchange_text = exchange_text_element.get_text(strip=True)
                                                                if exchange_text:
                                                                    result['with_exchange_price'] = exchange_text
                                                                    print(f"   🔁 Found exchange text using exact path: {exchange_text}")
                                                                else:
                                                                    print(f"   🔁 Exchange element found but no text content")
                                                            else:
                                                                print(f"   🔁 -KdBdD element not found in -B1t91 wrapper")
                                                        else:
                                                            print(f"   🔁 -B1t91 wrapper not found in exchange div")
                                                    else:
                                                        print(f"   🔁 Exchange div with data-disabled='true' data-checked='false' disabled not found")
                                                else:
                                                    print(f"   🔁 VTUEC- JvjVG5 wrapper not found in exchange label")
                                            else:
                                                print(f"   🔁 BUY_WITH_EXCHANGE label found but missing expected classes. Found: {label_classes}, Expected: {expected_classes}")
                                        else:
                                            print(f"   🔁 BUY_WITH_EXCHANGE label not found in BRgXml")
                                    else:
                                        print(f"   🔁 BRgXml wrapper not found in any cPHDOP col-12-12 element")
                                else:
                                    print(f"   🔁 No cPHDOP col-12-12 elements found in DOjaWF gdgoEp")
                            else:
                                print(f"   🔁 DOjaWF gdgoEp col-8-12 wrapper not found in DOjaWF YJG4Cf")
                        else:
                            print(f"   🔁 DOjaWF YJG4Cf wrapper not found in main wrapper")
                    else:
                        print(f"   🔁 Main wrapper _39kFie N3De93 JxFEK3 _48O0EI not found in container")
                else:
                    print(f"   🔁 Container with id='container' not found")
                
            # Fallback: Try the old method if the exact path fails
            if not result['with_exchange_price'] and not FAST_MODE:
                print(f"   🔁 Trying fallback method...")
                exchange_label = soup.find('label', attrs={'for': 'BUY_WITH_EXCHANGE'})
                if exchange_label:
                    exchange_text_element = exchange_label.find(class_='-KdBdD')
                    if exchange_text_element:
                        exchange_text = exchange_text_element.get_text(strip=True)
                        if exchange_text:
                            result['with_exchange_price'] = exchange_text
                            print(f"   🔁 Found exchange text using fallback: {exchange_text}")
                        else:
                            print(f"   🔁 Exchange element found but no text content")
                    else:
                        print(f"   🔁 BUY_WITH_EXCHANGE label found but no -KdBdD element")
                else:
                    print(f"   🔁 No BUY_WITH_EXCHANGE label found")
            
            # Additional fallback: Try to find -KdBdD element anywhere on the page
            if not result['with_exchange_price'] and not FAST_MODE:
                print(f"   🔁 Trying additional fallback: searching for -KdBdD anywhere on page...")
                all_kdbd_elements = soup.find_all(class_='-KdBdD')
                if all_kdbd_elements:
                    # Look for the one that contains price-like text
                    for elem in all_kdbd_elements:
                        text = elem.get_text(strip=True)
                        if text and ('₹' in text or any(char.isdigit() for char in text)):
                            result['with_exchange_price'] = text
                            print(f"   🔁 Found exchange text using additional fallback: {text}")
                            break
                    if not result['with_exchange_price']:
                        print(f"   🔁 Found -KdBdD elements but none contain price-like text")
                else:
                    print(f"   🔁 No -KdBdD elements found anywhere on the page")
            
            # Final fallback: Try to find any text that looks like an exchange price
            if not result['with_exchange_price'] and not FAST_MODE:
                print(f"   🔁 Trying final fallback: searching for exchange-related text...")
                # Look for any text that mentions exchange and contains price
                exchange_keywords = ['exchange', 'with exchange', 'buy with exchange']
                for keyword in exchange_keywords:
                    elements = soup.find_all(text=lambda text: text and keyword.lower() in text.lower())
                    for element in elements:
                        parent = element.parent
                        if parent:
                            # Look for price-like text in the same element or nearby
                            price_text = parent.get_text(strip=True)
                            if '₹' in price_text:
                                # Extract just the price part
                                price_match = re.search(r'₹[\d,]+', price_text)
                                if price_match:
                                    result['with_exchange_price'] = price_match.group()
                                    print(f"   🔁 Found exchange text using final fallback: {price_match.group()}")
                                    break
                        if result['with_exchange_price']:
                            break
                    if result['with_exchange_price']:
                        break
            
            # If all methods failed, log the issue
            if not result['with_exchange_price']:
                print(f"   🔍 All exchange price extraction methods failed.")
                
                # Try one more approach: look for any element containing exchange price patterns
                print(f"   🔁 Trying pattern-based search for exchange prices...")
                # Look for patterns like "₹X,XXX with exchange" or "Exchange: ₹X,XXX"
                price_patterns = [
                    r'₹[\d,]+.*exchange',
                    r'exchange.*₹[\d,]+',
                    r'with exchange.*₹[\d,]+',
                    r'₹[\d,]+.*with exchange'
                ]
                
                for pattern in price_patterns:
                    matches = soup.find_all(text=re.compile(pattern, re.IGNORECASE))
                    if matches:
                        for match in matches:
                            # Extract just the price part
                            price_match = re.search(r'₹[\d,]+', match)
                            if price_match:
                                result['with_exchange_price'] = price_match.group()
                                print(f"   🔁 Found exchange text using pattern '{pattern}': {price_match.group()}")
                                break
                        if result['with_exchange_price']:
                            break
                    
        except Exception as e:
            print(f"   ⚠️  Error extracting exchange text: {e}")

        # Keep exchange_amount for backward compatibility but set to None since we're not calculating
        result['exchange_amount'] = None
        
        # Summary of what was found
        if result['with_exchange_price']:
            print(f"   ✅ EXCHANGE PRICE EXTRACTION SUCCESSFUL: {result['with_exchange_price']}")
        else:
            print(f"   ❌ EXCHANGE PRICE EXTRACTION FAILED - No exchange price found")

        # 3. Check for sold out status: <div class="Z8JjpR">Sold Out</div>
        sold_out_found = False
        sold_out_element = soup.find('div', class_='Z8JjpR')
        if sold_out_element and 'sold out' in sold_out_element.get_text(strip=True).lower():
            sold_out_found = True
            print(f"   📢 Sold Out tag found: {sold_out_element.get_text(strip=True)}")
        
        # 4. Apply refined logic for in_stock determination
        if sold_out_found:
            # Rule 1: If "Sold Out" tag exists → in_stock = False
            result['in_stock'] = False
            print(f"   📦 Stock status: OUT OF STOCK (Sold Out tag found)")
        elif offers_found and not sold_out_found:
            # Rule 2: If bank offers found AND no "Sold Out" tag → in_stock = True
            result['in_stock'] = True
            print(f"   📦 Stock status: IN STOCK (Offers found + No Sold Out tag)")
        else:
            # Rule 3: Otherwise → in_stock = None (undetermined)
            result['in_stock'] = None
            print(f"   📦 Stock status: UNDETERMINED (No offers found or unclear status)")
        
        return result
        
    except (InvalidSessionIdException, WebDriverException) as e:
        # Bubble up so worker can recreate driver
        raise
    except Exception as e:
        print(f"   ⚠️  Error extracting price/stock: {e}")
        return {
            'price': None,
            'in_stock': None
        }

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

def get_flipkart_offers(driver, url, max_retries=2):
    """Enhanced Flipkart offers scraping"""
    for attempt in range(max_retries):
        try:
            logging.info(f"Visiting Flipkart URL (attempt {attempt + 1}/{max_retries}): {url}")
            driver.get(url)
            # Small settle; avoid long sleeps
            time.sleep(0.5 if FAST_MODE else 2)

            # Close login popup if it appears
            try:
                close_btn = WebDriverWait(driver, 3 if FAST_MODE else 6).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'✕')]"))
                )
                close_btn.click()
                if not FAST_MODE:
                    time.sleep(0.3)
            except TimeoutException:
                pass

            # Scroll to trigger offers
            driver.execute_script("window.scrollBy(0, 1200);")
            time.sleep(0.6 if FAST_MODE else 1.2)

            # Wait for offers section
            try:
                WebDriverWait(driver, 5 if FAST_MODE else 10).until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(text(),'Available offers')]"))
                )
            except TimeoutException:
                # Fallback: try a smaller scroll and shorter wait once
                try:
                    driver.execute_script("window.scrollBy(0, 800);")
                    WebDriverWait(driver, 3 if FAST_MODE else 6).until(
                        EC.presence_of_element_located((By.XPATH, "//div[contains(text(),'Available offers')]"))
                    )
                except TimeoutException:
                    if attempt < max_retries - 1:
                        continue
                    return []

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            offers = []
            
            # Find offers using multiple patterns
            offer_header = soup.find("div", string=lambda text: text and "Available offers" in text)
            if offer_header:
                parent = offer_header.find_parent("div")
                if parent:
                    offer_items = parent.find_all("li")
                    for item in offer_items:
                        text = item.get_text(" ", strip=True)
                        if text and len(text) > 10:
                            # Determine offer type based on content
                            offer_type = determine_offer_type_standalone(text)
                            
                            # Only add offers that are allowed
                            if offer_type in ["Flipkart Offer", "Bank Offer"]:
                                offers.append({
                                    "card_type": offer_type,
                                    "offer_title": "Available Offer",
                                    "description": text
                                })
                            else:
                                print(f"   🚫 Filtered out offer: {offer_type} - {text[:50]}...")

            # Remove duplicates
            unique_offers = []
            seen_descriptions = set()
            for offer in offers:
                desc = offer['description']
                if desc not in seen_descriptions and len(desc) > 15:
                    seen_descriptions.add(desc)
                    unique_offers.append(offer)

            # Log filtering summary
            total_offers_found = len(offers) + len([offer for offer in offers if offer.get('card_type') not in ["Flipkart Offer", "Bank Offer"]])
            filtered_out = total_offers_found - len(unique_offers)
            if filtered_out > 0:
                print(f"   📊 Offer filtering summary: {total_offers_found} total offers found, {filtered_out} filtered out, {len(unique_offers)} accepted")
                print(f"   ✅ Accepted offer types: Flipkart Offer, Bank Offer")
                print(f"   🚫 Filtered out offer types: Cashback, No Cost EMI, Partner Offer")

            logging.info(f"Extracted {len(unique_offers)} unique offers from {url} (filtered out unwanted types)")
            return unique_offers

        except (InvalidSessionIdException, WebDriverException) as e:
            # propagate invalid session so worker recreates driver
            raise
        except Exception as e:
            logging.error(f"Exception in get_flipkart_offers (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(3)
                continue
            else:
                return []
    
    return []

def create_chrome_driver():
    """
    Create and configure a new Chrome driver session for Flipkart scraping.
    Enhanced with resource management and proper cleanup configuration.
    """
    print("🤖 Running in headless server mode (no user interaction required)")
    
    options = uc.ChromeOptions()
    
    # Basic headless configuration
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_argument('--window-size=1920,1080')
    
    # Resource management optimizations
    options.add_argument('--max_old_space_size=512')  # Limit memory usage
    options.add_argument('--disable-background-timer-throttling')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--disable-backgrounding-occluded-windows')
    options.add_argument('--disable-client-side-phishing-detection')
    options.add_argument('--disable-default-apps')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins')
    options.add_argument('--disable-hang-monitor')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-prompt-on-repost')
    options.add_argument('--disable-sync')
    options.add_argument('--disable-translate')
    options.add_argument('--disable-web-resources')
    options.add_argument('--no-first-run')
    options.add_argument('--safebrowsing-disable-auto-update')
    
    # Memory and file handle optimizations
    options.add_argument('--memory-pressure-off')
    options.add_argument('--aggressive-cache-discard')
    options.add_argument('--disable-background-networking')
    
    # Anti-detection measures
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36')
    
    # Faster page load strategy (stop early after DOMContentLoaded)
    try:
        options.page_load_strategy = 'eager'
    except Exception:
        pass
    
    # Disable images/fonts/analytics to speed up
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 1,
        "profile.managed_default_content_settings.javascript": 1,
        "profile.managed_default_content_settings.fonts": 2,
        "profile.default_content_setting_values.plugins": 1,
        "profile.default_content_setting_values.popups": 1,
    }
    try:
        options.add_experimental_option('prefs', prefs)
    except Exception:
        pass
    
    # Note: Avoid experimental options that may be rejected in newer Chrome/Selenium combos
    # (e.g., 'useAutomationExtension' and 'excludeSwitches')
    
    try:
        driver = uc.Chrome(options=options)
        # Set timeouts to prevent hanging
        driver.set_page_load_timeout(20 if FAST_MODE else 30)
        driver.implicitly_wait(2 if FAST_MODE else 6)
        
        # Use CDP to block heavy resource types when possible
        try:
            driver.execute_cdp_cmd('Network.enable', {})
            # Block images, media, fonts, tracking
            blocked_types = ['Image', 'Media', 'Font', 'TextTrack', 'EventSource', 'Fetch', 'Preflight']
            driver.execute_cdp_cmd('Network.setBlockedURLS', { 'urls': ['*.png', '*.jpg', '*.jpeg', '*.gif', '*.webp', '*.svg', '*.mp4', '*.woff', '*.woff2', '*googletagmanager*', '*google-analytics*'] })
            driver.execute_cdp_cmd('Network.setCacheDisabled', { 'cacheDisabled': False })
        except Exception:
            pass
        return driver
    except Exception as e:
        logging.error(f"Failed to create Chrome driver: {e}")
        raise

def ensure_driver_alive(driver):
    """Return a valid driver; recreate if session is invalid."""
    try:
        # Try a benign call to trigger session check
        _ = driver.current_url  # may raise InvalidSessionIdException
        return driver
    except (InvalidSessionIdException, WebDriverException):
        try:
            try:
                driver.quit()
            except Exception:
                pass
            return create_chrome_driver()
        except Exception as e:
            logging.error(f"Failed to recreate Chrome driver: {e}")
            raise

def _process_single_flipkart_link(link_data: Dict[str, Any], analyzer: 'FlipkartOfferAnalyzer', visited_urls_file: str, driver) -> Dict[str, Any]:
    """Process a single Flipkart store link end-to-end using a provided driver. Returns stats dict."""
    stats = {
        'url': link_data.get('url'),
        'ranked_offers_count': 0,
        'had_offers': False,
        'error': None
    }
    try:
        # Small random jitter to avoid synchronized bursts
        time.sleep(random.uniform(0.2, 0.8))
        print(f"\n🔍 [T] Processing URL: {link_data['url']}")
    # Driver validity handled by worker
        store_link_ref = link_data['store_link_ref']
        existing_offers = 'ranked_offers' in store_link_ref and store_link_ref['ranked_offers']
        if existing_offers:
            print(f"   🔄 [T] Has existing offers, re-scraping")
        else:
            print(f"   🆕 [T] New link")

        offers = get_flipkart_offers(driver, link_data['url'])
        offers_found = bool(offers and len(offers) > 0)
        stats['had_offers'] = offers_found

        price_stock_info = extract_flipkart_price_and_stock(driver, link_data['url'], offers_found=offers_found)

        # Retry if stock undetermined
        retry_count = 0
        max_retries_for_undetermined = 2
        while price_stock_info['in_stock'] is None and retry_count < max_retries_for_undetermined:
            retry_count += 1
            time.sleep(1.2 if FAST_MODE else 3)
            offers = get_flipkart_offers(driver, link_data['url'])
            offers_found = bool(offers and len(offers) > 0)
            price_stock_info = extract_flipkart_price_and_stock(driver, link_data['url'], offers_found=offers_found)

        # Update price
        if price_stock_info.get('price'):
            store_link_ref['price'] = price_stock_info['price']

        # Update stock
        store_link_ref['in_stock'] = price_stock_info.get('in_stock')

        # platform_url
        try:
            store_link_ref['platform_url'] = driver.current_url
        except Exception:
            store_link_ref['platform_url'] = link_data['url']

        # exchange price
        if price_stock_info.get('with_exchange_price'):
            store_link_ref['with_exchange_price'] = price_stock_info['with_exchange_price']

        # product name
        if price_stock_info.get('product_name_via_url'):
            store_link_ref['product_name_via_url'] = price_stock_info['product_name_via_url']

        if offers:
            price_str = store_link_ref.get('price', '₹0')
            product_price = extract_price_amount(price_str)
            ranked_offers = analyzer.rank_offers(offers, product_price)
            store_link_ref['ranked_offers'] = ranked_offers
            stats['ranked_offers_count'] = len(ranked_offers)
        else:
            store_link_ref['ranked_offers'] = []

        append_visited_url(link_data['url'], visited_urls_file)

        return stats
    except Exception as e:
        stats['error'] = str(e)
        logging.error(f"Error processing single Flipkart link {link_data.get('url')}: {e}")
        return stats

def driver_worker(driver, task_queue: Queue, analyzer: 'FlipkartOfferAnalyzer', visited_urls_file: str, results: list, data_ref: Any, output_file: str):
    """Worker loop: consumes tasks and processes them with a shared driver."""
    while True:
        link_data = task_queue.get()
        if link_data is None:
            task_queue.task_done()
            break
        try:
            # Try up to 2 session recoveries for this task
            attempts = 0
            while True:
                try:
                    driver = ensure_driver_alive(driver)
                    stat = _process_single_flipkart_link(link_data, analyzer, visited_urls_file, driver)
                    results.append(stat)
                    break
                except (InvalidSessionIdException, WebDriverException) as e:
                    attempts += 1
                    logging.warning(f"Invalid driver session, recreating (attempt {attempts}) for URL: {link_data.get('url')}")
                    try:
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        time.sleep(min(2 * attempts, 5))
                        driver = create_chrome_driver()
                    except Exception as ce:
                        logging.error(f"Driver recreation failed: {ce}")
                        if attempts >= 2:
                            raise
                        continue
            # Update API status
            try:
                with STATUS_LOCK:
                    scraping_status['progress'] = len(results)
                    scraping_status['current_url'] = stat.get('url', '')
            except Exception:
                pass
            # Periodic snapshot every 25 processed
            if len(results) % 25 == 0:
                temp_backup = f"{output_file}.progress_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with FILE_SAVE_LOCK:
                    with open(temp_backup, 'w', encoding='utf-8') as f:
                        json.dump(data_ref, f, indent=2, ensure_ascii=False)
                print(f"   💾 Parallel progress saved to {temp_backup}")
        except Exception as e:
            logging.error(f"Worker error: {e}")
        finally:
            task_queue.task_done()


def process_comprehensive_flipkart_links(input_file="comprehensive_amazon_offers.json", 
                                       output_file="comprehensive_amazon_offers.json",
                                       flipkart_urls_file="visited_urls_flipkart.txt",
                                       max_workers: Optional[int] = None):
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
    # Initialize API progress totals (best-effort)
    try:
        with STATUS_LOCK:
            scraping_status['total'] = len(flipkart_links)
            scraping_status['progress'] = 0
            scraping_status['current_url'] = ''
    except Exception:
        pass
    
    # Initialize resource management
    print(f"🔧 Initializing resource management...")
    increase_file_limits()
    log_resource_usage("Initial system state - ")
    
    # Setup analyzer
    analyzer = FlipkartOfferAnalyzer()
    
    processed_count = 0
    new_offers_count = 0

    # Determine concurrency level (driver pool size)
    if max_workers is None:
        try:
            max_workers = min(8, multiprocessing.cpu_count() * 2)
        except Exception:
            max_workers = 6
    max_workers = max(1, int(max_workers))
    print(f"⚡ Driver pool size: {max_workers}")

    try:
        # Task queue and results list
        task_queue: Queue = Queue()
        results: list = []

        # Prime drivers
        print("🚗 Creating Chrome driver pool...")
        drivers = []
        for i in range(max_workers):
            try:
                drivers.append(create_chrome_driver())
            except Exception as e:
                logging.error(f"Failed to create driver {i}: {e}")
        if not drivers:
            raise RuntimeError("No Chrome drivers could be created")

        # Start worker threads
        threads = []
        for drv in drivers:
            t = threading.Thread(target=driver_worker, args=(drv, task_queue, analyzer, visited_urls_file, results, data, output_file), daemon=True)
            t.start()
            threads.append(t)

        # Enqueue tasks
        for link_data in flipkart_links:
            task_queue.put(link_data)

        # Wait for completion
        task_queue.join()

        # Stop workers
        for _ in drivers:
            task_queue.put(None)
        for t in threads:
            t.join(timeout=5)

        # Count results
        processed_count = len(results)
        new_offers_count = sum(int(r.get('ranked_offers_count', 0) or 0) for r in results)

        # Cleanup drivers
        print("🧹 Cleaning up driver pool...")
        for drv in drivers:
            try:
                drv.quit()
            except Exception:
                pass
    
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted! Saving progress...")
    
    finally:
        # No manual driver cleanup needed - context manager handles it
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

def run_flipkart_scraper_process(input_file="all_data.json", output_file=None, flipkart_urls_file="visited_urls_flipkart.txt", max_workers: Optional[int] = None):
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
        process_comprehensive_flipkart_links(input_file, output_file, flipkart_urls_file, max_workers=max_workers)
        
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
        max_workers = data.get('max_workers', None)
        if isinstance(max_workers, str):
            try:
                max_workers = int(max_workers)
            except ValueError:
                max_workers = None
        
        # Start scraping in a separate thread
        scraper_thread = threading.Thread(
            target=run_flipkart_scraper_process,
            args=(input_file, output_file, flipkart_urls_file, max_workers),
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
                'max_workers': max_workers,
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
    }), 200

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

if __name__ == "__main__":
    import sys
    
    # Check if script should run as API or direct execution
    if len(sys.argv) > 1 and sys.argv[1] == "--api":
        # Run as Flask API
        print("🚀 ENHANCED FLIPKART SCRAPER API MODE")
        print("Starting Flask API server...")
        print("Available endpoints:")
        print("  POST /start-scraping  - Start the Flipkart scraping process")
        print("  GET  /scraping-status - Get current scraping status")
        print("  POST /stop-scraping   - Stop the scraping process")
        print("  GET  /health         - Health check")
        print("  GET  /              - API information")
        print("-" * 60)
        
        # Get port from command line arguments or use default
        port = 5001  # Different port from Amazon scraper
        if len(sys.argv) > 2:
            try:
                port = int(sys.argv[2])
            except ValueError:
                print("Invalid port number, using default 5001")
                port = 5001
        
        print(f"🌐 Starting Flipkart API server on http://localhost:{port}")
        print(f"📖 Example usage:")
        print(f"   curl -X POST http://localhost:{port}/start-scraping")
        print(f"   curl -X GET http://localhost:{port}/scraping-status")
        print("-" * 60)
        
        # Run Flask app
        app.run(host='0.0.0.0', port=port, debug=False)
        
    else:
        # Run as direct script execution (original behavior)
        # Generate timestamped output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        input_file = "all_data.json"
        output_file = f"all_data_flipkart_{timestamp}.json"
        
        print("🚀 Enhanced Comprehensive Flipkart Scraper with Price & Stock Tracking")
        print("📍 Target: all_data.json")
        print("🔒 Amazon & Croma offers: COMPLETELY ISOLATED")
        print("🎯 Focus: ALL Flipkart links (re-scrapes everything)")
        print("🔍 Traversal: ALL nested locations (variants, all_matching_products, unmapped)")
        print("💰 NEW: Price extraction from Flipkart pages")
        print("📦 NEW: Refined stock status tracking with retry mechanism (in_stock: true/false/null)")
        print("📝 NEW: URL tracking in visited_urls_flipkart.txt")
        print("🔄 NEW: Smart session management (fresh session for each link)")
        print("🤖 NEW: Fully automated (headless, no user input)")
        print("🏆 Existing: Offer scraping and ranking")
        print()
        print("💡 TIP: Run with --api flag to start as API server instead:")
        print(f"   python {sys.argv[0]} --api [port]")
        print("-" * 80)
        
        # Auto-configuration: No user interaction required
        print("🚀 Starting automated processing with default settings:")
        print("   • Mode: Headless server mode")
        print("   • Start index: 0 (beginning)")
        print("   • Max entries: All available")
        print(f"   • Input file: {input_file}")
        print(f"   • Output file: {output_file}")
        print("   • Session management: Fresh session for each link")
        print("   • URL tracking: visited_urls_flipkart.txt")
        print()
        
        # Start processing immediately with default parameters
        process_comprehensive_flipkart_links(
            input_file=input_file,
            output_file=output_file
        ) 