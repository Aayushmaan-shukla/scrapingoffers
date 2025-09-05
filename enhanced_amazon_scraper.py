import os
import re
import json
import time
import glob
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from flask import Flask, request, jsonify
import threading
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import requests
import random
# Conditional import for resource module (Unix/Linux only)#################################################################################&&
try:
    import resource
    RESOURCE_AVAILABLE = True
except ImportError:
    RESOURCE_AVAILABLE = False

# Import psutil for thread monitoring
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("⚠️ psutil not available, thread monitoring will be limited")
import google.generativeai as genai

# ===============================================
# GOOGLE GEMINI API CONFIGURATION
# ===============================================

# Google Gemini API keys for analyzing descriptions
GEMINI_API_KEYS = [
    "AIzaSyA0mSe8Ty4dBlkh7cur7aHFtPA2uiqziN8",
    "AIzaSyChteT1w_MNjG49MQ8h09LtQF7oumQcRpQ",
    "AIzaSyD1r6qFYp8yywv6BLF_xzf8KFGEC6yoCZg"
]

# Initialize Gemini API
def initialize_gemini_api():
    """Initialize Gemini API with available keys."""
    for api_key in GEMINI_API_KEYS:
        try:
            genai.configure(api_key=api_key)
            # Test the API with a simple request
            model = genai.GenerativeModel('gemini-2.0-flash')
            response = model.generate_content("Hello")
            if response:
                logging.info(f"✅ Gemini API initialized successfully with key: {api_key[:20]}...")
                return True
        except Exception as e:
            logging.warning(f"⚠️ Failed to initialize Gemini API with key {api_key[:20]}...: {e}")
            continue
    
    logging.error("❌ All Gemini API keys failed to initialize")
    return False

def analyze_description_with_gemini(description: str, field_type: str) -> Optional[str]:
    """
    Use Google Gemini API to analyze description and extract missing bank or card_type information.
    
    Args:
        description: The offer description to analyze
        field_type: Either 'bank' or 'card_type'
    
    Returns:
        Extracted bank name or card type, or None if extraction failed
    """
    try:
        if not description or len(description.strip()) < 10:
            return None
        
        # Initialize Gemini API
        if not initialize_gemini_api():
            return None
        
        model = genai.GenerativeModel('gemini-2.0-flash')
        
        if field_type == 'bank':
            prompt = f"""
            Analyze this offer description and extract ONLY the bank name. 
            Return ONLY the bank name, no extra words, no explanations.
            If no bank or payment method is mentioned, return null.
            
            Description: {description}
            
            Bank name:"""
        elif field_type == 'card_type':
            prompt = f"""
            Analyze this offer description and extract ONLY the card type.
            Return ONLY: "Credit Card", "Debit Card", "Credit/Debit Card", or null if unclear.
            No extra words, no explanations.
            
            Description: {description}
            
            Card type:"""
        else:
            return None
        
        response = model.generate_content(prompt)
        if response and response.text:
            result = response.text.strip()
            
            # Clean up the response
            if result.lower() in ['null', 'none', 'n/a', 'not mentioned', 'unclear']:
                return None
            
            # For card_type, normalize the response
            if field_type == 'card_type':
                result_lower = result.lower()
                if 'credit' in result_lower and 'debit' in result_lower:
                    return "Credit/Debit Card"
                elif 'credit' in result_lower:
                    return "Credit Card"
                elif 'debit' in result_lower:
                    return "Debit Card"
                else:
                    return None
            
            # For bank, ensure it ends with "Bank" if it's a bank name
            if field_type == 'bank' and result and not result.endswith('Bank'):
                # Check if it's a known bank that should end with "Bank"
                known_banks = ['HDFC', 'ICICI', 'Axis', 'Kotak', 'IndusInd', 'Yes', 'IDFC', 'Federal', 'RBL', 'DCB', 'AU', 'Equitas', 'Ujjivan']
                if any(bank.lower() in result.lower() for bank in known_banks):
                    result += " Bank"
            
            logging.info(f"✅ Gemini API extracted {field_type}: '{result}' from description")
            return result
        
        return None
        
    except Exception as e:
        logging.error(f"❌ Error using Gemini API for {field_type} extraction: {e}")
        return None

# ===============================================
# ENHANCED AMAZON SCRAPER WITH PROXY ROTATION
# ===============================================
# 
# NEW FEATURES ADDED:
# 1. Automatic proxy rotation using ProxyScape API
# 2. Connection error detection and recovery
# 3. Resource management to prevent "Too many open files"
# 4. Automatic retry logic with different proxies
# 5. Load distribution across multiple connections
# 
# This scraper now automatically handles:
# - [Errno 24] Too many open files
# - Connection refused errors
# - Max retries exceeded
# - NewConnectionError
# - Connection broken errors
# 
# ===============================================

# ===============================================
# PROXY MANAGEMENT FOR ERROR HANDLING
# ===============================================

class ProxyManager:
    """
    Manages proxy rotation using ProxyScape API to handle connection errors and rate limiting.
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.proxyscape.com"
        self.proxies = []
        self.current_proxy_index = 0
        self.max_retries_per_proxy = 3
        self.proxy_retry_counts = {}
        
    def fetch_proxies(self) -> List[Dict[str, str]]:
        """Fetch available proxies from ProxyScape API."""
        try:
            url = f"{self.base_url}/v1/proxies"
            headers = {"Authorization": f"Bearer {self.api_key}"}
            
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle different possible API response formats
            if 'data' in data and isinstance(data['data'], list):
                self.proxies = data['data']
                logging.info(f"✅ Fetched {len(self.proxies)} proxies from ProxyScape")
                return self.proxies
            elif 'proxies' in data and isinstance(data['proxies'], list):
                self.proxies = data['proxies']
                logging.info(f"✅ Fetched {len(self.proxies)} proxies from ProxyScape")
                return self.proxies
            elif isinstance(data, list):
                self.proxies = data
                logging.info(f"✅ Fetched {len(self.proxies)} proxies from ProxyScape")
                return self.proxies
            else:
                logging.warning(f"⚠️ Unexpected API response format from ProxyScape: {data}")
                # Try to create fallback proxies for testing
                self.proxies = self._create_fallback_proxies()
                return self.proxies
                
        except Exception as e:
            logging.error(f"❌ Failed to fetch proxies from ProxyScape: {e}")
            # Create fallback proxies for testing
            self.proxies = self._create_fallback_proxies()
            return self.proxies
    
    def _create_fallback_proxies(self) -> List[Dict[str, str]]:
        """Create fallback proxies for testing when API is unavailable."""
        fallback_proxies = [
            {
                'host': '127.0.0.1',
                'port': '8080',
                'username': 'test',
                'password': 'test'
            }
        ]
        logging.warning("⚠️ Using fallback proxies due to API unavailability")
        return fallback_proxies
    
    def test_proxy_connectivity(self, proxy: Dict[str, str]) -> bool:
        """Test if a proxy is working by making a test request."""
        try:
            test_url = "http://httpbin.org/ip"
            timeout = 10
            
            response = requests.get(
                test_url, 
                proxies=proxy, 
                timeout=timeout,
                verify=False  # Disable SSL verification for testing
            )
            
            if response.status_code == 200:
                logging.info(f"✅ Proxy {proxy.get('http', 'unknown')} is working")
                return True
            else:
                logging.warning(f"⚠️ Proxy {proxy.get('http', 'unknown')} returned status {response.status_code}")
                return False
                
        except Exception as e:
            logging.warning(f"⚠️ Proxy {proxy.get('http', 'unknown')} test failed: {e}")
            return False
    
    def get_next_proxy(self) -> Optional[Dict[str, str]]:
        """Get the next available proxy in rotation."""
        if not self.proxies:
            self.fetch_proxies()
        
        if not self.proxies:
            return None
        
        # Try to find a working proxy
        attempts = 0
        max_attempts = len(self.proxies) * 2  # Try each proxy twice
        
        while attempts < max_attempts:
            # Get current proxy
            proxy = self.proxies[self.current_proxy_index]
            
            # Format proxy for Selenium
            formatted_proxy = {
                'http': f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
                'https': f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
            }
            
            # Test the proxy
            if self.test_proxy_connectivity(formatted_proxy):
                # Move to next proxy for next call
                self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)
                logging.info(f"🔄 Using working proxy: {proxy['host']}:{proxy['port']}")
                return formatted_proxy
            else:
                # Mark this proxy as failed and try the next one
                self.mark_proxy_failed(proxy)
                attempts += 1
                
                if not self.proxies:  # If all proxies were removed
                    logging.warning("⚠️ All proxies have been marked as failed")
                    return None
        
        logging.warning("⚠️ Could not find a working proxy after testing all available ones")
        return None
    
    def mark_proxy_failed(self, proxy: Dict[str, str]) -> None:
        """Mark a proxy as failed and potentially remove it from rotation."""
        proxy_key = f"{proxy['host']}:{proxy['port']}"
        self.proxy_retry_counts[proxy_key] = self.proxy_retry_counts.get(proxy_key, 0) + 1
        
        if self.proxy_retry_counts[proxy_key] >= self.max_retries_per_proxy:
            logging.warning(f"⚠️ Proxy {proxy_key} exceeded max retries, removing from rotation")
            # Remove failed proxy
            self.proxies = [p for p in self.proxies if f"{p['host']}:{p['port']}" != proxy_key]
            if self.proxies:
                self.current_proxy_index = self.current_proxy_index % len(self.proxies)
            else:
                self.current_proxy_index = 0

# Initialize proxy manager with your API key
PROXY_MANAGER = ProxyManager("wvm4z69kf54pc9rod7ck")

# Configuration for proxy usage
PROXY_CONFIG = {
    'enabled': True,  # Set to False to disable proxy usage
    'rotation_frequency': 10,  # Use proxy every N links
    'max_retries_per_proxy': 3,
    'connection_error_keywords': [
        'too many open files', 'connection refused', 'max retries exceeded',
        'newconnectionerror', 'connection broken', 'errno 24', 'errno 111'
    ]
}

# Configuration for data consistency checks
DATA_CONSISTENCY_CONFIG = {
    'enabled': True,  # Set to False to disable inconsistent data retry
    'retry_on_inconsistent_data': True,  # Retry when price unavailable but in_stock=True
    'max_retries_for_inconsistent_data': 1,  # Maximum retries for inconsistent data
    'inconsistent_data_keywords': [
        'price not available', 'price not found', 'currently unavailable'
    ]
}

# Enhanced error detection configuration
ERROR_DETECTION_CONFIG = {
    'chrome_driver_errors': [
        'chrome not reachable', 'session not created', 'invalid session id',
        'no such window', 'stale element reference', 'timeout', 'connection refused',
        'too many open files', 'errno 24', 'errno 111', 'max retries exceeded',
        'newconnectionerror', 'connection broken', 'webdriver exception',
        'selenium.common.exceptions', 'undetected_chromedriver'
    ],
    'http_connection_errors': [
        'connection refused', 'connection aborted', 'connection reset',
        'max retries exceeded', 'newconnectionerror', 'connection broken',
        'timeout', 'unreachable', 'network is unreachable'
    ],
    'file_descriptor_errors': [
        'too many open files', 'errno 24', 'resource temporarily unavailable'
    ],
    'session_errors': [
        'session not created', 'invalid session id', 'no such window',
        'stale element reference', 'chrome not reachable'
    ],
    'max_retry_errors': [
        'max retries exceeded', 'retry', 'timeout', 'connection'
    ]
}

# Thread management configuration
THREAD_MANAGEMENT_CONFIG = {
    'refresh_interval': 50,  # Refresh threads every N processed URLs
    'max_thread_usage': 0.8,  # Maximum thread usage threshold (80%)
    'cleanup_interval': 100,  # Cleanup resources every N processed URLs
    'driver_restart_interval': 200,  # Restart driver every N processed URLs
}

# Configuration for CAPTCHA handling
CAPTCHA_CONFIG = {
    'enabled': True,  # Set to False to disable CAPTCHA handling
    'auto_click_continue_shopping': True,  # Automatically click "Continue shopping" button
    'wait_after_captcha_click': 5,  # Seconds to wait after clicking CAPTCHA button
    'additional_wait_after_captcha': 2,  # Additional seconds to wait for page stabilization
}

# ===============================================
# RESOURCE MANAGEMENT FOR "TOO MANY OPEN FILES"
# ===============================================

def manage_file_descriptors():
    """
    Manage file descriptors to prevent "Too many open files" errors.
    This is especially important for long-running scraping sessions.
    """
    if RESOURCE_AVAILABLE:
        try:
            # Get current limits
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            print(f"📁 Current file descriptor limits - Soft: {soft}, Hard: {hard}")
            
            # Try to increase soft limit to hard limit
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
                print(f"✅ Increased file descriptor limit to {hard}")
            except ValueError:
                print(f"⚠️ Could not increase file descriptor limit (requires root privileges)")
                
        except Exception as e:
            print(f"⚠️ Error managing file descriptors: {e}")
    else:
        print("⚠️ Resource module not available (Windows system)")
        # On Windows, we'll rely on proxy rotation and session management
        print("   💡 Using proxy rotation and session management for resource control")

def cleanup_resources():
    """
    Clean up resources to prevent memory leaks and file descriptor exhaustion.
    """
    import gc
    gc.collect()
    print("🧹 Garbage collection completed")

def detect_error_type(error_message):
    """
    Detect the type of error based on error message patterns.
    
    Args:
        error_message: The error message string
        
    Returns:
        dict: Error type classification
    """
    error_str = str(error_message).lower()
    
    return {
        'is_chrome_driver_error': any(keyword in error_str for keyword in ERROR_DETECTION_CONFIG['chrome_driver_errors']),
        'is_http_connection_error': any(keyword in error_str for keyword in ERROR_DETECTION_CONFIG['http_connection_errors']),
        'is_file_descriptor_error': any(keyword in error_str for keyword in ERROR_DETECTION_CONFIG['file_descriptor_errors']),
        'is_session_error': any(keyword in error_str for keyword in ERROR_DETECTION_CONFIG['session_errors']),
        'is_max_retry_error': any(keyword in error_str for keyword in ERROR_DETECTION_CONFIG['max_retry_errors']),
        'requires_driver_restart': any(keyword in error_str for keyword in ERROR_DETECTION_CONFIG['chrome_driver_errors'] + ERROR_DETECTION_CONFIG['session_errors']),
        'requires_resource_cleanup': any(keyword in error_str for keyword in ERROR_DETECTION_CONFIG['file_descriptor_errors'])
    }

def restart_chrome_driver(driver, proxy=None, use_proxy=False):
    """
    Safely restart Chrome driver with proper cleanup.
    
    Args:
        driver: Current Chrome driver instance
        proxy: Proxy configuration dictionary
        use_proxy: Whether to use proxy configuration
        
    Returns:
        New Chrome driver instance
    """
    try:
        # Safely quit the current driver
        if driver:
            try:
                driver.quit()
                print("🔄 Previous Chrome driver closed")
                logging.info("🔄 Previous Chrome driver closed")
            except Exception as e:
                logging.warning(f"Error closing previous driver: {e}")
        
        # Clean up resources before creating new driver
        cleanup_resources()
        
        # Wait a bit before creating new driver
        time.sleep(3)
        
        # Create new driver
        new_driver = create_chrome_driver(proxy=proxy, use_proxy=use_proxy)
        
        print("✅ Chrome driver restarted successfully")
        logging.info("✅ Chrome driver restarted successfully")
        
        return new_driver
        
    except Exception as e:
        logging.error(f"❌ Failed to restart Chrome driver: {e}")
        print(f"❌ Failed to restart Chrome driver: {e}")
        raise e

def refresh_thread_resources():
    """
    Refresh thread resources to prevent thread consumption.
    """
    try:
        # Force garbage collection
        import gc
        gc.collect()
        
        # Clean up any hanging threads
        import threading
        for thread in threading.enumerate():
            if thread.is_alive() and thread.name != threading.current_thread().name:
                if hasattr(thread, '_stop'):
                    try:
                        thread._stop()
                    except:
                        pass
        
        print("🔄 Thread resources refreshed")
        logging.info("🔄 Thread resources refreshed")
        
    except Exception as e:
        logging.warning(f"Error refreshing thread resources: {e}")

def check_thread_usage():
    """
    Check current thread usage and return status.
    
    Returns:
        dict: Thread usage information
    """
    try:
        import threading
        import os
        
        if PSUTIL_AVAILABLE:
            # Get current process
            process = psutil.Process(os.getpid())
            
            # Get thread count
            thread_count = process.num_threads()
            
            # Get system thread limit (approximate)
            try:
                if hasattr(psutil, 'cpu_count'):
                    max_threads = psutil.cpu_count() * 4  # Conservative estimate
                else:
                    max_threads = 100  # Fallback
            except:
                max_threads = 100
        else:
            # Fallback when psutil is not available
            thread_count = len(threading.enumerate())
            max_threads = 100  # Conservative estimate
        
        thread_usage = thread_count / max_threads if max_threads > 0 else 0
        
        return {
            'thread_count': thread_count,
            'max_threads': max_threads,
            'usage_ratio': thread_usage,
            'is_high_usage': thread_usage > THREAD_MANAGEMENT_CONFIG['max_thread_usage']
        }
        
    except Exception as e:
        logging.warning(f"Error checking thread usage: {e}")
        return {
            'thread_count': 0,
            'max_threads': 100,
            'usage_ratio': 0,
            'is_high_usage': False
        }

# Use a local data directory by default; allow override via env var
DATA_DIR = os.getenv("SCRAPED_OFFER_DATA_DIR", os.path.join(os.getcwd(), "data"))
os.makedirs(DATA_DIR, exist_ok=True)
VISITED_URLS_FILE = os.path.join(DATA_DIR, "visited_urls.txt")

# Disk-backed cache for per-URL scraping results to avoid re-scraping duplicates
CACHE_FILE = os.path.join(DATA_DIR, "amazon_url_cache.json")

def load_url_cache(cache_file: str = CACHE_FILE) -> Dict[str, Dict]:
    """
    Load the URL result cache from disk.
    """
    try:
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                if isinstance(cache, dict):
                    logging.info(f"Loaded URL cache with {len(cache)} entries from {cache_file}")
                    return cache
    except Exception as e:
        logging.warning(f"Failed to load URL cache: {e}")
    return {}

def save_url_cache(cache: Dict[str, Dict], cache_file: str = CACHE_FILE) -> None:
    """
    Persist the URL result cache to disk atomically.
    """
    try:
        tmp_file = f"{cache_file}.tmp"
        with open(tmp_file, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
        os.replace(tmp_file, cache_file)
        logging.info(f"Saved URL cache with {len(cache)} entries to {cache_file}")
    except Exception as e:
        logging.warning(f"Failed to save URL cache: {e}")

def apply_cached_result_to_store_link(store_link: Dict, cached: Dict) -> None:
    """
    Apply cached scraping fields onto a store_link dict in the same format used by the script.
    """
    for key in [
        'price', 'in_stock', 'product_name_via_url', 'platform_url',
        'with_exchange_price', 'ranked_offers'
    ]:
        if key in cached:
            store_link[key] = cached[key]

def extract_store_link_snapshot(store_link: Dict) -> Dict:
    """
    Capture the subset of fields we cache for a processed URL.
    """
    snapshot = {}
    for key in [
        'price', 'in_stock', 'product_name_via_url', 'platform_url',
        'with_exchange_price', 'ranked_offers'
    ]:
        if key in store_link:
            snapshot[key] = store_link[key]
    return snapshot

def save_progress_and_pause(output_file: str, data: List[Dict], cache: Dict[str, Dict]) -> None:
    """
    Save current progress and cache to disk and pause (exit function) for stability.
    """
    try:
        progress_file = f"{output_file}.progress_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        ensure_parent_dir(progress_file)
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logging.error(f"Severe connection/resource errors. Progress saved to {progress_file}. Pausing run.")
        print(f"   💾 Progress saved to {progress_file}. Pausing run due to persistent errors.")
    except Exception as e:
        logging.error(f"Failed to save progress before pause: {e}")
    # Always attempt to persist the cache as well
    save_url_cache(cache)

# Setup logging (log file inside mounted volume)
logging.basicConfig(
    filename=os.path.join(DATA_DIR, 'enhanced_amazon_scraper.log'),
    filemode='a',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

# Path helpers
def resolve_input_path(preferred_path: str) -> str:
    """Prefer DATA_DIR path for relative inputs; fallback to working dir if missing."""
    if os.path.isabs(preferred_path):
        return preferred_path
    candidate = os.path.join(DATA_DIR, preferred_path)
    if os.path.exists(candidate):
        return candidate
    # No Docker-specific fallback; return the original path (resolved by caller's CWD)
    return preferred_path

def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

# ===============================================
# SCREENSHOT FUNCTIONALITY FOR DEBUGGING
# ===============================================

def create_screenshots_dir():
    """Create a screenshots directory for storing debug screenshots."""
    screenshots_dir = os.path.join(DATA_DIR, "screenshots")
    os.makedirs(screenshots_dir, exist_ok=True)
    return screenshots_dir

def take_screenshot(driver, url, action="debug", suffix=""):
    """
    Take a screenshot for debugging purposes.
    
    Args:
        driver: Selenium WebDriver instance
        url: The URL being processed
        action: Description of the action (e.g., "price_unavailable", "offers_not_found", "error")
        suffix: Additional suffix for the filename
    """
    try:
        screenshots_dir = create_screenshots_dir()
        
        # Create timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Clean URL for filename (remove special characters)
        clean_url = re.sub(r'[^\w\-_.]', '_', url)[:100]  # Limit length
        
        # Create filename
        filename = f"{action}_{clean_url}_{timestamp}{suffix}.png"
        filepath = os.path.join(screenshots_dir, filename)
        
        # Take screenshot
        driver.save_screenshot(filepath)
        
        # Log the screenshot
        logging.info(f"📸 Screenshot saved: {filepath} for {action} on {url}")
        print(f"📸 Screenshot saved: {filepath}")
        
        return filepath
        
    except Exception as e:
        logging.error(f"Failed to take screenshot for {action} on {url}: {e}")
        print(f"❌ Screenshot failed: {e}")
        return None



# ===============================================
# NEW FUNCTIONALITY: URL TRACKING AND PRICE/AVAILABILITY CHECKING
# ===============================================

def manage_visited_urls_file(file_path="visited_urls.txt"):
    """
    Check if visited_urls.txt exists, create it if not, and return the file path.
    """
    if not os.path.exists(file_path):
        print(f"📝 Creating new visited URLs file: {file_path}")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"# Visited URLs tracking file created on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        logging.info(f"Created new visited URLs file: {file_path}")
    else:
        print(f"📋 Using existing visited URLs file: {file_path}")
        logging.info(f"Using existing visited URLs file: {file_path}")
    
    return file_path

def load_visited_urls(file_path="visited_urls.txt"):
    """
    Load visited URLs from the tracking file.
    """
    visited_urls = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    visited_urls.add(line)
        logging.info(f"Loaded {len(visited_urls)} visited URLs from {file_path}")
    except FileNotFoundError:
        logging.warning(f"Visited URLs file not found: {file_path}")
    
    return visited_urls

def append_visited_url(url, file_path="visited_urls.txt"):
    """
    Append a new URL to the visited URLs file.
    """
    try:
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(f"{url}\n")
        logging.info(f"Added URL to visited list: {url}")
    except Exception as e:
        logging.error(f"Error adding URL to visited list: {e}")

def extract_price_from_page(driver, url):
    """
    Extract price from an Amazon product page using the span class patterns we analyzed.
    Returns the price string if found, otherwise returns None.
    """
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Look for price using the a-price-whole class pattern
        price_elements = soup.find_all('span', class_='a-price-whole')
        
        if price_elements:
            # Try to find the most prominent price (usually the first one in main content)
            for price_elem in price_elements:
                price_text = price_elem.get_text(strip=True)
                if price_text and re.search(r'\d+', price_text):
                    # Clean up the price text
                    price_clean = re.sub(r'[^\d,.]', '', price_text)
                    if price_clean:
                        logging.info(f"Extracted price: {price_text} from {url}")
                        return price_text
        
        # Fallback: look for other price patterns
        price_patterns = [
            soup.find('span', class_='a-price a-text-price a-size-medium apexPriceToPay'),
            soup.find('span', class_='a-price aok-align-center reinventPricePriceToPayMargin priceToPay'),
            soup.find('span', {'data-a-size': 'xl', 'class': 'a-price'})
        ]
        
        for price_container in price_patterns:
            if price_container:
                price_whole = price_container.find('span', class_='a-price-whole')
                if price_whole:
                    price_text = price_whole.get_text(strip=True)
                    if price_text and re.search(r'\d+', price_text):
                        logging.info(f"Extracted price (fallback): {price_text} from {url}")
                        return price_text
        
        logging.warning(f"No price found on page: {url}")
        return None
        
    except Exception as e:
        logging.error(f"Error extracting price from {url}: {e}")
        return None

def check_availability_status(driver, url):
    """
    Check if the product is available or shows "Currently unavailable" message.
    Returns the availability status.
    Includes retry logic: attempts once more with 5-second delay if exception occurs.
    """
    def _perform_availability_check():
        """Internal function to perform the actual availability check."""
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Look for the "Currently unavailable" span we analyzed
        unavailable_elements = soup.find_all('span', class_='a-size-medium a-color-success')
        
        for elem in unavailable_elements:
            text = elem.get_text(strip=True)
            if 'currently unavailable' in text.lower():
                logging.info(f"Product unavailable: {text} from {url}")
                return "Currently unavailable"
        
        # Look for other availability indicators
        availability_patterns = [
            soup.find('div', id='availability'),
            soup.find('span', class_='a-color-success'),
            soup.find('span', class_='a-color-base'),
        ]
        
        for avail_elem in availability_patterns:
            if avail_elem:
                text = avail_elem.get_text(strip=True).lower()
                if any(phrase in text for phrase in ['currently unavailable', 'out of stock', 'temporarily unavailable']):
                    logging.info(f"Product unavailable (pattern match): {text} from {url}")
                    return "Currently unavailable"
                elif any(phrase in text for phrase in ['in stock', 'available', 'add to cart']):
                    logging.info(f"Product available: {text} from {url}")
                    return "Available"
        
        # If no clear unavailability message found, assume available
        logging.info(f"Availability status unclear, assuming available for {url}")
        return "Available"
    
    # First attempt
    try:
        return _perform_availability_check()
        
    except Exception as e:
        logging.warning(f"First attempt failed checking availability for {url}: {e}")
        logging.info(f"Attempting retry with 5-second delay for {url}")
        
        # Retry attempt with 5-second delay
        try:
            time.sleep(5)  # 5-second delay as requested
            logging.info(f"Retrying availability check for {url} after 5-second delay")
            
            # Re-load the page for fresh content
            driver.get(url)
            time.sleep(3)  # Wait for page to load
            
            # Perform the availability check again
            result = _perform_availability_check()
            logging.info(f"Retry successful for availability check: {url}")
            return result
            
        except Exception as retry_e:
            logging.error(f"Retry attempt also failed for availability check {url}: {retry_e}")
            logging.info(f"Moving ahead after retry failure for {url}")
            return "Unknown"

def extract_price_and_availability(driver, url):
    """
    Main function to extract both price and availability from an Amazon product page.
    Returns a dictionary with price and availability information including in_stock status.
    """
    try:
        logging.info(f"Extracting price and availability from: {url}")
        
        # Load the page
        driver.get(url)
        time.sleep(3)  # Wait for page to load
        
        # Check for CAPTCHA page and handle it if present
        captcha_handled = check_and_handle_captcha(driver, url)
        if captcha_handled:
            # If CAPTCHA was handled, we need to get fresh page source
            time.sleep(CAPTCHA_CONFIG['additional_wait_after_captcha'])  # Additional wait for page to stabilize
            logging.info(f"CAPTCHA handled for {url}, proceeding with extraction...")
        
        # Get page soup for element checking
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Initialize variables
        price = None
        in_stock = True  # Default to in stock
        availability = "Available"  # Default availability
        product_name = None
        
        # Extract product name from the productTitle span
        product_title_element = soup.find('span', id='productTitle')
        if product_title_element:
            product_name = product_title_element.get_text(strip=True)
            logging.info(f"Product name extracted: {product_name[:100]}... from {url}")
        else:
            logging.info(f"Product title element not found for {url}")
        
        # Check for unavailable status first (class 'a-size-medium a-color-success')
        unavailable_elements = soup.find_all('span', class_='a-size-medium a-color-success')
        for elem in unavailable_elements:
            text = elem.get_text(strip=True).lower()
            if 'currently unavailable' in text or 'out of stock' in text:
                # Found unavailable indicator - don't update price, set in_stock = false
                in_stock = False
                availability = "Currently unavailable"
                price = "Currently unavailable"
                logging.info(f"Product unavailable (a-size-medium a-color-success): {text} from {url}")
                
                # Take screenshot when price is unavailable
                take_screenshot(driver, url, "price_unavailable", "_unavailable")
                break
        
                # Check for price availability (class 'a-price-whole')
        if in_stock:  # Only check for price if product is in stock
            price_elements = soup.find_all('span', class_='a-price-whole')
            if price_elements:
                # Found price element - update price and set in_stock = true
                price = extract_price_from_page(driver, url)
                if price:
                    in_stock = True
                    availability = "Available"
                    logging.info(f"Price found (a-price-whole exists), in_stock = true for {url}")
                else:
                    price = "Price not found"
            else:
                # No price element found
                price = "Price not found"
                availability = "Price not available"
                
                # Take screenshot when no price is found
                take_screenshot(driver, url, "price_not_found", "_no_price")
        
        # If no specific conditions met, use fallback availability check
        if availability == "Available" and not price:
            availability = check_availability_status(driver, url)
            if availability == "Currently unavailable":
                in_stock = False
                price = "Currently unavailable"
        
        result = {
            'price': price if price else "Price not found",
            'availability': availability,
            'in_stock': in_stock,
            'extracted_at': datetime.now().isoformat(),
            'product_name_via_url': product_name
        }
        
        logging.info(f"Extraction result for {url}: {result}")
        return result
        
    except Exception as e:
        logging.error(f"Error in extract_price_and_availability for {url}: {e}")
        return {
            'price': "Error extracting price",
            'availability': "Error checking availability",
            'in_stock': False,
            'extracted_at': datetime.now().isoformat(),
            'error': str(e),
            'product_name_via_url': None
        }

# Import ranking logic from improved_offervalue.py
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

class OfferAnalyzer:
    def __init__(self):
        # Comprehensive bank reputation scores for Indian banks - All names end with "Bank"
        self.bank_scores = {
            # Public Sector Banks (PSBs)
            "State Bank of India": 75,
            "Punjab National Bank": 72,
            "Bank of Baroda": 70,
            "Canara Bank": 68,
            "Union Bank of India": 65,
            "Indian Bank": 65,
            "Bank of India": 65,
            "UCO Bank": 62,
            "Indian Overseas Bank": 62,
            "Central Bank of India": 62,
            "Bank of Maharashtra": 60,
            "Punjab & Sind Bank": 60,
            
            # Private Sector Banks
            "HDFC Bank": 85,
            "ICICI Bank": 90,
            "Axis Bank": 80,
            "Kotak Mahindra Bank": 70,
            "IndusInd Bank": 68,
            "Yes Bank": 60,
            "IDFC FIRST Bank": 65,
            "Federal Bank": 63,
            "South Indian Bank": 60,
            "RBL Bank": 62,
            "DCB Bank": 60,
            "Tamilnad Mercantile Bank": 58,
            "Karur Vysya Bank": 58,
            "CSB Bank": 58,
            "City Union Bank": 58,
            "Bandhan Bank": 60,
            "Jammu & Kashmir Bank": 58,
            
            # Small Finance Banks
            "AU Small Finance Bank": 65,
            "Equitas Small Finance Bank": 62,
            "Ujjivan Small Finance Bank": 60,
            "Suryoday Small Finance Bank": 58,
            "ESAF Small Finance Bank": 58,
            "Fincare Small Finance Bank": 58,
            "Jana Small Finance Bank": 58,
            "North East Small Finance Bank": 58,
            "Capital Small Finance Bank": 58,
            "Unity Small Finance Bank": 58,
            "Shivalik Small Finance Bank": 58,
            
            # Foreign Banks
            "Citibank": 80,
            "HSBC Bank": 78,
            "Standard Chartered Bank": 75,
            "Deutsche Bank": 75,
            "Barclays Bank": 75,
            "DBS Bank": 72,
            "JP Morgan Chase Bank": 75,
            "Bank of America": 75,
            
            # Co-operative Banks
            "Saraswat Co-operative Bank": 60,
            "Shamrao Vithal Co-operative Bank": 55,
            "PMC Bank": 50,
            "TJSB Sahakari Bank": 55,
            
            # Credit Card Companies
            "American Express": 85,

            #digital payments
            "PhonePe": 82,
            "Google Pay": 88,
            "Paytm": 75,
            "BHIM": 70,
            "Amazon Pay": 85,
            "MobiKwik": 68,
            "Freecharge": 65,
            "Airtel Payments Bank": 72,
            "JioMoney": 70,
            "PayZapp": 68,
            "CRED": 80,
            "Navi": 75,
            "WhatsApp Pay": 85,
            "YONO by SBI": 75,
            "Ola Money": 70,
            "Slice": 75,
            "Pockets by ICICI Bank": 78,
            "Super Money": 65,
            "Freo": 70,
            "Jupiter": 75,
            "InstantPay": 68,
            "FamPay": 70,
            "Finin": 72,
            "OneCard": 78,
            "UPI": 85
        }
        
        # Enhanced bank name patterns for better matching - All should end with "Bank"
        self.bank_name_patterns = {
            # Public Sector Banks (PSBs)
            "State Bank of India": ["SBI", "State Bank", "State Bank of India", "SBI Bank"],
            "Punjab National Bank": ["PNB", "Punjab National Bank", "PNB Bank"],
            "Bank of Baroda": ["BoB", "Bank of Baroda", "Baroda", "BoB Bank"],
            "Canara Bank": ["Canara", "Canara Bank"],
            "Union Bank of India": ["Union Bank", "Union Bank of India"],
            "Indian Bank": ["Indian Bank"],
            "Bank of India": ["Bank of India"],
            "UCO Bank": ["UCO", "UCO Bank"],
            "Indian Overseas Bank": ["IOB", "Indian Overseas Bank", "IOB Bank"],
            "Central Bank of India": ["Central Bank", "Central Bank of India"],
            "Bank of Maharashtra": ["Bank of Maharashtra", "Maharashtra Bank"],
            "Punjab & Sind Bank": ["Punjab & Sind Bank"],
            
            # Private Sector Banks
            "HDFC Bank": ["HDFC", "HDFC Bank"],
            "ICICI Bank": ["ICICI", "ICICI Bank"],
            "Axis Bank": ["Axis", "Axis Bank"],
            "Kotak Mahindra Bank": ["Kotak", "Kotak Mahindra", "Kotak Mahindra Bank"],
            "IndusInd Bank": ["IndusInd", "IndusInd Bank"],
            "Yes Bank": ["Yes Bank", "YES Bank"],
            "IDFC FIRST Bank": ["IDFC", "IDFC FIRST", "IDFC Bank"],
            "Federal Bank": ["Federal", "Federal Bank"],
            "South Indian Bank": ["South Indian Bank"],
            "RBL Bank": ["RBL", "RBL Bank"],
            "DCB Bank": ["DCB Bank"],
            "Tamilnad Mercantile Bank": ["TMB", "Tamilnad Mercantile Bank"],
            "Karur Vysya Bank": ["Karur Vysya Bank"],
            "CSB Bank": ["CSB Bank"],
            "City Union Bank": ["City Union Bank"],
            "Bandhan Bank": ["Bandhan Bank"],
            "Jammu & Kashmir Bank": ["Jammu & Kashmir Bank"],
            
            # Small Finance Banks
            "AU Small Finance Bank": ["AU Bank", "AU Small Finance", "AU", "AU Small Finance Bank"],
            "Equitas Small Finance Bank": ["Equitas", "Equitas Bank", "Equitas Small Finance Bank"],
            "Ujjivan Small Finance Bank": ["Ujjivan", "Ujjivan Bank", "Ujjivan Small Finance Bank"],
            "Suryoday Small Finance Bank": ["Suryoday Small Finance Bank"],
            "ESAF Small Finance Bank": ["ESAF Small Finance Bank"],
            "Fincare Small Finance Bank": ["Fincare Small Finance Bank"],
            "Jana Small Finance Bank": ["Jana Small Finance Bank"],
            "North East Small Finance Bank": ["North East Small Finance Bank"],
            "Capital Small Finance Bank": ["Capital Small Finance Bank"],
            "Unity Small Finance Bank": ["Unity Small Finance Bank"],
            "Shivalik Small Finance Bank": ["Shivalik Small Finance Bank"],
            
            # Foreign Banks
            "Citibank": ["Citi", "Citibank", "CitiBank"],
            "HSBC Bank": ["HSBC", "HSBC Bank"],
            "Standard Chartered Bank": ["Standard Chartered", "StanChart", "SC Bank", "Standard Chartered Bank"],
            "Deutsche Bank": ["Deutsche Bank"],
            "Barclays Bank": ["Barclays Bank"],
            "DBS Bank": ["DBS", "DBS Bank"],
            "JP Morgan Chase Bank": ["JP Morgan Chase Bank"],
            "Bank of America": ["Bank of America"],
            
            # Co-operative Banks
            "Saraswat Co-operative Bank": ["Saraswat Co-operative Bank"],
            "Shamrao Vithal Co-operative Bank": ["Shamrao Vithal Co-operative Bank"],
            "PMC Bank": ["PMC Bank"],
            "TJSB Sahakari Bank": ["TJSB Sahakari Bank"],
            
            # Credit Card Companies
            "American Express": ["Amex", "American Express"],
            "PhonePe": ["PhonePe", "Phone Pe", "Phonepe"],
            "Google Pay": ["Google Pay", "GooglePay", "G Pay", "GPay"],
            "Paytm": ["Paytm", "PayTM", "PAYTM"],
            "BHIM": ["BHIM", "Bharat Interface for Money"],
            "Amazon Pay": ["Amazon Pay", "AmazonPay", "Amazon Pay UPI"],
            "MobiKwik": ["MobiKwik", "Mobikwik", "Mobi Kwik"],
            "Freecharge": ["Freecharge", "Free Charge", "FreeCharge"],
            "Airtel Payments Bank": ["Airtel Payments Bank", "Airtel Bank", "Airtel Payments"],
            "JioMoney": ["JioMoney", "Jio Money", "Jio"],
            "PayZapp": ["PayZapp", "Pay Zapp", "Payzapp"],
            "CRED": ["CRED", "Cred"],
            "Navi": ["Navi", "NAVI"],
            "WhatsApp Pay": ["WhatsApp Pay", "WhatsAppPay", "WhatsApp"],
            "YONO by SBI": ["YONO by SBI", "YONO", "Yono", "YONO SBI"],
            "Ola Money": ["Ola Money", "OlaMoney", "Ola"],
            "Slice": ["Slice", "SLICE"],
            "Pockets by ICICI Bank": ["Pockets by ICICI Bank", "Pockets", "ICICI Pockets"],
            "Super Money": ["Super Money", "SuperMoney"],
            "Freo": ["Freo", "FREO"],
            "Jupiter": ["Jupiter", "JUPITER"],
            "InstantPay": ["InstantPay", "Instant Pay", "Instantpay"],
            "FamPay": ["FamPay", "Fam Pay", "Fampay"],
            "Finin": ["Finin", "FININ"],
            "OneCard": ["OneCard", "One Card", "Onecard"],
            "UPI": ["UPI", "upi", "Unified Payments Interface"]

        }
        
        # Card providers list
        self.card_providers = [
            "Visa", "Mastercard", "RuPay", "American Express", "Amex", 
            "Diners Club", "Discover", "UnionPay", "JCB", "Maestro", 
            "Cirrus", "PLUS"
        ]
        
        # Default bank score if not found in the list
        self.default_bank_score = 70

    def extract_card_type(self, description: str) -> Optional[str]:
        """Extract card type (Credit Card/Debit Card) from offer description with enhanced detection."""
        description_lower = description.lower()
        
        # Enhanced patterns for better detection
        credit_patterns = [
            r'\bcredit\s+card\b', r'\bcc\b', r'\bcredit\b.*\bcard\b',
            r'\bmaster\s+card\b.*\bcredit\b', r'\bvisa\s+card\b.*\bcredit\b'
        ]
        
        debit_patterns = [
            r'\bdebit\s+card\b', r'\bdc\b', r'\bdebit\b.*\bcard\b',
            r'\bvisa\s+card\b.*\bdebit\b', r'\bmaster\s+card\b.*\bdebit\b'
        ]
        
        # Check for credit card patterns FIRST (more specific)
        credit_match = any(re.search(pattern, description_lower) for pattern in credit_patterns)
        # Check for debit card patterns
        debit_match = any(re.search(pattern, description_lower) for pattern in debit_patterns)
        
        # If both credit and debit are explicitly mentioned, return "Credit/Debit Card"
        if credit_match and debit_match:
            return "Credit/Debit Card"
        
        # If only credit is mentioned, return "Credit Card"
        if credit_match:
            return "Credit Card"
        
        # If only debit is mentioned, return "Debit Card"
        if debit_match:
            return "Debit Card"
        
        # Only use "Credit/Debit Card" for ambiguous cases like:
        # - "all cards", "bank card", "card", "cards"
        # - "credit & debit", "credit and debit", "credit/debit", "credit or debit"
        # - "both credit and debit"
        ambiguous_patterns = [
            'credit & debit', 'credit and debit', 'credit/debit', 'credit or debit', 
            'both credit and debit', 'all cards', 'bank card', 'card', 'cards'
        ]
        
        if any(phrase in description_lower for phrase in ambiguous_patterns):
            return "Credit/Debit Card"
        
        # If card is mentioned but no explicit credit/debit, and it's a bank offer context
        # Only then treat as Credit/Debit Card to avoid false positives
        has_card_word = re.search(r'\bcard(s)?\b', description_lower) is not None
        has_bank_offer_word = 'bank offer' in description_lower or ('bank' in description_lower and 'offer' in description_lower)
        mentions_provider = any(p.lower() in description_lower for p in self.card_providers)
        
        # Only use Credit/Debit Card if we have strong evidence it's a bank offer
        if has_card_word and (has_bank_offer_word or mentions_provider):
            return "Credit/Debit Card"
        
        return None
    def extract_card_provider(self, description: str) -> Optional[str]:
        """Extract card provider from offer description with enhanced matching."""
        description_lower = description.lower()
        
        # Enhanced provider matching with context
        for provider in self.card_providers:
            # Direct match
            if provider.lower() in description_lower:
                return provider
            
            # Special cases for common variations
            if provider == "Mastercard" and "master" in description_lower:
                return "Mastercard"
            elif provider == "RuPay" and "rupay" in description_lower:
                return "RuPay"
        
        return None

    def extract_amount(self, description: str) -> float:
        """Extract numerical amount from offer description with enhanced patterns."""
        try:
            # Enhanced flat discount patterns
            flat_patterns = [
                r'(?:Additional\s+)?[Ff]lat\s+(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
                r'(?:Additional\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)\s+(?:Instant\s+)?Discount',
                r'(?:Get\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)\s+(?:off|discount)',
                r'(?:Save\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
                r'₹\s*([\d,]+\.?\d*)'
            ]
            
            for pattern in flat_patterns:
                match = re.search(pattern, description, re.IGNORECASE)
                if match:
                    amount = float(match.group(1).replace(',', ''))
                    logging.info(f"Extracted amount: ₹{amount} using pattern: {pattern[:30]}...")
                    return amount
            
            # Handle percentage discounts with caps
            percent_patterns = [
                r'([\d.]+)%\s+(?:Instant\s+)?Discount\s+up\s+to\s+(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
                r'Up\s+to\s+([\d.]+)%\s+(?:off|discount).*?(?:max|maximum|up\s+to)\s+(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
                r'([\d.]+)%\s+(?:off|discount).*?(?:capped\s+at|maximum)\s+(?:INR\s+|₹\s*)([\d,]+\.?\d*)'
            ]
            
            for pattern in percent_patterns:
                match = re.search(pattern, description, re.IGNORECASE)
                if match:
                    cap_amount = float(match.group(2).replace(',', ''))
                    logging.info(f"Extracted capped amount: ₹{cap_amount} from percentage offer")
                    return cap_amount
            
            # Handle cashback patterns
            cashback_patterns = [
                r'(?:Get\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)\s+(?:cashback|cash\s+back)',
                r'(?:Earn\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)\s+(?:cashback|cash\s+back)'
            ]
            
            for pattern in cashback_patterns:
                match = re.search(pattern, description, re.IGNORECASE)
                if match:
                    amount = float(match.group(1).replace(',', ''))
                    logging.info(f"Extracted cashback amount: ₹{amount}")
                    return amount
            
            return 0.0
        except (ValueError, AttributeError) as e:
            logging.warning(f"Error extracting amount from '{description[:50]}...': {e}")
            return 0.0

    def extract_bank(self, description: str) -> Optional[str]:
        """Extract bank name from offer description with enhanced matching using entire description."""
        if not description:
            return None
        
        description_lower = description.lower()
        
        # First, try exact matches with bank name patterns (longest first to avoid partial matches)
        for bank_key, patterns in self.bank_name_patterns.items():
            for pattern in patterns:
                if pattern.lower() in description_lower:
                    logging.info(f"Found bank '{bank_key}' using pattern '{pattern}' in description")
                    return bank_key  # This will always end with "Bank" as per our updated patterns
        
        # If no pattern match, try direct bank scores dictionary (all end with "Bank")
        sorted_banks = sorted(self.bank_scores.keys(), key=len, reverse=True)
        for bank in sorted_banks:
            if bank.lower() in description_lower:
                logging.info(f"Found bank '{bank}' through direct matching in description")
                return bank  # This will always end with "Bank"
        
        # Enhanced pattern matching for common bank variations - ensure all return "Bank" suffix
        bank_variations = {
            'hdfc': 'HDFC Bank',
            'icici': 'ICICI Bank', 
            'axis': 'Axis Bank',
            'sbi': 'State Bank of India',
            'kotak': 'Kotak Mahindra Bank',
            'yes': 'Yes Bank',
            'idfc': 'IDFC FIRST Bank',
            'indusind': 'IndusInd Bank',
            'federal': 'Federal Bank',
            'rbl': 'RBL Bank',
            'citi': 'Citibank',
            'hsbc': 'HSBC Bank',
            'standard chartered': 'Standard Chartered Bank',
            'au bank': 'AU Small Finance Bank',
            'equitas': 'Equitas Small Finance Bank',
            'ujjivan': 'Ujjivan Small Finance Bank',
            'pnb': 'Punjab National Bank',
            'bob': 'Bank of Baroda',
            'canara': 'Canara Bank',
            'union bank': 'Union Bank of India',
            'indian bank': 'Indian Bank',
            'bank of india': 'Bank of India',
            'uco': 'UCO Bank',
            'iob': 'Indian Overseas Bank',
            'central bank': 'Central Bank of India',
            'amex': 'American Express',
            'american express': 'American Express'
        }
        
        for variation, standard_name in bank_variations.items():
            if variation in description_lower:
                logging.info(f"Found bank '{standard_name}' using variation '{variation}' in description")
                return standard_name  # This will always end with "Bank" or be a proper company name
        
        logging.debug(f"No bank found in description: {description[:100]}...")
        return None

    def extract_validity(self, description: str) -> Optional[str]:
        """Extract validity period from offer description with enhanced patterns."""
        validity_patterns = [
            r'valid\s+(?:till|until|up\s+to)\s+([^,\.;]+)',
            r'offer\s+valid\s+(?:till|until|up\s+to)\s+([^,\.;]+)',
            r'expires?\s+(?:on|by)?\s+([^,\.;]+)',
            r'valid\s+(?:from|between).*?(?:to|till|until)\s+([^,\.;]+)',
            r'(?:validity|valid)\s*:\s*([^,\.;]+)'
        ]
        
        for pattern in validity_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                validity = match.group(1).strip()
                logging.info(f"Extracted validity: {validity}")
                return validity
        
        return None

    def extract_min_spend(self, description: str) -> Optional[float]:
        """Extract minimum spend requirement from offer description with enhanced patterns."""
        # Enhanced patterns to catch different formats
        patterns = [
            r'(?:Mini|Minimum)\s+purchase\s+value\s+(?:of\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
            r'(?:Mini|Minimum)\s+(?:purchase|spend|transaction)\s+(?:of\s+|value\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
            r'min(?:imum)?\s+(?:purchase|spend|transaction)\s+(?:of\s+|value\s+)?(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
            r'valid\s+on\s+(?:orders?|purchases?)\s+(?:of\s+|above\s+|worth\s+)(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
            r'applicable\s+on\s+(?:purchases?|orders?|transactions?)\s+(?:of\s+|above\s+|worth\s+)(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
            r'(?:on\s+)?(?:orders?|purchases?|spending)\s+(?:of\s+|above\s+|worth\s+)(?:INR\s+|₹\s*)([\d,]+\.?\d*)\s+(?:or\s+more|and\s+above)',
            r'(?:minimum|min)\s+(?:spend|purchase|order)\s*:\s*(?:INR\s+|₹\s*)([\d,]+\.?\d*)',
            r'(?:spend|purchase|order)\s+(?:minimum|min|at\s+least)\s+(?:INR\s+|₹\s*)([\d,]+\.?\d*)'
        ]
        
        for pattern in patterns:
            min_spend_match = re.search(pattern, description, re.IGNORECASE)
            if min_spend_match:
                try:
                    extracted_value = float(min_spend_match.group(1).replace(',', ''))
                    logging.info(f"Extracted min_spend: ₹{extracted_value} from: {description[:100]}...")
                    return extracted_value
                except ValueError:
                    continue
        
        logging.debug(f"No min_spend found in: {description[:100]}...")
        return None

    def determine_offer_type(self, card_title: str, description: str) -> str:
        """Determine offer type based on card title and description."""
        card_title_lower = card_title.lower() if card_title else ""
        description_lower = description.lower() if description else ""
        
        # Enhanced type detection
        if any(keyword in card_title_lower for keyword in ['bank offer', 'instant discount', 'card offer']):
            return "Bank Offer"
        elif any(keyword in card_title_lower for keyword in ['no cost emi', 'no-cost emi', 'emi']):
            return "No Cost EMI"
        elif any(keyword in card_title_lower for keyword in ['cashback', 'cash back']):
            return "Cashback"
        elif any(keyword in card_title_lower for keyword in ['partner offer', 'partner']):
            return "Partner Offers"
        elif any(keyword in description_lower for keyword in ['bank', 'credit card', 'debit card']):
            return "Bank Offer"  # Fallback for bank-related offers
        else:
            return card_title if card_title else "Other Offer"

    def parse_offer(self, offer: Dict[str, str]) -> Offer:
        """Parse offer details from raw offer data with enhanced processing."""
        card_title = offer.get('card_type', '').strip()
        description = offer.get('offer_description', '').strip()
        
        # Determine offer type
        offer_type = self.determine_offer_type(card_title, description)
        
        # Ensure title is robust in all cases, especially when cards appear after other sections
        normalized_card_title = card_title.lower()
        if not card_title or normalized_card_title in ['summary', '']:
            title = offer_type or 'Offer'
        else:
            title = card_title
        
        # Extract offer details
        amount = self.extract_amount(description)
        bank = self.extract_bank(description)  # This now uses entire description
        validity = self.extract_validity(description)
        min_spend = self.extract_min_spend(description)
        card_type = self.extract_card_type(description)
        # Replace any ambiguous or 'both' type with explicit "Credit/Debit Card"
        if card_type and card_type.strip().lower() in { 'both', 'credit & debit', 'credit and debit', 'credit/debit', 'credit or debit' }:
            card_type = "Credit/Debit Card"
        card_provider = self.extract_card_provider(description)
        
        # Use Gemini API to fill missing bank or card_type information
        if not bank and description:
            print(f"   🤖 Bank is null, using Gemini API to analyze description...")
            gemini_bank = analyze_description_with_gemini(description, 'bank')
            if gemini_bank:
                bank = gemini_bank
                print(f"   ✅ Gemini API extracted bank: {bank}")
            else:
                print(f"   ❌ Gemini API could not extract bank information")
        
        if not card_type and description:
            print(f"   🤖 Card type is null, using Gemini API to analyze description...")
            gemini_card_type = analyze_description_with_gemini(description, 'card_type')
            if gemini_card_type:
                card_type = gemini_card_type
                print(f"   ✅ Gemini API extracted card type: {card_type}")
            else:
                print(f"   ❌ Gemini API could not extract card type information")
        
        # Determine if it's an instant discount
        is_instant = 'instant' in description.lower() or 'cashback' not in description.lower()
        
        # Enhanced logging
        logging.info(f"Parsed offer - Original Title: '{card_title}' -> Final Title: '{title}', Type: {offer_type}, Amount: ₹{amount}, Bank: {bank}, Min_spend: ₹{min_spend if min_spend else 'None'}, Card Type: {card_type}, Card Provider: {card_provider}")
        
        return Offer(
            title=title,
            description=description,
            amount=amount,
            type=offer_type,
            bank=bank,
            validity=validity,
            min_spend=min_spend,
            is_instant=is_instant,
            card_type=card_type,
            card_provider=card_provider
        )

    def generate_comprehensive_note(self, offer: Offer, product_price: float, is_applicable: bool, net_effective_price: float) -> str:
        """Generate comprehensive, human-like notes for offers."""
        if offer.type == "Bank Offer":
            if is_applicable:
                savings_amount = product_price - net_effective_price
                savings_percentage = (savings_amount / product_price) * 100 if product_price > 0 else 0
                
                note_parts = []
                
                # Main benefit description
                if savings_amount > 0:
                    note_parts.append(f"🎉 Excellent savings! You'll save ₹{savings_amount:,.0f} ({savings_percentage:.1f}%) with this offer.")
                else:
                    note_parts.append("💡 Great offer available for your purchase!")
                
                # Bank and card details
                bank_info = ""
                if offer.bank and offer.card_type:
                    # Remove "Card" suffix for natural language
                    card_type_display = offer.card_type.replace(" Card", "").lower()
                    bank_info = f"using your {offer.bank} {card_type_display} card"
                elif offer.bank:
                    bank_info = f"using your {offer.bank} card"
                elif offer.card_type:
                    # Remove "Card" suffix for natural language
                    card_type_display = offer.card_type.replace(" Card", "").lower()
                    bank_info = f"using your {card_type_display} card"
                
                if bank_info:
                    note_parts.append(f"Simply pay {bank_info} to get ₹{offer.amount:,.0f} instant discount.")
                else:
                    note_parts.append(f"You'll get ₹{offer.amount:,.0f} instant discount on your purchase.")
                
                # Minimum spend information
                if offer.min_spend:
                    note_parts.append(f"✅ This phone (₹{product_price:,.0f}) meets the minimum spend requirement of ₹{offer.min_spend:,.0f}.")
                else:
                    note_parts.append("✅ No minimum purchase requirement - the discount applies immediately!")
                
                # Final price information
                note_parts.append(f"Your final price will be ₹{net_effective_price:,.0f} instead of ₹{product_price:,.0f}.")
                
                # Additional details
                if offer.card_provider:
                    note_parts.append(f"Works with {offer.card_provider} cards.")
                
                if offer.validity:
                    note_parts.append(f"⏰ Offer valid {offer.validity}.")
                
                return " ".join(note_parts)
            
            else:
                # Not applicable - minimum spend not met
                shortfall = offer.min_spend - product_price if offer.min_spend else 0
                
                note_parts = []
                note_parts.append(f"⚠️ Unfortunately, this offer isn't applicable for this phone.")
                note_parts.append(f"The offer requires a minimum purchase of ₹{offer.min_spend:,.0f}, but this phone costs ₹{product_price:,.0f}.")
                note_parts.append(f"You would need to add ₹{shortfall:,.0f} more to your cart to use this offer.")
                
                if offer.bank and offer.card_type:
                    # Remove "Card" suffix for natural language
                    card_type_display = offer.card_type.replace(" Card", "").lower()
                    note_parts.append(f"However, if you reach the minimum spend using your {offer.bank} {card_type_display} card, you could save ₹{offer.amount:,.0f}!")
                elif offer.bank:
                    note_parts.append(f"But if you meet the minimum spend with your {offer.bank} card, you could save ₹{offer.amount:,.0f}!")
                else:
                    note_parts.append(f"If you meet the minimum spend, you could save ₹{offer.amount:,.0f}!")
                
                if offer.validity:
                    note_parts.append(f"⏰ Offer valid {offer.validity}.")
                
                return " ".join(note_parts)
        
        elif offer.type == "No Cost EMI":
            note_parts = []
            note_parts.append(f"💳 Convert your purchase into easy EMIs without any additional interest charges!")
            
            if offer.amount > 0:
                note_parts.append(f"You can save up to ₹{offer.amount:,.0f} on interest that you would normally pay.")
            
            if offer.min_spend and not is_applicable:
                note_parts.append(f"⚠️ This EMI option requires a minimum purchase of ₹{offer.min_spend:,.0f}, but this phone costs ₹{product_price:,.0f}.")
            elif offer.min_spend:
                note_parts.append(f"✅ This phone meets the minimum requirement of ₹{offer.min_spend:,.0f} for no-cost EMI.")
            else:
                note_parts.append("✅ Available for this purchase with no minimum spend requirement.")
            
            if offer.bank:
                note_parts.append(f"Available with {offer.bank} cards.")
            
            if offer.validity:
                note_parts.append(f"⏰ Offer valid {offer.validity}.")
            
            return " ".join(note_parts)
        
        elif offer.type == "Cashback":
            note_parts = []
            
            if is_applicable:
                note_parts.append(f"💰 Earn ₹{offer.amount:,.0f} cashback on your purchase!")
                note_parts.append("The cashback will be credited to your account after the purchase.")
                
                if offer.min_spend:
                    note_parts.append(f"✅ This phone (₹{product_price:,.0f}) meets the minimum spend requirement of ₹{offer.min_spend:,.0f}.")
                else:
                    note_parts.append("✅ No minimum purchase requirement.")
            else:
                note_parts.append(f"⚠️ This cashback offer requires a minimum purchase of ₹{offer.min_spend:,.0f}.")
                note_parts.append(f"This phone costs ₹{product_price:,.0f}, so you'll need to add ₹{offer.min_spend - product_price:,.0f} more to qualify.")
            
            if offer.bank:
                note_parts.append(f"Available with {offer.bank} cards.")
            
            if offer.validity:
                note_parts.append(f"⏰ Offer valid {offer.validity}.")
            
            return " ".join(note_parts)
        
        elif offer.type == "Partner Offers":
            note_parts = []
            note_parts.append(f"🤝 Special partner offer providing ₹{offer.amount:,.0f} value!")
            
            if is_applicable:
                note_parts.append("✅ This offer is applicable for your purchase.")
                if offer.min_spend:
                    note_parts.append(f"This phone meets the minimum requirement of ₹{offer.min_spend:,.0f}.")
            else:
                note_parts.append(f"⚠️ Requires minimum purchase of ₹{offer.min_spend:,.0f} to qualify.")
            
            if offer.validity:
                note_parts.append(f"⏰ Offer valid {offer.validity}.")
            
            return " ".join(note_parts)
        
        else:
            # Generic offer type
            note_parts = []
            if offer.amount > 0:
                note_parts.append(f"💫 This {offer.type.lower()} offers ₹{offer.amount:,.0f} value.")
            else:
                note_parts.append(f"💫 Special {offer.type.lower()} available for your purchase.")
            
            if not is_applicable and offer.min_spend:
                note_parts.append(f"⚠️ Requires minimum purchase of ₹{offer.min_spend:,.0f}.")
            elif is_applicable and offer.min_spend:
                note_parts.append(f"✅ This phone meets the minimum requirement of ₹{offer.min_spend:,.0f}.")
            
            if offer.validity:
                note_parts.append(f"⏰ Offer valid {offer.validity}.")
            
            return " ".join(note_parts)

    def calculate_offer_score(self, offer: Offer, product_price: float) -> float:
        """Calculate score specifically for Bank Offers only."""
        
        # Only calculate scores for Bank Offers
        if offer.type != "Bank Offer":
            return 0
        
        base_score = 80  # Base score for Bank Offers
        
        # PRIMARY FACTOR: Flat discount amount (heavily weighted)
        if product_price > 0 and offer.amount > 0:
            discount_percentage = (offer.amount / product_price) * 100
            # High weight for discount amount (up to 50 points)
            discount_points = min(discount_percentage * 2, 50)  
            base_score += discount_points
            logging.info(f"Bank Offer discount bonus: {discount_points:.1f} points for ₹{offer.amount} discount")

        # CRITICAL FACTOR: Minimum spend requirement
        if offer.min_spend and offer.min_spend > product_price:
            # Check if ALL bank offers have min spend > product price
            # For now, apply penalty but keep some score for ranking among non-applicable offers
            penalty_percentage = ((offer.min_spend - product_price) / product_price) * 100
            
            if penalty_percentage > 50:  # Very high min spend
                base_score = 15  # Low but rankable score
                logging.info(f"HIGH MIN SPEND: ₹{offer.min_spend} vs ₹{product_price} - Score set to 15")
            else:
                # Moderate penalty
                penalty = penalty_percentage * 0.5
                base_score -= penalty
                base_score = max(base_score, 20)  # Minimum rankable score
                logging.info(f"MODERATE MIN SPEND PENALTY: -{penalty:.1f} points")
        
        # BONUS: For applicable offers (min spend <= product price)
        elif offer.min_spend is None or offer.min_spend <= product_price:
            # Major bonus for applicable offers
            if offer.min_spend is None:
                base_score += 20  # Big bonus for no restrictions
                logging.info(f"NO MIN SPEND BONUS: +20 points")
            else:
                # Bonus for reasonable minimum spend
                spend_ratio = offer.min_spend / product_price if product_price > 0 else 0
                if spend_ratio <= 0.9:  # Min spend is 90% or less of product price
                    bonus = (1 - spend_ratio) * 10  # Up to 10 points bonus
                    base_score += bonus
                    logging.info(f"REASONABLE MIN SPEND BONUS: +{bonus:.1f} points")

        # INSTANT DISCOUNT BONUS
        if offer.is_instant:
            base_score += 5
            logging.info(f"INSTANT DISCOUNT BONUS: +5 points")

        # Neutralize reputation biases: no bank/card-type/card-provider adjustments
        # Keep score driven by discount amount, min spend applicability, and instant nature only

        final_score = max(0, min(100, base_score))
        logging.info(f"FINAL BANK OFFER SCORE: {final_score:.1f} for ₹{offer.amount} discount (Bank: {offer.bank}, Min spend: ₹{offer.min_spend if offer.min_spend else 'None'}, Card: {offer.card_type}, Provider: {offer.card_provider})")
        return final_score

    def rank_offers(self, offers_data: List[Dict], product_price: float) -> List[Dict[str, Any]]:
        """Rank offers based on comprehensive scoring - focusing only on Bank Offers."""
        logging.info(f"Ranking offers for product price: ₹{product_price}")
        
        # Parse all offers
        parsed_offers = [self.parse_offer(offer) for offer in offers_data if isinstance(offer, dict)]
        
        # Separate Bank Offers from other offers
        bank_offers = [offer for offer in parsed_offers if offer.type == "Bank Offer"]
        other_offers = [offer for offer in parsed_offers if offer.type != "Bank Offer"]
        
        logging.info(f"Found {len(bank_offers)} Bank Offers and {len(other_offers)} other offers")
        
        # Process all offers (both bank and others) to create the result list
        all_ranked_offers = []
        
        # Process Bank Offers with ranking
        if bank_offers:
            # Calculate scores for bank offers only
            scored_bank_offers = []
            for offer in bank_offers:
                score = self.calculate_offer_score(offer, product_price)
                
                # Calculate net effective price and applicability
                if offer.min_spend and product_price < offer.min_spend:
                    net_effective_price = product_price  # Offer not applicable - no discount
                    is_applicable = False
                    logging.info(f"Bank Offer NOT applicable - Min spend: ₹{offer.min_spend}, Product price: ₹{product_price}")
                else:
                    net_effective_price = max(product_price - offer.amount, 0)
                    is_applicable = True
                    logging.info(f"Bank Offer applicable - Net price: ₹{net_effective_price}")
                
                # Generate comprehensive human-like note
                note = self.generate_comprehensive_note(offer, product_price, is_applicable, net_effective_price)
                
                scored_bank_offers.append({
                    'title': offer.title,
                    'description': offer.description,
                    'amount': offer.amount,
                    'bank': offer.bank,
                    'validity': offer.validity,
                    'min_spend': offer.min_spend,
                    'score': score,
                    'is_instant': offer.is_instant,
                    'net_effective_price': net_effective_price,
                    'is_applicable': is_applicable,
                    'note': note,
                    'offer_type': 'Bank Offer',
                    'card_type': offer.card_type,
                    'card_provider': offer.card_provider
                })
            
            # Sort bank offers by score in descending order
            scored_bank_offers.sort(key=lambda x: x['score'], reverse=True)
            
            # Add rank numbers to bank offers only
            for idx, offer in enumerate(scored_bank_offers):
                offer['rank'] = idx + 1
            
            all_ranked_offers.extend(scored_bank_offers)
        
        # Process other offers (No Cost EMI, Cashback, Partner Offers, etc.) - no ranking
        for offer in other_offers:
            # Calculate basic info but no ranking
            if offer.min_spend and product_price < offer.min_spend:
                net_effective_price = product_price
                is_applicable = False
            else:
                net_effective_price = max(product_price - offer.amount, 0)
                is_applicable = True
            
            # Generate comprehensive human-like note
            note = self.generate_comprehensive_note(offer, product_price, is_applicable, net_effective_price)
            
            all_ranked_offers.append({
                'title': offer.title,
                'description': offer.description,
                'amount': offer.amount,
                'bank': offer.bank,
                'validity': offer.validity,
                'min_spend': offer.min_spend,
                'score': None,  # No score for non-bank offers
                'is_instant': offer.is_instant,
                'net_effective_price': net_effective_price,
                'is_applicable': is_applicable,
                'note': note,
                'offer_type': offer.type,
                'rank': None,  # No rank for non-bank offers
                'card_type': offer.card_type,
                'card_provider': offer.card_provider
            })
        
        logging.info(f"Ranked {len(bank_offers)} bank offers, included {len(other_offers)} other offers without ranking")
        return all_ranked_offers

# Helper to extract ASIN from URL
def extract_asin_from_url(url):
    match = re.search(r"/dp/([A-Z0-9]{10})", url)
    return match.group(1) if match else None

# Bank offer scraping logic (reusing from amazonBOmain.py)
def get_bank_offers(driver, url, max_retries=2):
    for attempt in range(max_retries):
        try:
            logging.info(f"Visiting URL (attempt {attempt + 1}/{max_retries}): {url}")
            driver.get(url)
            time.sleep(5)  # let page load
            
            all_offers = []
            
            # Follow the nested structure to find offer cards
            soup = BeautifulSoup(driver.page_source, "html.parser")
            body = soup.find("body", class_=lambda x: x and "a-aui_72554-c" in x)
            if not body:
                logging.warning("body with class 'a-aui_72554-c' not found")
                if attempt < max_retries - 1:
                    continue
                return all_offers
            
            a_page = body.find("div", id="a-page")
            if not a_page:
                logging.warning("div#a-page not found")
                if attempt < max_retries - 1:
                    continue
                return all_offers
            
            dp = a_page.find("div", id="dp", class_=lambda x: x and "wireless" in x and "en_IN" in x)
            if not dp:
                logging.warning("div#dp.wireless.en_IN not found")
                if attempt < max_retries - 1:
                    continue
                return all_offers
            
            dp_container = dp.find("div", id="dp-container", class_="a-container", role="main")
            if not dp_container:
                logging.warning("div#dp-container.a-container[role=main] not found")
                if attempt < max_retries - 1:
                    continue
                return all_offers
            
            ppd = dp_container.find("div", id="ppd")
            if not ppd:
                logging.warning("div#ppd not found")
                if attempt < max_retries - 1:
                    continue
                return all_offers
            
            center_col = ppd.find("div", id="centerCol", class_="centerColAlign")
            if not center_col:
                logging.warning("div#centerCol.centerColAlign not found")
                if attempt < max_retries - 1:
                    continue
                return all_offers
            
            vsxoffers_feature_div = center_col.find(
                "div", id="vsxoffers_feature_div", class_="celwidget", attrs={"data-feature-name": "vsxoffers"}
            )
            if not vsxoffers_feature_div:
                logging.warning("div#vsxoffers_feature_div.celwidget[data-feature-name=vsxoffers] not found")
                if attempt < max_retries - 1:
                    continue
                return all_offers
            
            # Find clickable offer cards using Selenium
            try:
                from selenium.webdriver.common.by import By
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
                
                # Wait for offers to be present
                wait = WebDriverWait(driver, 10)
                offer_cards = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".offers-items")))
                logging.info(f"Found {len(offer_cards)} clickable offer cards")
                
                for i, card in enumerate(offer_cards):
                    try:
                        logging.info(f"Processing clickable offer card {i+1}")
                        
                        # Get card title
                        card_title_element = card.find_element(By.CSS_SELECTOR, ".offers-items-title")
                        card_title = card_title_element.text.strip() if card_title_element else f"Card {i+1}"
                        logging.info(f"Card title: {card_title}")
                        
                        # Get card summary - try multiple selectors to capture full text
                        try:
                            # First try to get the full text from a-truncate-full (preferred)
                            card_summary_element = card.find_element(By.CSS_SELECTOR, ".a-truncate-full")
                            card_summary = card_summary_element.text.strip()
                            
                            # If truncate-full is empty or too short, try alternative methods
                            if not card_summary or len(card_summary) < 10:
                                # Try getting from truncate section
                                try:
                                    truncate_container = card.find_element(By.CSS_SELECTOR, ".a-truncate")
                                    card_summary = truncate_container.get_attribute("data-a-word-break") or truncate_container.text.strip()
                                except:
                                    pass
                                
                                # If still empty, try getting innerHTML and parsing
                                if not card_summary or len(card_summary) < 10:
                                    try:
                                        content_div = card.find_element(By.CSS_SELECTOR, ".offers-items-content")
                                        inner_html = content_div.get_attribute("innerHTML")
                                        if inner_html:
                                            inner_soup = BeautifulSoup(inner_html, 'html.parser')
                                            # Look for the full text in offscreen elements
                                            full_text_elem = inner_soup.find("span", class_="a-truncate-full a-offscreen")
                                            if full_text_elem:
                                                card_summary = full_text_elem.get_text(strip=True)
                                    except:
                                        pass
                        except:
                            try:
                                # Fallback to any text content in offers-items-content
                                content_element = card.find_element(By.CSS_SELECTOR, ".offers-items-content")
                                card_summary = content_element.text.strip()
                            except:
                                card_summary = "No summary available"
                        
                        logging.info(f"Card summary captured: {card_summary[:100]}{'...' if len(card_summary) > 100 else ''}")
                        
                        # Try to click on the card to load detailed offers
                        try:
                            # Scroll to the card to make sure it's visible
                            driver.execute_script("arguments[0].scrollIntoView(true);", card)
                            time.sleep(1)
                            
                            # Try to find and click the clickable element
                            clickable_element = card.find_element(By.CSS_SELECTOR, ".a-declarative")
                            driver.execute_script("arguments[0].click();", clickable_element)
                            logging.info(f"Clicked on {card_title} card")
                            
                            # Wait for detailed content to load
                            time.sleep(3)
                            
                            # Get updated page source and parse for detailed offers
                            updated_soup = BeautifulSoup(driver.page_source, "html.parser")
                            
                            # Look for detailed offers in the loaded content
                            card_id = card.get_attribute("id")
                            if card_id:
                                detailed_section_id = card_id.replace("itembox-", "")
                                detailed_section = updated_soup.find("div", id=detailed_section_id)
                                
                                if detailed_section:
                                    # Look for the detailed offers list
                                    offers_list = detailed_section.find("div", class_="a-section a-spacing-small a-spacing-top-small vsx-offers-desktop-lv__list")
                                    
                                    if offers_list:
                                        # Extract all individual offers
                                        individual_offers = offers_list.find_all("div", class_="a-section vsx-offers-desktop-lv__item")
                                        logging.info(f"Found {len(individual_offers)} detailed offers in {card_title}")
                                        
                                        for offer in individual_offers:
                                            offer_title = offer.find("h1", class_="a-size-base-plus a-spacing-mini a-spacing-top-small a-text-bold")
                                            offer_desc = offer.find("p", class_="a-spacing-mini a-size-base-plus")
                                            
                                            if offer_title and offer_desc:
                                                offer_data = {
                                                    "card_type": card_title,
                                                    "offer_title": offer_title.get_text(strip=True),
                                                    "offer_description": offer_desc.get_text(strip=True)
                                                }
                                                all_offers.append(offer_data)
                                                logging.info(f"Extracted detailed offer: {offer_data}")
                                    else:
                                        logging.info(f"No detailed offers list found for {card_title}, using summary")
                                        offer_data = {
                                            "card_type": card_title,
                                            "offer_title": "Summary",
                                            "offer_description": card_summary
                                        }
                                        all_offers.append(offer_data)
                                else:
                                    logging.info(f"No detailed section found for {card_title}, using summary")
                                    offer_data = {
                                        "card_type": card_title,
                                        "offer_title": "Summary", 
                                        "offer_description": card_summary
                                    }
                                    all_offers.append(offer_data)
                            
                            # Try to close any modal/popup that might have opened
                            try:
                                close_buttons = driver.find_elements(By.CSS_SELECTOR, "[data-action='a-popover-close'], .a-button-close, .a-offscreen")
                                for close_btn in close_buttons:
                                    if close_btn.is_displayed():
                                        driver.execute_script("arguments[0].click();", close_btn)
                                        break
                            except:
                                pass
                            
                            # Press Escape to close any modals
                            try:
                                from selenium.webdriver.common.keys import Keys
                                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
                            except:
                                pass
                                
                        except (ElementClickInterceptedException, TimeoutException) as e:
                            logging.warning(f"Could not click on {card_title} card: {e}")
                            # Fall back to summary
                            offer_data = {
                                "card_type": card_title,
                                "offer_title": "Summary",
                                "offer_description": card_summary
                            }
                            all_offers.append(offer_data)
                            
                    except Exception as e:
                        logging.error(f"Error processing clickable card {i+1}: {e}")
                        continue
                        
            except Exception as e:
                logging.error(f"Error with Selenium interaction: {e}")
                # Fall back to original BeautifulSoup parsing
                soup = BeautifulSoup(driver.page_source, "html.parser")
                offer_cards = soup.find_all("div", class_="offers-items")
                logging.info(f"Falling back to BeautifulSoup parsing, found {len(offer_cards)} cards")
                
                for i, card in enumerate(offer_cards):
                    try:
                        card_title = card.find("h6", class_="offers-items-title")
                        
                        # Try multiple approaches to get the full card summary text
                        card_summary_text = "No summary"
                        
                        # First try a-truncate-full (preferred - contains full untruncated text)
                        card_summary = card.find("span", class_="a-truncate-full")
                        if card_summary:
                            card_summary_text = card_summary.get_text(strip=True)
                        
                        # If truncate-full is empty or too short, try other selectors
                        if not card_summary_text or card_summary_text == "No summary" or len(card_summary_text) < 10:
                            # Try to find the offscreen full text element
                            offscreen_full = card.find("span", class_="a-truncate-full a-offscreen")
                            if offscreen_full:
                                card_summary_text = offscreen_full.get_text(strip=True)
                                logging.info(f"Found offscreen full text: {card_summary_text[:50]}...")
                            else:
                                # Try general content area
                                content_area = card.find("div", class_="offers-items-content")
                                if content_area:
                                    # Get all text from content area, excluding truncated versions
                                    all_text = content_area.get_text(strip=True)
                                    if all_text and len(all_text) > 20:  # Reasonable length check
                                        card_summary_text = all_text
                        
                        if card_title:
                            card_title_text = card_title.get_text(strip=True)
                            
                            offer_data = {
                                "card_type": card_title_text,
                                "offer_title": "Summary",
                                "offer_description": card_summary_text
                            }
                            all_offers.append(offer_data)
                            logging.info(f"Fallback extraction: {offer_data}")
                            
                    except Exception as e:
                        logging.error(f"Error in fallback parsing for card {i+1}: {e}")
                        continue
            
            logging.info(f"Total offers extracted: {len(all_offers)}")
            return all_offers if all_offers else []
            
        except Exception as e:
            logging.error(f"Exception in get_bank_offers (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in 3 seconds...")
                time.sleep(3)
                continue
            else:
                return [{"error": str(e)}]
    
    return []

def extract_price_amount(price_str):
    """Extract numeric amount from price string like '₹30,999'"""
    if not price_str:
        return 0.0
    
    # Remove currency symbols and extract numbers
    numbers = re.findall(r'[\d,]+\.?\d*', price_str)
    if numbers:
        return float(numbers[0].replace(',', ''))
    return 0.0

def extract_with_exchange_price_from_page(driver, product_price_amount: Optional[float] = None) -> Optional[str]:
    """Extract the 'With Exchange' price from the current product page if present.

    Strategy:
    - Use the explicit price element: span#priceAfterBuyBackDiscount
    - Do not compute or transform values; return the on-page text as-is
    Returns the exact on-page text (e.g., '₹21,378.00') or None if not found.
    """
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # 1) Direct 'You Pay' price inside priceAfterBuyBackDiscountSection
        direct_price_elem = soup.find('span', id='priceAfterBuyBackDiscount')
        if direct_price_elem:
            raw_text = direct_price_elem.get_text(strip=False)
            if raw_text:
                value = raw_text.strip()
                logging.info(f"With Exchange (You Pay) found: {value}")
                return value

        # 2) Sometimes the numeric value is present only in the section text
        section = soup.find('div', id='priceAfterBuyBackDiscountSection')
        if section:
            section_text = section.get_text(" ", strip=True)
            if section_text and re.search(r"\d", section_text):
                logging.info(f"With Exchange section text (You Pay) found: {section_text}")
                return section_text

        # 3) Fallback: visible 'Up to X off' text without computing any derived price
        max_off_div = soup.find('div', id='maxBuyBackDiscountSection')
        if max_off_div:
            raw_text = max_off_div.get_text(" ", strip=True)
            if raw_text:
                logging.info(f"With Exchange section text (Up to off) found: {raw_text}")
                return raw_text

        # 4) Last resort: scan the accordion row for any 'You Pay' line
        accordion = soup.find('div', id='buyBackAccordionRow')
        if accordion:
            accordion_text = accordion.get_text(" ", strip=True)
            if 'you pay' in accordion_text.lower() and re.search(r"\d", accordion_text):
                logging.info(f"With Exchange (accordion fallback) found: {accordion_text}")
                return accordion_text

        logging.info("With Exchange price not found on page")
        return None
    except Exception as e:
        logging.debug(f"Error extracting with-exchange price: {e}")
        return None

class ComprehensiveAmazonExtractor:
    """
    Enhanced Amazon link extractor that finds ALL Amazon links in deep nested JSON structures.
    """
    
    def __init__(self):
        self.amazon_links = []
        self.stats = {
            'total_entries': 0,
            'amazon_links_variants': 0,
            'amazon_links_all_matching_products': 0,
            'amazon_links_unmapped': 0,
            'total_amazon_links': 0,
            'entries_with_amazon': 0
        }
    
    def find_all_amazon_store_links(self, data: List[Dict]) -> List[Dict]:
        """
        Comprehensively find ALL Amazon store links in the JSON data.
        
        Returns:
            List of dictionaries containing Amazon link information and location details
        """
        print(f"🔍 Starting comprehensive Amazon link extraction from {len(data)} entries...")
        
        for entry_idx, entry in enumerate(data):
            self.stats['total_entries'] += 1
            
            if entry_idx % 100 == 0 and entry_idx > 0:
                print(f"   Processed {entry_idx} entries...")
            
            if not isinstance(entry, dict) or 'scraped_data' not in entry:
                continue
                
            scraped_data = entry['scraped_data']
            if not isinstance(scraped_data, dict):
                continue
            
            entry_has_amazon = False
            
            # 1. CHECK VARIANTS (original location)
            if 'variants' in scraped_data and isinstance(scraped_data['variants'], list):
                for variant_idx, variant in enumerate(scraped_data['variants']):
                    if isinstance(variant, dict) and 'store_links' in variant:
                        store_links = variant['store_links']
                        if isinstance(store_links, list):
                            for store_idx, store_link in enumerate(store_links):
                                if isinstance(store_link, dict):
                                    name = store_link.get('name', '').lower()
                                    if 'amazon' in name:
                                        self.amazon_links.append({
                                            'entry_idx': entry_idx,
                                            'location_type': 'variants',
                                            'location_idx': variant_idx,
                                            'store_idx': store_idx,
                                            'entry': entry,
                                            'location_data': variant,
                                            'store_link': store_link,
                                            'path': f'entry[{entry_idx}].scraped_data.variants[{variant_idx}].store_links[{store_idx}]'
                                        })
                                        self.stats['amazon_links_variants'] += 1
                                        entry_has_amazon = True
            
            # 2. CHECK ALL_MATCHING_PRODUCTS (missed in original)
            if 'all_matching_products' in scraped_data and isinstance(scraped_data['all_matching_products'], list):
                for product_idx, product in enumerate(scraped_data['all_matching_products']):
                    if isinstance(product, dict) and 'store_links' in product:
                        store_links = product['store_links']
                        if isinstance(store_links, list):
                            for store_idx, store_link in enumerate(store_links):
                                if isinstance(store_link, dict):
                                    name = store_link.get('name', '').lower()
                                    if 'amazon' in name:
                                        self.amazon_links.append({
                                            'entry_idx': entry_idx,
                                            'location_type': 'all_matching_products',
                                            'location_idx': product_idx,
                                            'store_idx': store_idx,
                                            'entry': entry,
                                            'location_data': product,
                                            'store_link': store_link,
                                            'path': f'entry[{entry_idx}].scraped_data.all_matching_products[{product_idx}].store_links[{store_idx}]'
                                        })
                                        self.stats['amazon_links_all_matching_products'] += 1
                                        entry_has_amazon = True
            
            # 3. CHECK UNMAPPED (missed in original)
            if 'unmapped' in scraped_data and isinstance(scraped_data['unmapped'], list):
                for unmapped_idx, unmapped_item in enumerate(scraped_data['unmapped']):
                    if isinstance(unmapped_item, dict) and 'store_links' in unmapped_item:
                        store_links = unmapped_item['store_links']
                        if isinstance(store_links, list):
                            for store_idx, store_link in enumerate(store_links):
                                if isinstance(store_link, dict):
                                    name = store_link.get('name', '').lower()
                                    if 'amazon' in name:
                                        self.amazon_links.append({
                                            'entry_idx': entry_idx,
                                            'location_type': 'unmapped',
                                            'location_idx': unmapped_idx,
                                            'store_idx': store_idx,
                                            'entry': entry,
                                            'location_data': unmapped_item,
                                            'store_link': store_link,
                                            'path': f'entry[{entry_idx}].scraped_data.unmapped[{unmapped_idx}].store_links[{store_idx}]'
                                        })
                                        self.stats['amazon_links_unmapped'] += 1
                                        entry_has_amazon = True
            
            if entry_has_amazon:
                self.stats['entries_with_amazon'] += 1
        
        self.stats['total_amazon_links'] = len(self.amazon_links)
        
        print(f"✅ Comprehensive extraction complete!")
        print(f"   📊 LOCATION BREAKDOWN:")
        print(f"      variants: {self.stats['amazon_links_variants']} links")
        print(f"      all_matching_products: {self.stats['amazon_links_all_matching_products']} links")
        print(f"      unmapped: {self.stats['amazon_links_unmapped']} links")
        print(f"   📈 TOTALS:")
        print(f"      Total Amazon links found: {self.stats['total_amazon_links']}")
        print(f"      Entries with Amazon links: {self.stats['entries_with_amazon']}")
        print(f"      Total entries processed: {self.stats['total_entries']}")
        
        return self.amazon_links



def create_chrome_driver(proxy=None, use_proxy=False, max_retries=3):
    """
    Create and configure a new Chrome driver session with optional proxy support.
    Includes robust error handling and retry mechanism.
    
    Args:
        proxy: Proxy configuration dictionary
        use_proxy: Whether to use proxy configuration
        max_retries: Maximum number of retry attempts for driver creation
    """
    for attempt in range(max_retries):
        try:
            options = uc.ChromeOptions()
            # Use CHROME_BIN if explicitly provided; otherwise rely on system default
            chrome_bin = os.getenv("CHROME_BIN")
            if chrome_bin:
                options.binary_location = chrome_bin
            
            print("🤖 Running in headless mode")
            options.add_argument('--headless=new')  # Use new headless mode
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-web-security')
            options.add_argument('--disable-features=VizDisplayCompositor')
            options.add_argument('--window-size=1920,1080')
            
            # Additional options for better compatibility and stability
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-plugins')
            options.add_argument('--disable-images')
            options.add_argument('--disable-javascript')
            options.add_argument('--disable-default-apps')
            options.add_argument('--disable-sync')
            options.add_argument('--disable-translate')
            options.add_argument('--disable-background-timer-throttling')
            options.add_argument('--disable-renderer-backgrounding')
            options.add_argument('--disable-backgrounding-occluded-windows')
            options.add_argument('--disable-ipc-flooding-protection')
            options.add_argument('--max_old_space_size=4096')
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # Add proxy configuration if provided
            if use_proxy and proxy:
                try:
                    # Extract proxy details from the proxy dictionary
                    if 'http' in proxy:
                        proxy_url = proxy['http']
                        # Parse proxy URL to extract host, port, username, password
                        if '@' in proxy_url:
                            auth_part, host_part = proxy_url.split('@')
                            username, password = auth_part.replace('http://', '').split(':')
                            host, port = host_part.split(':')
                        else:
                            host, port = proxy_url.replace('http://', '').split(':')
                            username = password = None
                        
                        # Set proxy arguments
                        if username and password:
                            options.add_argument(f'--proxy-server={host}:{port}')
                            # Note: Selenium doesn't support proxy authentication directly in options
                            # We'll need to handle this differently or use a different approach
                            logging.warning(f"⚠️ Proxy authentication not fully supported in current setup")
                        else:
                            options.add_argument(f'--proxy-server={host}:{port}')
                        
                        print(f"🔄 Using proxy: {host}:{port}")
                        logging.info(f"🔄 Chrome driver configured with proxy: {host}:{port}")
                        
                except Exception as e:
                    logging.error(f"❌ Error configuring proxy: {e}")
                    print(f"❌ Proxy configuration failed: {e}")
            
            # Create driver with timeout
            driver = uc.Chrome(options=options)
            
            # Test the driver by navigating to a simple page
            driver.set_page_load_timeout(30)
            driver.implicitly_wait(10)
            
            # Quick test to ensure driver is working
            driver.get("about:blank")
            
            print(f"✅ Chrome driver created successfully (attempt {attempt + 1})")
            logging.info(f"✅ Chrome driver created successfully (attempt {attempt + 1})")
            return driver
            
        except Exception as e:
            error_str = str(e).lower()
            logging.error(f"❌ Chrome driver creation failed (attempt {attempt + 1}/{max_retries}): {e}")
            print(f"❌ Chrome driver creation failed (attempt {attempt + 1}/{max_retries}): {e}")
            
            # Check for specific error types that might benefit from different handling
            if any(keyword in error_str for keyword in ['connection', 'timeout', 'refused', 'unreachable']):
                print(f"   🔄 Connection-related error detected, waiting before retry...")
                time.sleep(5)
            elif 'too many open files' in error_str or 'errno 24' in error_str:
                print(f"   🔄 File descriptor limit reached, cleaning up resources...")
                cleanup_resources()
                time.sleep(3)
            elif 'session' in error_str or 'patch' in error_str:
                print(f"   🔄 Session/patch error detected, waiting before retry...")
                time.sleep(10)
            else:
                print(f"   🔄 Generic error, waiting before retry...")
                time.sleep(3)
            
            if attempt == max_retries - 1:
                logging.error(f"❌ Failed to create Chrome driver after {max_retries} attempts")
                print(f"❌ Failed to create Chrome driver after {max_retries} attempts")
                raise e

def process_comprehensive_amazon_store_links(input_file, output_file, start_idx=0, max_entries=None):
    """
    Enhanced process that finds and processes ALL Amazon store links comprehensively.
    
    NEW FEATURES:
    1. Tracks visited URLs in visited_urls.txt file (creates if not exists)
    2. Extracts product price and availability status for each Amazon URL
    3. Updates the 'price' key at the same level as 'url' with:
       - Actual price if available (from span class="a-price-whole")
       - "Currently unavailable" if span class="a-size-medium a-color-success" contains unavailable message
    4. Maintains existing bank offers scraping functionality
    5. Skips URLs already in visited_urls.txt to preserve existing offers
    6. BROWSER SESSION MANAGEMENT: Creates fresh Chrome session for each link
    7. OFFER FILTERING: Automatically removes Cashback, No Cost EMI, and Partner Offers
       - Only Bank Offers and other allowed offer types are saved to the final output
    8. GEMINI AI INTEGRATION: Uses Google Gemini API to analyze descriptions when bank or card_type is null
    """
    
    # Load the JSON data
    # Resolve input path to mounted volume if necessary
    input_file = resolve_input_path(input_file)
    print(f"📖 Loading data from {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"✅ Loaded {len(data)} entries")
    
    # Setup visited URLs tracking with new functionality
    visited_urls_file = manage_visited_urls_file(VISITED_URLS_FILE)
    visited_urls = load_visited_urls(visited_urls_file)
    
    # Load URL cache to reuse results for duplicate links within the same file/run
    url_cache: Dict[str, Dict] = load_url_cache()
    
    # Use comprehensive extractor to find ALL Amazon links
    extractor = ComprehensiveAmazonExtractor()
    amazon_store_links = extractor.find_all_amazon_store_links(data)
    
    # Apply start index and max entries limit
    if start_idx > 0:
        amazon_store_links = amazon_store_links[start_idx:]
        print(f"⏩ Starting from index {start_idx}, processing {len(amazon_store_links)} links")
    
    if max_entries:
        amazon_store_links = amazon_store_links[:max_entries]
        print(f"🔢 Limited to processing {len(amazon_store_links)} links")
    
    # Setup resource management
    manage_file_descriptors()
    
    # Setup Chrome driver and analyzer
    driver = create_chrome_driver()
    analyzer = OfferAnalyzer()
    
    # Session management variables  
    SESSION_REFRESH_PER_LINK = True  # Recreate session after each processed link
    
    try:
        for idx, link_data in enumerate(amazon_store_links):
            try:
                entry = link_data['entry']
                store_link = link_data['store_link']
                location_type = link_data['location_type']
                
                print(f"\n🔍 Processing {idx + 1}/{len(amazon_store_links)}: {entry.get('display_name', entry.get('product_name', 'N/A'))}")
                print(f"   📍 Location: {location_type}[{link_data['location_idx']}].store_links[{link_data['store_idx']}]")
                print(f"   🛒 Path: {link_data['path'][:100]}...")
                print(f"   🔧 Session: Fresh session for each link")
                
                amazon_url = store_link.get('url', '')
                if not amazon_url:
                    print(f"   ⚠️  No URL found")
                    continue
                
                print(f"   🔗 Amazon URL: {amazon_url[:100]}...")
                
                # If URL already processed in this or previous run, reuse cached result instead of skipping
                if amazon_url in visited_urls or amazon_url in url_cache:
                    cached = url_cache.get(amazon_url)
                    if cached:
                        apply_cached_result_to_store_link(store_link, cached)
                        print(f"   ♻️  Applied cached data for already processed URL")
                    else:
                        print(f"   ⏭️  URL already scraped (no cache snapshot found), leaving existing fields as-is")
                    # Ensure visited tracking is updated and continue to next link
                    append_visited_url(amazon_url, visited_urls_file)
                    visited_urls.add(amazon_url)
                    # Small delay to be gentle on resources
                    time.sleep(1)
                    continue
                
                # Session management: recreate driver for each link (if not the first link)
                if idx > 0:
                    print(f"   🔄 Creating fresh Chrome session for this link...")
                    try:
                        driver.quit()
                        time.sleep(2)  # Brief pause before creating new session
                    except Exception as e:
                        logging.warning(f"Error closing previous session: {e}")
                    
                    # Check if we should use a proxy for this session
                    use_proxy = False
                    proxy = None
                    
                    # Use proxy every few links to distribute load
                    if PROXY_CONFIG['enabled'] and idx % PROXY_CONFIG['rotation_frequency'] == 0:
                        proxy = PROXY_MANAGER.get_next_proxy()
                        if proxy:
                            use_proxy = True
                            print(f"   🔄 Using proxy for session rotation (every {PROXY_CONFIG['rotation_frequency']}th link)")
                    
                    driver = create_chrome_driver(proxy=proxy, use_proxy=use_proxy)
                    print(f"   ✅ New Chrome session created successfully")
                
                # Extract price and availability information
                price_availability_info = extract_price_and_availability(driver, amazon_url)
                
                # Set in_stock status based on extracted information
                store_link['in_stock'] = price_availability_info.get('in_stock', True)
                
                # Add product name from URL scraping at the same level as url
                if price_availability_info.get('product_name_via_url'):
                    store_link['product_name_via_url'] = price_availability_info['product_name_via_url']
                    print(f"   📝 Product name: {price_availability_info['product_name_via_url'][:100]}...")
                
                # Only update price if in_stock is true, otherwise keep existing price
                if store_link['in_stock']:
                    # If price was extracted successfully, update it
                    if price_availability_info['price'] and price_availability_info['price'] not in ["Price not found", "Error extracting price", "Currently unavailable"]:
                        store_link['price'] = price_availability_info['price']
                    elif 'price' not in store_link or not store_link['price']:
                        store_link['price'] = "Price not available"
                # If in_stock is false, keep the existing price value unchanged
                
                print(f"   💰 Price: {store_link['price']}")
                print(f"   📦 Availability: {price_availability_info['availability']}")
                print(f"   📋 In Stock: {store_link['in_stock']}")

                # Check for inconsistent data: Price not available but in_stock is true
                if DATA_CONSISTENCY_CONFIG['enabled'] and DATA_CONSISTENCY_CONFIG['retry_on_inconsistent_data']:
                    availability_text = price_availability_info.get('availability', '').lower()
                    is_price_unavailable = any(keyword in availability_text for keyword in DATA_CONSISTENCY_CONFIG['inconsistent_data_keywords'])
                    
                    if is_price_unavailable and store_link['in_stock']:
                        print(f"   ⚠️  Inconsistent data detected: Price unavailable but in_stock=True")
                        print(f"   🔄 Retrying link to get accurate information...")
                        
                        # Take a screenshot before retry for debugging
                        take_screenshot(driver, amazon_url, "inconsistent_data_before_retry", "_before_retry")
                        
                        # Wait a bit before retry
                        time.sleep(3)
                        
                        try:
                            # Refresh the page and retry extraction
                            driver.refresh()
                            time.sleep(2)  # Wait for page to load
                            
                            # Retry price and availability extraction
                            retry_price_availability_info = extract_price_and_availability(driver, amazon_url)
                            
                            # Update with retry results
                            store_link['in_stock'] = retry_price_availability_info.get('in_stock', True)
                            
                            if retry_price_availability_info.get('product_name_via_url'):
                                store_link['product_name_via_url'] = retry_price_availability_info['product_name_via_url']
                                print(f"   📝 Product name (retry): {retry_price_availability_info['product_name_via_url'][:100]}...")
                            
                            # Update price with retry results
                            if store_link['in_stock']:
                                if retry_price_availability_info['price'] and retry_price_availability_info['price'] not in ["Price not found", "Error extracting price", "Currently unavailable"]:
                                    store_link['price'] = retry_price_availability_info['price']
                                elif 'price' not in store_link or not store_link['price']:
                                    store_link['price'] = "Price not available"
                            
                            print(f"   💰 Price (after retry): {store_link['price']}")
                            print(f"   📦 Availability (after retry): {retry_price_availability_info['availability']}")
                            print(f"   📋 In Stock (after retry): {store_link['in_stock']}")
                            
                            # Take screenshot after retry for comparison
                            take_screenshot(driver, amazon_url, "inconsistent_data_after_retry", "_after_retry")
                            
                            print(f"   ✅ Retry completed successfully")
                            
                        except Exception as retry_error:
                            logging.error(f"Retry failed for inconsistent data on {amazon_url}: {retry_error}")
                            print(f"   ❌ Retry failed: {retry_error}")
                            # Continue with original data if retry fails

                # Record the final visited platform URL (may differ due to redirects)
                try:
                    store_link['platform_url'] = driver.current_url
                except Exception:
                    store_link['platform_url'] = store_link.get('url', '')

                # Try to capture With Exchange price if present
                try:
                    base_price_amount = extract_price_amount(store_link.get('price', ''))
                    with_exchange_price = extract_with_exchange_price_from_page(driver, base_price_amount)
                    if with_exchange_price:
                        store_link['with_exchange_price'] = with_exchange_price
                        print(f"   🔄 With Exchange price: {with_exchange_price}")
                        logging.info(f"Set with_exchange_price for {amazon_url}: {with_exchange_price}")
                    else:
                        logging.info(f"No with_exchange_price found for {amazon_url}")
                except Exception as e:
                    logging.debug(f"Error setting with_exchange_price for {amazon_url}: {e}")
                
                # Get bank offers and other offers
                offers = get_bank_offers(driver, amazon_url)
                
                if offers:
                    # Get product price for ranking
                    price_str = store_link.get('price', '₹0')
                    product_price = extract_price_amount(price_str)
                    
                    # Rank the offers
                    ranked_offers = analyzer.rank_offers(offers, product_price)
                    
                    # Filter out unwanted offer types (Cashback, No Cost EMI, Partner Offers)
                    # Only Bank Offers and other allowed types will be saved
                    filtered_offers = []
                    removed_count = 0
                    
                    for offer in ranked_offers:
                        offer_type = offer.get('offer_type', '').lower()
                        if offer_type in ['cashback', 'no cost emi', 'partner offers']:
                            removed_count += 1
                            logging.info(f"Removing offer type '{offer_type}' for {amazon_url}")
                        else:
                            filtered_offers.append(offer)
                    
                    # Initialize Gemini API after filtering offers
                    if filtered_offers:
                        print(f"   🤖 Initializing Gemini API for AI analysis of {len(filtered_offers)} filtered offers...")
                        if initialize_gemini_api():
                            print(f"   ✅ Gemini API initialized successfully")
                        else:
                            print(f"   ⚠️  Gemini API initialization failed, continuing without AI analysis")
                    
                    # Update the store_link with filtered offers (only Bank Offers and other allowed types)
                    store_link['ranked_offers'] = filtered_offers
                    
                    if removed_count > 0:
                        print(f"   🗑️  Removed {removed_count} unwanted offers (Cashback/EMI/Partner)")
                    
                    print(f"   ✅ Found and ranked {len(offers)} offers, kept {len(filtered_offers)} after filtering")
                    
                    # Log the ranking summary for remaining offers
                    for i, offer in enumerate(filtered_offers[:3], 1):
                        score_display = offer['score'] if offer['score'] is not None else 'N/A'
                        print(f"   🏆 Rank {i}: {offer['title']} (Score: {score_display}, Amount: ₹{offer['amount']})")
                else:
                    print(f"   ❌ No offers found")
                    store_link['ranked_offers'] = []
                    
                    # Take screenshot when no offers are found
                    take_screenshot(driver, amazon_url, "offers_not_found", "_no_offers")
                
                # Add URL to visited list after successful processing
                append_visited_url(amazon_url, visited_urls_file)
                visited_urls.add(amazon_url)
                # Update cache with the processed result
                url_cache[amazon_url] = extract_store_link_snapshot(store_link)
                
                # Session refreshed after each link - no counter needed
                
                # Save progress every 100 entries (optimized backup frequency)
                if (idx + 1) % 100 == 0:
                    # Create new backup file
                    backup_file = f"{output_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    ensure_parent_dir(backup_file)
                    with open(backup_file, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    print(f"   💾 Progress saved to {backup_file} (every 100 URLs)")
                    
                    # Delete previous backup file to save storage
                    try:
                        # Find all backup files for this output file
                        backup_dir = os.path.dirname(output_file)
                        backup_base = os.path.basename(output_file)
                        backup_pattern = f"{backup_base}.backup_*.json"
                        backup_files = glob.glob(os.path.join(backup_dir, backup_pattern))
                        
                        # Sort by creation time and keep only the current one
                        if len(backup_files) > 1:
                            backup_files.sort(key=lambda x: os.path.getctime(x), reverse=True)
                            # Delete all but the most recent backup file
                            for old_backup in backup_files[1:]:
                                os.remove(old_backup)
                                print(f"   🗑️  Deleted old backup: {os.path.basename(old_backup)}")
                    except Exception as e:
                        logging.warning(f"Could not delete old backup files: {e}")
                        print(f"   ⚠️  Could not delete old backup files: {e}")
                    
                    # Persist cache and clean up resources every 100 URLs to prevent memory leaks
                    save_url_cache(url_cache)
                    cleanup_resources()
                
                # Small delay between requests
                time.sleep(2)
                
            except Exception as e:
                logging.error(f"Error processing link {idx + 1}: {e}")
                print(f"   ❌ Error processing link: {e}")
                
                # Enhanced error detection using the new error detection system
                error_info = detect_error_type(str(e))
                
                # Track persistent errors and pause if threshold exceeded
                if not hasattr(process_comprehensive_amazon_store_links, '_persistent_error_count'):
                    process_comprehensive_amazon_store_links._persistent_error_count = 0  # type: ignore
                
                # Increment error count for critical errors
                if (error_info['is_chrome_driver_error'] or error_info['is_http_connection_error'] or 
                    error_info['is_file_descriptor_error'] or error_info['is_session_error']):
                    process_comprehensive_amazon_store_links._persistent_error_count += 1  # type: ignore
                else:
                    process_comprehensive_amazon_store_links._persistent_error_count = 0  # type: ignore
                
                # Check if we need to pause due to persistent errors
                if getattr(process_comprehensive_amazon_store_links, '_persistent_error_count', 0) >= 5:  # type: ignore
                    print(f"   ⚠️  Too many persistent errors, saving progress and pausing...")
                    save_progress_and_pause(output_file, data, url_cache)
                    return
                
                # Handle different types of errors with appropriate recovery strategies
                if error_info['requires_driver_restart']:
                    print(f"   🔄 Chrome driver error detected, restarting driver...")
                    try:
                        driver = restart_chrome_driver(driver)
                        print(f"   ✅ Chrome driver restarted successfully")
                        continue  # Retry the same link with new driver
                    except Exception as restart_error:
                        logging.error(f"Failed to restart Chrome driver: {restart_error}")
                        print(f"   ❌ Failed to restart Chrome driver: {restart_error}")
                
                elif error_info['requires_resource_cleanup']:
                    print(f"   🧹 File descriptor error detected, cleaning up resources...")
                    cleanup_resources()
                    time.sleep(5)  # Wait for resources to be freed
                
                elif error_info['is_http_connection_error'] and PROXY_CONFIG['enabled']:
                    print(f"   🔄 HTTP connection error detected, attempting proxy rotation...")
                    try:
                        # Close current driver to free up resources
                        try:
                            driver.quit()
                        except:
                            pass
                        
                        # Get next available proxy
                        proxy = PROXY_MANAGER.get_next_proxy()
                        if proxy:
                            print(f"   🔄 Switching to proxy for retry...")
                            driver = create_chrome_driver(proxy=proxy, use_proxy=True)
                            
                            # Wait a bit before retrying
                            time.sleep(5)
                            
                            # Try to process the same link again
                            try:
                                print(f"   🔄 Retrying with proxy...")
                                
                                # Extract price and availability information with proxy
                                price_availability_info = extract_price_and_availability(driver, amazon_url)
                                
                                # Set in_stock status based on extracted information
                                store_link['in_stock'] = price_availability_info.get('in_stock', True)
                                
                                # Add product name from URL scraping at the same level as url
                                if price_availability_info.get('product_name_via_url'):
                                    store_link['product_name_via_url'] = price_availability_info['product_name_via_url']
                                    print(f"   📝 Product name: {price_availability_info['product_name_via_url'][:100]}...")
                                
                                # Only update price if in_stock is true, otherwise keep existing price
                                if store_link['in_stock']:
                                    if price_availability_info['price'] and price_availability_info['price'] not in ["Price not found", "Error extracting price", "Currently unavailable"]:
                                        store_link['price'] = price_availability_info['price']
                                    elif 'price' not in store_link or not store_link['price']:
                                        store_link['price'] = "Price not available"
                                
                                print(f"   💰 Price: {store_link['price']}")
                                print(f"   📦 Availability: {price_availability_info['availability']}")
                                print(f"   📋 In Stock: {store_link['in_stock']}")

                                # Check for inconsistent data: Price not available but in_stock is true (proxy retry)
                                if DATA_CONSISTENCY_CONFIG['enabled'] and DATA_CONSISTENCY_CONFIG['retry_on_inconsistent_data']:
                                    availability_text = price_availability_info.get('availability', '').lower()
                                    is_price_unavailable = any(keyword in availability_text for keyword in DATA_CONSISTENCY_CONFIG['inconsistent_data_keywords'])
                                    
                                    if is_price_unavailable and store_link['in_stock']:
                                            print(f"   ⚠️  Inconsistent data detected in proxy retry: Price unavailable but in_stock=True")
                                            print(f"   🔄 Retrying link again with page refresh...")
                                            
                                            # Take a screenshot before retry for debugging
                                            take_screenshot(driver, amazon_url, "inconsistent_data_proxy_retry_before", "_proxy_before_retry")
                                            
                                            # Wait a bit before retry
                                            time.sleep(3)
                                            
                                            try:
                                                # Refresh the page and retry extraction
                                                driver.refresh()
                                                time.sleep(2)  # Wait for page to load
                                                
                                                # Retry price and availability extraction
                                                retry_price_availability_info = extract_price_and_availability(driver, amazon_url)
                                                
                                                # Update with retry results
                                                store_link['in_stock'] = retry_price_availability_info.get('in_stock', True)
                                                
                                                if retry_price_availability_info.get('product_name_via_url'):
                                                    store_link['product_name_via_url'] = retry_price_availability_info['product_name_via_url']
                                                    print(f"   📝 Product name (proxy retry): {retry_price_availability_info['product_name_via_url'][:100]}...")
                                                
                                                # Update price with retry results
                                                if store_link['in_stock']:
                                                    if retry_price_availability_info['price'] and retry_price_availability_info['price'] not in ["Price not found", "Error extracting price", "Currently unavailable"]:
                                                        store_link['price'] = retry_price_availability_info['price']
                                                    elif 'price' not in store_link or not store_link['price']:
                                                        store_link['price'] = "Price not available"
                                                
                                                print(f"   💰 Price (after proxy retry): {store_link['price']}")
                                                print(f"   📦 Availability (after proxy retry): {retry_price_availability_info['availability']}")
                                                print(f"   📋 In Stock (after proxy retry): {store_link['in_stock']}")
                                                
                                                # Take screenshot after retry for comparison
                                                take_screenshot(driver, amazon_url, "inconsistent_data_proxy_retry_after", "_proxy_after_retry")
                                                
                                                print(f"   ✅ Proxy retry completed successfully")
                                                
                                            except Exception as retry_error:
                                                logging.error(f"Proxy retry failed for inconsistent data on {amazon_url}: {retry_error}")
                                                print(f"   ❌ Proxy retry failed: {retry_error}")
                                                # Continue with original data if retry fails

                                # Record the final visited platform URL
                                try:
                                    store_link['platform_url'] = driver.current_url
                                except Exception:
                                    store_link['platform_url'] = store_link.get('url', '')
                                
                                # Try to capture With Exchange price if present
                                try:
                                    base_price_amount = extract_price_amount(store_link.get('price', ''))
                                    with_exchange_price = extract_with_exchange_price_from_page(driver, base_price_amount)
                                    if with_exchange_price:
                                        store_link['with_exchange_price'] = with_exchange_price
                                        print(f"   🔄 With Exchange price: {with_exchange_price}")
                                        logging.info(f"Set with_exchange_price for {amazon_url}: {with_exchange_price}")
                                    else:
                                        logging.info(f"No with_exchange_price found for {amazon_url}")
                                except Exception as e:
                                    logging.debug(f"Error setting with_exchange_price for {amazon_url}: {e}")
                                
                                # Get bank offers and other offers
                                offers = get_bank_offers(driver, amazon_url)
                                
                                if offers:
                                    # Get product price for ranking
                                    price_str = store_link.get('price', '₹0')
                                    product_price = extract_price_amount(price_str)
                                    
                                    # Rank the offers
                                    ranked_offers = analyzer.rank_offers(offers, product_price)
                                    
                                    # Filter out unwanted offer types
                                    filtered_offers = []
                                    removed_count = 0
                                    
                                    for offer in ranked_offers:
                                        offer_type = offer.get('offer_type', '').lower()
                                        if offer_type in ['cashback', 'no cost emi', 'partner offers']:
                                            removed_count += 1
                                            logging.info(f"Removing offer type '{offer_type}' for {amazon_url}")
                                        else:
                                            filtered_offers.append(offer)
                                    
                                    # Initialize Gemini API after filtering offers
                                    if filtered_offers:
                                        print(f"   🤖 Initializing Gemini API for AI analysis of {len(filtered_offers)} filtered offers...")
                                        if initialize_gemini_api():
                                            print(f"   ✅ Gemini API initialized successfully")
                                        else:
                                            print(f"   ⚠️  Gemini API initialization failed, continuing without AI analysis")
                                    
                                    store_link['ranked_offers'] = filtered_offers
                                    
                                    if removed_count > 0:
                                        print(f"   🗑️  Removed {removed_count} unwanted offers (Cashback/EMI/Partner)")
                                    
                                    print(f"   ✅ Found and ranked {len(offers)} offers, kept {len(filtered_offers)} after filtering")
                                    
                                    # Log the ranking summary for remaining offers
                                    for i, offer in enumerate(filtered_offers[:3], 1):
                                        score_display = offer['score'] if offer['score'] is not None else 'N/A'
                                        print(f"   🏆 Rank {i}: {offer['title']} (Score: {score_display}, Amount: ₹{offer['amount']})")
                                else:
                                    print(f"   ❌ No offers found")
                                    store_link['ranked_offers'] = []
                                    
                                    # Take screenshot when no offers are found
                                    take_screenshot(driver, amazon_url, "offers_not_found", "_no_offers")
                                
                                # Add URL to visited list after successful processing
                                append_visited_url(amazon_url, visited_urls_file)
                                visited_urls.add(amazon_url)
                                
                                print(f"   ✅ Successfully processed with proxy after connection error!")
                                continue  # Skip to next link since we successfully processed this one
                                
                            except Exception as retry_error:
                                logging.error(f"Proxy retry failed for link {idx + 1}: {retry_error}")
                                print(f"   ❌ Proxy retry failed: {retry_error}")
                                
                                # Mark proxy as failed
                                if proxy:
                                    PROXY_MANAGER.mark_proxy_failed(proxy)
                                
                                # Create a new driver without proxy for next iteration
                                try:
                                    driver.quit()
                                except:
                                    pass
                                driver = create_chrome_driver()
                                
                        else:
                            print(f"   ⚠️  No more proxies available, continuing without proxy...")
                            driver = create_chrome_driver()
                    except Exception as proxy_error:
                        logging.error(f"Error during proxy rotation: {proxy_error}")
                        print(f"   ❌ Proxy rotation failed: {proxy_error}")
                        driver = create_chrome_driver()
                
                # Thread management - refresh threads periodically
                if (idx + 1) % THREAD_MANAGEMENT_CONFIG['refresh_interval'] == 0:
                    print(f"   🔄 Refreshing thread resources...")
                    refresh_thread_resources()
                    
                    # Check thread usage
                    thread_info = check_thread_usage()
                    if thread_info['is_high_usage']:
                        print(f"   ⚠️  High thread usage detected: {thread_info['thread_count']}/{thread_info['max_threads']} ({thread_info['usage_ratio']:.1%})")
                        cleanup_resources()
                
                # Driver restart for stability
                if (idx + 1) % THREAD_MANAGEMENT_CONFIG['driver_restart_interval'] == 0:
                    print(f"   🔄 Periodic driver restart for stability...")
                    try:
                        driver = restart_chrome_driver(driver)
                        print(f"   ✅ Periodic driver restart completed")
                    except Exception as restart_error:
                        logging.error(f"Periodic driver restart failed: {restart_error}")
                        print(f"   ❌ Periodic driver restart failed: {restart_error}")
                
                # Resource cleanup
                if (idx + 1) % THREAD_MANAGEMENT_CONFIG['cleanup_interval'] == 0:
                    print(f"   🧹 Periodic resource cleanup...")
                    cleanup_resources()
                    save_url_cache(url_cache)
                        
                        # Get next available proxy
                        proxy = PROXY_MANAGER.get_next_proxy()
                        if proxy:
                            print(f"   🔄 Switching to proxy for retry...")
                            driver = create_chrome_driver(proxy=proxy, use_proxy=True)
                            
                            # Wait a bit before retrying
                            time.sleep(5)
                            
                            # Try to process the same link again
                            try:
                                print(f"   🔄 Retrying with proxy...")
                                
                                # Extract price and availability information with proxy
                                price_availability_info = extract_price_and_availability(driver, amazon_url)
                                
                                # Set in_stock status based on extracted information
                                store_link['in_stock'] = price_availability_info.get('in_stock', True)
                                
                                # Add product name from URL scraping at the same level as url
                                if price_availability_info.get('product_name_via_url'):
                                    store_link['product_name_via_url'] = price_availability_info['product_name_via_url']
                                    print(f"   📝 Product name: {price_availability_info['product_name_via_url'][:100]}...")
                                
                                # Only update price if in_stock is true, otherwise keep existing price
                                if store_link['in_stock']:
                                    if price_availability_info['price'] and price_availability_info['price'] not in ["Price not found", "Error extracting price", "Currently unavailable"]:
                                        store_link['price'] = price_availability_info['price']
                                    elif 'price' not in store_link or not store_link['price']:
                                        store_link['price'] = "Price not available"
                                
                                print(f"   💰 Price: {store_link['price']}")
                                print(f"   📦 Availability: {price_availability_info['availability']}")
                                print(f"   📋 In Stock: {store_link['in_stock']}")

                                # Check for inconsistent data: Price not available but in_stock is true (proxy retry)
                                if DATA_CONSISTENCY_CONFIG['enabled'] and DATA_CONSISTENCY_CONFIG['retry_on_inconsistent_data']:
                                    availability_text = price_availability_info.get('availability', '').lower()
                                    is_price_unavailable = any(keyword in availability_text for keyword in DATA_CONSISTENCY_CONFIG['inconsistent_data_keywords'])
                                    
                                    if is_price_unavailable and store_link['in_stock']:
                                            print(f"   ⚠️  Inconsistent data detected in proxy retry: Price unavailable but in_stock=True")
                                            print(f"   🔄 Retrying link again with page refresh...")
                                            
                                            # Take a screenshot before retry for debugging
                                            take_screenshot(driver, amazon_url, "inconsistent_data_proxy_retry_before", "_proxy_before_retry")
                                            
                                            # Wait a bit before retry
                                            time.sleep(3)
                                            
                                            try:
                                                # Refresh the page and retry extraction
                                                driver.refresh()
                                                time.sleep(2)  # Wait for page to load
                                                
                                                # Retry price and availability extraction
                                                retry_price_availability_info = extract_price_and_availability(driver, amazon_url)
                                                
                                                # Update with retry results
                                                store_link['in_stock'] = retry_price_availability_info.get('in_stock', True)
                                                
                                                if retry_price_availability_info.get('product_name_via_url'):
                                                    store_link['product_name_via_url'] = retry_price_availability_info['product_name_via_url']
                                                    print(f"   📝 Product name (proxy retry): {retry_price_availability_info['product_name_via_url'][:100]}...")
                                                
                                                # Update price with retry results
                                                if store_link['in_stock']:
                                                    if retry_price_availability_info['price'] and retry_price_availability_info['price'] not in ["Price not found", "Error extracting price", "Currently unavailable"]:
                                                        store_link['price'] = retry_price_availability_info['price']
                                                    elif 'price' not in store_link or not store_link['price']:
                                                        store_link['price'] = "Price not available"
                                                
                                                print(f"   💰 Price (after proxy retry): {store_link['price']}")
                                                print(f"   📦 Availability (after proxy retry): {retry_price_availability_info['availability']}")
                                                print(f"   📋 In Stock (after proxy retry): {store_link['in_stock']}")
                                                
                                                # Take screenshot after retry for comparison
                                                take_screenshot(driver, amazon_url, "inconsistent_data_proxy_retry_after", "_proxy_after_retry")
                                                
                                                print(f"   ✅ Proxy retry completed successfully")
                                                
                                            except Exception as retry_error:
                                                logging.error(f"Proxy retry failed for inconsistent data on {amazon_url}: {retry_error}")
                                                print(f"   ❌ Proxy retry failed: {retry_error}")
                                                # Continue with original data if retry fails

                                # Record the final visited platform URL
                                try:
                                    store_link['platform_url'] = driver.current_url
                                except Exception:
                                    store_link['platform_url'] = store_link.get('url', '')
                                
                                # Try to capture With Exchange price if present
                                try:
                                    base_price_amount = extract_price_amount(store_link.get('price', ''))
                                    with_exchange_price = extract_with_exchange_price_from_page(driver, base_price_amount)
                                    if with_exchange_price:
                                        store_link['with_exchange_price'] = with_exchange_price
                                        print(f"   🔄 With Exchange price: {with_exchange_price}")
                                        logging.info(f"Set with_exchange_price for {amazon_url}: {with_exchange_price}")
                                    else:
                                        logging.info(f"No with_exchange_price found for {amazon_url}")
                                except Exception as e:
                                    logging.debug(f"Error setting with_exchange_price for {amazon_url}: {e}")
                                
                                # Get bank offers and other offers
                                offers = get_bank_offers(driver, amazon_url)
                                
                                if offers:
                                    # Get product price for ranking
                                    price_str = store_link.get('price', '₹0')
                                    product_price = extract_price_amount(price_str)
                                    
                                    # Rank the offers
                                    ranked_offers = analyzer.rank_offers(offers, product_price)
                                    
                                    # Filter out unwanted offer types
                                    filtered_offers = []
                                    removed_count = 0
                                    
                                    for offer in ranked_offers:
                                        offer_type = offer.get('offer_type', '').lower()
                                        if offer_type in ['cashback', 'no cost emi', 'partner offers']:
                                            removed_count += 1
                                            logging.info(f"Removing offer type '{offer_type}' for {amazon_url}")
                                        else:
                                            filtered_offers.append(offer)
                                    
                                    # Initialize Gemini API after filtering offers
                                    if filtered_offers:
                                        print(f"   🤖 Initializing Gemini API for AI analysis of {len(filtered_offers)} filtered offers...")
                                        if initialize_gemini_api():
                                            print(f"   ✅ Gemini API initialized successfully")
                                        else:
                                            print(f"   ⚠️  Gemini API initialization failed, continuing without AI analysis")
                                    
                                    store_link['ranked_offers'] = filtered_offers
                                    
                                    if removed_count > 0:
                                        print(f"   🗑️  Removed {removed_count} unwanted offers (Cashback/EMI/Partner)")
                                    
                                    print(f"   ✅ Found and ranked {len(offers)} offers, kept {len(filtered_offers)} after filtering")
                                    
                                    # Log the ranking summary for remaining offers
                                    for i, offer in enumerate(filtered_offers[:3], 1):
                                        score_display = offer['score'] if offer['score'] is not None else 'N/A'
                                        print(f"   🏆 Rank {i}: {offer['title']} (Score: {score_display}, Amount: ₹{offer['amount']})")
                                else:
                                    print(f"   ❌ No offers found")
                                    store_link['ranked_offers'] = []
                                    take_screenshot(driver, amazon_url, "offers_not_found", "_no_offers")
                                
                                # Add URL to visited list after successful processing
                                append_visited_url(amazon_url, visited_urls_file)
                                visited_urls.add(amazon_url)
                                
                                print(f"   ✅ Successfully processed with proxy after connection error!")
                                continue  # Skip to next link since we successfully processed this one
                                
                            except Exception as retry_error:
                                logging.error(f"Proxy retry failed for link {idx + 1}: {retry_error}")
                                print(f"   ❌ Proxy retry failed: {retry_error}")
                                
                                # Mark proxy as failed
                                if proxy:
                                    PROXY_MANAGER.mark_proxy_failed(proxy)
                                
                                # Create a new driver without proxy for next iteration
                                try:
                                    driver.quit()
                                except:
                                    pass
                                driver = create_chrome_driver()
                                
                        else:
                            print(f"   ⚠️  No more proxies available, continuing without proxy...")
                            driver = create_chrome_driver()
                    except Exception as proxy_error:
                        logging.error(f"Error during proxy rotation: {proxy_error}")
                        print(f"   ❌ Proxy rotation failed: {proxy_error}")
                        driver = create_chrome_driver()
                
                # Take screenshot when there's an error processing the link
                try:
                    take_screenshot(driver, amazon_url, "error_processing_link", f"_link_{idx + 1}")
                except:
                    pass  # Don't let screenshot errors stop the process
                
                continue
    
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted! Saving progress...")
    
    finally:
        driver.quit()
        
        # Save final output
        # Ensure output directory exists
        output_file = output_file if os.path.isabs(output_file) else os.path.join(DATA_DIR, output_file)
        ensure_parent_dir(output_file)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        # Save cache at the end
        save_url_cache(url_cache)
        
        print(f"\n✅ Final output saved to {output_file}")
        
        # Enhanced Summary
        total_processed = sum(1 for link_data in amazon_store_links 
                            if link_data['store_link'].get('ranked_offers'))
        total_offers_after_filtering = sum(len(link_data['store_link'].get('ranked_offers', [])) 
                                         for link_data in amazon_store_links)
        
        print(f"\n📊 COMPREHENSIVE SUMMARY:")
        print(f"   🎯 EXTRACTION STATS:")
        print(f"      Found Amazon links in variants: {extractor.stats['amazon_links_variants']}")
        print(f"      Found Amazon links in all_matching_products: {extractor.stats['amazon_links_all_matching_products']}")
        print(f"      Found Amazon links in unmapped: {extractor.stats['amazon_links_unmapped']}")
        print(f"      Total Amazon links found: {extractor.stats['total_amazon_links']}")
        print(f"   🔄 PROCESSING STATS:")
        print(f"      Processed Amazon links: {total_processed}")
        print(f"      Total offers after filtering: {total_offers_after_filtering}")
        print(f"      Entries with Amazon links: {extractor.stats['entries_with_amazon']}")
        print(f"   🗑️  FILTERING INFO:")
        print(f"      Only Bank Offers and other allowed types kept (Cashback/EMI/Partner offers removed)")

# ===============================================
# API SETUP AND ENDPOINTS
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

def run_scraper_process(input_file="all_data.json", output_file=None, start_idx=0, max_entries=None):
    """
    Function to run the scraper process in a separate thread
    """
    global scraping_status
    
    try:
        # Generate timestamped output filename if not provided
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(DATA_DIR, f"all_data_amazon_{timestamp}.json")

        # Resolve input path toward mounted volume if relative
        input_file = resolve_input_path(input_file)
        
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
        
        logging.info(f"API triggered scraper process started with output file: {output_file}")
        
        # Run the main scraping function
        process_comprehensive_amazon_store_links(input_file, output_file, start_idx, max_entries)
        
        # Mark as completed
        scraping_status.update({
            'is_running': False,
            'completed': True,
            'end_time': datetime.now().isoformat()
        })
        
        logging.info("API triggered scraper process completed successfully")
        
    except Exception as e:
        # Mark as error
        scraping_status.update({
            'is_running': False,
            'completed': False,
            'error': str(e),
            'end_time': datetime.now().isoformat()
        })
        
        logging.error(f"API triggered scraper process failed: {e}")

@app.route('/start-scraping', methods=['POST'])
def start_scraping():
    """
    API endpoint to start the scraping process
    """
    global scraping_status
    
    # Check if scraping is already running
    if scraping_status['is_running']:
        return jsonify({
            'status': 'error',
            'message': 'Scraping is already in progress',
            'data': scraping_status
        }), 400
    
    try:
        # Get parameters from request (if any)
        data = request.get_json() if request.is_json else {}
        
        input_file = data.get('input_file', os.path.join(DATA_DIR, 'all_data.json'))
        # Generate timestamped output filename if not provided
        output_file = data.get('output_file', None)
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(DATA_DIR, f"all_data_amazon_{timestamp}.json")
        start_idx = data.get('start_idx', 0)
        max_entries = data.get('max_entries', None)
        
        # Start scraping in a separate thread
        scraper_thread = threading.Thread(
            target=run_scraper_process,
            args=(input_file, output_file, start_idx, max_entries),
            daemon=True
        )
        scraper_thread.start()
        
        return jsonify({
            'status': 'success',
            'message': 'Scraping process started successfully',
            'data': {
                'input_file': input_file,
                'output_file': output_file,
                'start_idx': start_idx,
                'max_entries': max_entries,
                'started_at': scraping_status['start_time']
            }
        }), 200
        
    except Exception as e:
        logging.error(f"Error starting scraper via API: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to start scraping: {str(e)}',
            'data': None
        }), 500

@app.route('/scraping-status', methods=['GET'])
def get_scraping_status():
    """
    API endpoint to get the current scraping status
    """
    return jsonify({
        'status': 'success',
        'message': 'Status retrieved successfully',
        'data': scraping_status
    }), 200

@app.route('/stop-scraping', methods=['POST'])
def stop_scraping():
    """
    API endpoint to stop the scraping process (graceful stop)
    """
    global scraping_status
    
    if not scraping_status['is_running']:
        return jsonify({
            'status': 'error',
            'message': 'No scraping process is currently running',
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
        'message': 'Scraping process stop requested',
        'data': scraping_status
    }), 200

@app.route('/health', methods=['GET'])
def health_check():
    """
    API endpoint for health check
    """
    return jsonify({
        'status': 'success',
        'message': 'Enhanced Amazon Scraper API is healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    }), 200

@app.route('/', methods=['GET'])
def api_info():
    """
    API endpoint for basic information
    """
    return jsonify({
        'name': 'Enhanced Amazon Scraper API',
        'version': '1.0.0',
        'description': 'API to trigger comprehensive Amazon scraping with price & availability tracking',
        'endpoints': {
            'POST /start-scraping': 'Start the scraping process',
            'GET /scraping-status': 'Get current scraping status',
            'POST /stop-scraping': 'Stop the scraping process',
            'GET /health': 'Health check',
            'GET /': 'API information'
        },
        'features': [
            'Product prices and availability status',
            'Ranked bank offers',
            'URL visit tracking',
            'Smart session management',
            'Progress tracking via API'
        ]
    }), 200

def check_and_handle_captcha(driver, url):
    """
    Check if the current page is a CAPTCHA page and handle it by clicking the "Continue shopping" button.
    
    Args:
        driver: Selenium WebDriver instance
        url: The URL being processed
        
    Returns:
        bool: True if CAPTCHA was detected and handled, False otherwise
    """
    # Check if CAPTCHA handling is enabled
    if not CAPTCHA_CONFIG['enabled']:
        return False
        
    try:
        # Check if this is a CAPTCHA page by looking for the "Continue shopping" button
        captcha_button = driver.find_element(By.XPATH, "//button[@type='submit' and @class='a-button-text' and @alt='Continue shopping']")
        
        if captcha_button and CAPTCHA_CONFIG['auto_click_continue_shopping']:
            logging.info(f"CAPTCHA page detected for {url}, attempting to handle...")
            print(f"   🚨 CAPTCHA page detected, clicking 'Continue shopping' button...")
            

            
            # Click the "Continue shopping" button
            captcha_button.click()
            
            # Wait for the page to load after clicking
            time.sleep(CAPTCHA_CONFIG['wait_after_captcha_click'])
            
            # Check if we're still on a CAPTCHA page or if we've been redirected
            try:
                # Try to find the button again to see if we're still on CAPTCHA page
                driver.find_element(By.XPATH, "//button[@type='submit' and @class='a-button-text' and @alt='Continue shopping']")
                logging.warning(f"Still on CAPTCHA page after clicking button for {url}")
                print(f"   ⚠️  Still on CAPTCHA page, may need manual intervention")
                return False
            except:
                # Button not found, we've likely been redirected to the actual product page
                logging.info(f"Successfully handled CAPTCHA for {url}, redirected to product page")
                print(f"   ✅ CAPTCHA handled successfully, proceeding with scraping...")
                

                
                # Wait a bit more for the page to fully load
                time.sleep(CAPTCHA_CONFIG['additional_wait_after_captcha'])
                
                return True
                
    except Exception as e:
        # No CAPTCHA button found, this is a normal page
        return False
    


if __name__ == "__main__":
    import sys
    
    # Check if script should run as API or direct execution
    if len(sys.argv) > 1 and sys.argv[1] == "--api":
        # Run as Flask API
        print("🚀 ENHANCED AMAZON SCRAPER API MODE")
        print("Starting Flask API server...")
        print("Available endpoints:")
        print("  POST /start-scraping  - Start the scraping process")
        print("  GET  /scraping-status - Get current scraping status")
        print("  POST /stop-scraping   - Stop the scraping process")
        print("  GET  /health         - Health check")
        print("  GET  /              - API information")
        print("-" * 60)
        
        # Get port from command line arguments or use default
        port = 5000
        if len(sys.argv) > 2:
            try:
                port = int(sys.argv[2])
            except ValueError:
                print("Invalid port number, using default 5000")
                port = 5000
        
        print(f"🌐 Starting API server on http://localhost:{port}")
        print(f"📖 Example usage:")
        print(f"   curl -X POST http://localhost:{port}/start-scraping")
        print(f"   curl -X GET http://localhost:{port}/scraping-status")
        print("-" * 60)
        
        # Run Flask app
        app.run(host='0.0.0.0', port=port, debug=False)
        
    else:
        # Run as direct script execution (original behavior)
        input_file = os.path.join(DATA_DIR, "all_data.json")
        # Generate timestamped output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(DATA_DIR, f"all_data_amazon_{timestamp}.json")
        
        print("🚀 ENHANCED COMPREHENSIVE AMAZON SCRAPER WITH PRICE & AVAILABILITY TRACKING")
        print("This script finds ALL Amazon store links in deep nested JSON and adds:")
        print("  🎯 Product prices and availability status")
        print("  🏆 Ranked bank offers") 
        print("  📝 URL visit tracking (visited_urls.txt) - preserves existing offers")
        print("  🔄 Smart session management (fresh session for each link)")
        print("  🤖 Gemini AI integration for missing bank/card_type data")
        print("🎯 FEATURES: Price extraction + Availability checking + Bank offers + URL tracking + Per-link session refresh + AI analysis")
        print("🤖 DEFAULT MODE: Headless browser, processes all URLs, backups every 100 URLs")
        print()
        print("💡 TIP: Run with --api flag to start as API server instead:")
        print(f"   python {sys.argv[0]} --api [port]")
        print("-" * 80)
        
        # Default configuration - no user interaction required
        start_idx = 0  # Always start from beginning
        max_entries = None  # Process all entries
        
        print(f"⚙️  CONFIGURATION:")
        print(f"   📍 Start index: {start_idx} (beginning)")
        print(f"   🔢 Max entries: {'All' if max_entries is None else max_entries}")
        print(f"   📁 Input file: {input_file}")
        print(f"   📄 Output file: {output_file}")
        print(f"   🤖 Browser mode: Headless (server mode)")
        print(f"   🔄 Session management: Fresh session for each link")
        print(f"   💾 Backup frequency: Every 100 processed URLs")
        print(f"   🔄 URL handling: Skip already visited URLs to preserve offers")
        print(f"   🤖 AI integration: Gemini API for missing bank/card_type data")
        print()
        
        process_comprehensive_amazon_store_links(input_file, output_file, start_idx, max_entries) 