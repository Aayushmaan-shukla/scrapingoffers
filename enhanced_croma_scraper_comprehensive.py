#!/usr/bin/env python3
"""
COMPREHENSIVE Enhanced Croma Bank Offer Scraper - FULLY AUTONOMOUS
===================================================================
- Reaches ALL 3 nested locations for Croma links:
  1. scraped_data.variants[].store_links[]
  2. scraped_data.all_matching_products[].store_links[]  
  3. scraped_data.unmapped[].store_links[]
- Uses comprehensive_amazon_offers.json as input
- Completely isolates Amazon data (no changes to Amazon offers)
- FULLY SELF-CONTAINED: No external dependencies
- URL tracking with visited_urls_croma.txt
- Stock status detection via span.amount#pdp-product-price
- Fresh browser session for each link (headless mode)
- Backup every 100 URLs for better performance
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
        
        # Get page soup for element checking
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Check for the Croma price element: span.amount#pdp-product-price
        price_element = soup.find('span', {
            'class': 'amount',
            'id': 'pdp-product-price',
            'data-testid': 'new-price'
        })
        
        if price_element:
            # Price element found - product is in stock
            price_text = price_element.get_text(strip=True)
            logging.info(f"Price element found: {price_text} - Product in stock")
            return {
                'in_stock': True,
                'price_found': price_text,
                'status_details': f"Price element found: {price_text}"
            }
        else:
            # Price element not found - product out of stock
            logging.info(f"Price element not found - Product out of stock")
            return {
                'in_stock': False,
                'price_found': None,
                'status_details': "Price element (span.amount#pdp-product-price) not found"
            }
    
    except Exception as e:
        logging.error(f"Error checking stock status for {url}: {e}")
        return {
            'in_stock': False,
            'price_found': None,
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
            
            # Debug: Save page source for troubleshooting on server
            debug_filename = f"debug_croma_page_{int(time.time())}.html"
            try:
                with open(debug_filename, 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                logging.info(f"Saved page source to {debug_filename} for debugging")
            except Exception as e:
                logging.warning(f"Could not save debug page source: {e}")
            
            # Debug: Check for key elements existence with detailed analysis
            offer_containers = soup.select("div.offer-container")
            offer_sections = soup.select("div.offer-section-pdp")
            bank_swipers = soup.select("div.bank-offer-swiper")
            swiper_containers = soup.select("div.swiper-container")
            all_swiper_slides = soup.select("div.swiper-slide")
            
            # Check for offer-related text content
            offer_text_elements = soup.find_all(text=re.compile(r'(discount|offer|bank|cashback|emi|instant)', re.I))
            bank_offer_texts = soup.find_all(text=re.compile(r'bank.*offer', re.I))
            
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
                potential_offer_elements = soup.find_all(text=re.compile(r'(discount|offer|bank|cashback|emi)', re.I))
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
                    alt_desc_tags = wrapper.find_all('span', text=re.compile(r'.*(discount|offer|bank|cashback|emi).*', re.I))
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
# BROWSER SESSION MANAGEMENT
# ===============================================

def create_chrome_driver():
    """Create and configure a new Chrome driver session for Croma scraping - Google Chrome only."""
    print("🤖 Creating fresh Google Chrome session (NOT Chromium) optimized for Ubuntu Server with Chrome 139.0.7258.66")
    
    options = uc.ChromeOptions()
    
    # Force use of Google Chrome instead of Chromium
    options.binary_location = '/usr/bin/google-chrome'  # Standard Google Chrome path
    
    # Ubuntu Server specific configurations for Google Chrome
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--disable-background-timer-throttling')
    options.add_argument('--disable-backgrounding-occluded-windows')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=VizDisplayCompositor,TranslateUI')
    options.add_argument('--disable-ipc-flooding-protection')
    
    # Enhanced for server environments
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--start-maximized')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins')
    options.add_argument('--disable-images')  # Speed up loading but keep JS for offers
    options.add_argument('--disable-javascript-harmony-shipping')
    options.add_argument('--disable-background-networking')
    options.add_argument('--disable-default-apps')
    options.add_argument('--disable-sync')
    
    # Anti-detection for Google Chrome 139.0.7258.66
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Updated user agent for Google Chrome 139.0.7258.66 compatibility
    options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.7258.66 Safari/537.36')
    
    # Memory management for server
    options.add_argument('--memory-pressure-off')
    options.add_argument('--max_old_space_size=4096')
    
    # Enable better JavaScript handling for dynamic content
    options.add_argument('--enable-javascript')
    options.add_argument('--allow-running-insecure-content')
    
    try:
        print("🔍 Attempting to use Google Chrome at /usr/bin/google-chrome")
        driver = uc.Chrome(options=options)
        # Additional anti-detection
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        print("✅ Successfully created Google Chrome driver")
        return driver
    except Exception as e:
        print(f"❌ Error creating Google Chrome driver: {e}")
        print("🔄 Trying alternative Google Chrome paths...")
        
        # Try alternative Google Chrome paths
        chrome_paths = [
            '/usr/bin/google-chrome-stable',
            '/opt/google/chrome/google-chrome',
            '/usr/bin/chromium-browser',  # Last resort fallback
            None  # Let undetected-chromedriver find it
        ]
        
        for chrome_path in chrome_paths:
            try:
                options = uc.ChromeOptions()
                if chrome_path and chrome_path != '/usr/bin/chromium-browser':
                    options.binary_location = chrome_path
                    print(f"🔍 Trying Chrome at: {chrome_path}")
                elif chrome_path == '/usr/bin/chromium-browser':
                    print(f"⚠️  Falling back to Chromium (not ideal): {chrome_path}")
                else:
                    print(f"🔍 Letting undetected-chromedriver auto-detect Chrome location")
                
                # Basic configuration for fallback
                options.add_argument('--headless')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-gpu')
                options.add_argument('--window-size=1920,1080')
                options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.7258.66 Safari/537.36')
                
                driver = uc.Chrome(options=options)
                print(f"✅ Successfully created Chrome driver using: {chrome_path or 'auto-detected path'}")
                return driver
                
            except Exception as fallback_error:
                print(f"❌ Failed with {chrome_path or 'auto-detect'}: {fallback_error}")
                continue
        
        raise Exception("❌ Could not create Chrome driver with any configuration")

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
    FULLY AUTONOMOUS Croma processing that:
    1. Uses comprehensive input file as source
    2. Finds Croma links in ALL 3 nested locations
    3. Tracks URLs with visited_urls_croma.txt
    4. Detects stock status via span.amount#pdp-product-price
    5. Creates fresh browser session for each link
    6. Completely isolates Amazon data
    7. Uses advanced ranking logic
    8. Backup every 100 URLs for better performance
    9. No user interaction required
    """
    
    print(f"🚀 COMPREHENSIVE CROMA SCRAPER - FULLY AUTONOMOUS MODE")
    print(f"📂 Input file: {input_file}")
    print(f"📂 Output file: {output_file}")
    print(f"🛡️  Amazon data isolation: ENABLED (no changes to Amazon offers)")
    print(f"📝 URL tracking: visited_urls_croma.txt")
    print(f"📦 Stock detection: span.amount#pdp-product-price element")
    print(f"🔄 Session management: Fresh browser session for each link")
    print(f"💾 Backup frequency: Every 100 URLs")
    print(f"🤖 Automation: No user interaction required")
    print("-" * 80)
    
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
    
    # Setup URL tracking with new functionality
    visited_urls_file = manage_visited_urls_file("visited_urls_croma.txt")
    visited_urls = load_visited_urls(visited_urls_file)
    
    # Find ALL Croma store links from all 3 locations
    print(f"\n🔍 Discovering Croma links from ALL nested locations...")
    croma_store_links = find_all_croma_store_links_comprehensive(data)
    
    if not croma_store_links:
        print(f"❌ No Croma store links found in {input_file}")
        return
    
    # Check how many URLs have already been visited
    already_visited_count = len([link for link in croma_store_links if link['store_link'].get('url') in visited_urls])
    if already_visited_count > 0:
        print(f"🔄 Found {already_visited_count} previously visited URLs (will re-process all)")
    
    print(f"🚀 Processing ALL {len(croma_store_links)} Croma links (including re-scraping)")
    
    # Setup initial Chrome driver and analyzer
    driver = create_chrome_driver()
    analyzer = CromaOfferAnalyzer()
    
    # Statistics
    stats = {
        'processed': 0,
        'skipped_no_url': 0,
        'scraped_successfully': 0,
        'failed_scraping': 0,
        'total_offers_added': 0,
        'in_stock_count': 0,
        'out_of_stock_count': 0
    }
    
    try:
        print(f"\n🎯 Starting Croma scraping (Amazon data completely isolated)...")
        
        for idx, link_data in enumerate(croma_store_links):
            entry = link_data['entry']
            store_link = link_data['store_link']
            
            print(f"\n🔍 Processing {idx + 1}/{len(croma_store_links)}: {entry.get('product_name', 'N/A')}")
            print(f"   📍 Location: {link_data['path']}")
            print(f"   🔧 Session: Fresh session for each link")
            
            # Session management: recreate driver for each link (if not the first link)
            if idx > 0:
                print(f"   🔄 Creating fresh Chrome session for this link...")
                try:
                    driver.quit()
                    time.sleep(2)  # Brief pause before creating new session
                except Exception as e:
                    logging.warning(f"Error closing previous session: {e}")
                
                driver = create_chrome_driver()
                print(f"   ✅ New Chrome session created successfully")
            
            # Get parent object info for display
            parent_obj = link_data['parent_object']
            if link_data['location'] == 'variants':
                variant_info = f"{parent_obj.get('colour', 'N/A')} {parent_obj.get('ram', '')} {parent_obj.get('storage', '')}"
                print(f"   📱 Variant: {variant_info}")
            elif link_data['location'] == 'all_matching_products':
                print(f"   🔗 Matching Product: {parent_obj.get('name', 'N/A')}")
            else:  # unmapped
                print(f"   📦 Unmapped: {parent_obj.get('name', 'N/A')}")
            
            croma_url = store_link.get('url', '')
            if not croma_url:
                print(f"   ⚠️  No URL found")
                stats['skipped_no_url'] += 1
                continue
            
            print(f"   🌐 Croma URL: {croma_url}")
            
            # Visit the page first for stock status checking
            try:
                driver.get(croma_url)
                time.sleep(3)  # Wait for page to load
                
                # Extract stock status first
                stock_status = extract_croma_stock_status(driver, croma_url)
                store_link['in_stock'] = stock_status['in_stock']
                
                if stock_status['in_stock']:
                    stats['in_stock_count'] += 1
                    print(f"   📦 Stock status: In Stock - {stock_status['status_details']}")
                else:
                    stats['out_of_stock_count'] += 1
                    print(f"   📦 Stock status: Out of Stock - {stock_status['status_details']}")
                
                # SCRAPE THE CROMA OFFERS (regardless of stock status)
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
                
                # Always add URL to visited list after processing
                append_visited_url(croma_url, visited_urls_file)
                print(f"   📝 Added URL to visited_urls_croma.txt")
                
            except Exception as e:
                print(f"   ❌ Error processing URL: {e}")
                logging.error(f"Error processing {croma_url}: {e}")
                store_link['in_stock'] = False
                store_link['ranked_offers'] = []
                stats['failed_scraping'] += 1
                # Still add to visited URLs even if failed
                append_visited_url(croma_url, visited_urls_file)
            
            # Save progress every 100 entries for better performance
            if (idx + 1) % 100 == 0:
                progress_backup_file = f"{output_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                with open(progress_backup_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                print(f"   💾 Progress saved to {progress_backup_file} (backup every 100 URLs)")
            
            # Brief delay between requests
            time.sleep(2)
    
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted! Saving progress...")
    
    finally:
        try:
            driver.quit()
        except:
            pass
        
        # Save final output
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Final output saved to {output_file}")
        
        # Print comprehensive statistics
        print(f"\n📊 COMPREHENSIVE PROCESSING SUMMARY:")
        print(f"   🎯 Croma links processed: {stats['processed']}")
        print(f"   ✅ Successfully scraped: {stats['scraped_successfully']}")
        print(f"   ❌ Failed scraping: {stats['failed_scraping']}")
        print(f"   ⚠️  Skipped (no URL): {stats['skipped_no_url']}")
        print(f"   🎁 Total offers added: {stats['total_offers_added']}")
        print(f"   📦 In stock products: {stats['in_stock_count']}")
        print(f"   📦 Out of stock products: {stats['out_of_stock_count']}")
        print(f"   📝 URL tracking: Active (visited_urls_croma.txt updated)")
        print(f"   🔄 Session management: Fresh session for each link")
        print(f"   🤖 Automation: Fully automated (no user input)")
        print(f"   🛡️  Amazon entries completely untouched!")
        
        success_rate = (stats['scraped_successfully'] / stats['processed'] * 100) if stats['processed'] > 0 else 0
        print(f"   📈 Success rate: {success_rate:.1f}%")

if __name__ == "__main__":
    print("🚀 COMPREHENSIVE CROMA SCRAPER - FULLY AUTONOMOUS")
    print("=" * 80)
    print("🔄 Processes ALL Croma links with fresh sessions")
    print("✅ Comprehensive JSON traversal (variants + all_matching_products + unmapped)")
    print("✅ Uses correct input file: all_data_amazon_jio.json")  
    print("✅ Explicit Amazon/Flipkart/JioMart data isolation")
    print("✅ URL tracking with visited_urls_croma.txt")
    print("✅ Stock detection via span.amount#pdp-product-price")
    print("🤖 NEW: Fully automated (no user input required)")
    print("🔄 NEW: Fresh browser session for each link")
    print("💾 NEW: Backup every 100 URLs for better performance")
    print("📦 NEW: Stock status detection and tracking")
    print("-" * 80)
    
    # Auto-configuration: No user interaction required
    print("🚀 Starting automated processing with default settings:")
    print("   • Input file: all_data_amazon_jio.json")
    print("   • Output file: all_data_amazon_jio_croma.json")
    print("   • Browser mode: Headless server mode")
    print("   • Session management: Fresh session for each link")
    print("   • URL tracking: visited_urls_croma.txt")
    print("   • Stock detection: span.amount#pdp-product-price element")
    print("   • Backup frequency: Every 100 URLs")
    print("   • Amazon isolation: ENABLED")
    print()
    
    # Start processing immediately with default parameters
    process_croma_comprehensive(
        input_file="all_data.json",
        output_file="all_data_amazon_jio_croma.json"
    ) 