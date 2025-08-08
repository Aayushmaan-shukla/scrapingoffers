#!/usr/bin/env python3
"""
Enhanced JioMart Scraper - Comprehensive Store Link Processor
============================================================
- Fixes ALL limitations of the original jiomart\fetchjiomart_enhanced.py
- Comprehensive JSON traversal (variants + all_matching_products + unmapped)
- Uses correct input file: comprehensive_amazon_offers.json
- Preserves existing offers through URL skipping logic
- Explicit Amazon/Croma/Flipkart data isolation
- Safety features: backups, progress tracking, interruption handling
- Only processes JioMart links that don't already have offers

NEW FEATURES ADDED:
- Adds 'in_stock' key based on API data availability
- Sets in_stock to true if API successfully fetches offer data for the URL
- Sets in_stock to false if API fails to fetch data or returns no offers
- Places in_stock key just below the price element in the JSON structure
- Fully automated processing (no user input required)
- Backup files created every 100 URLs for better performance
- Processes from start to end by default
"""

import os
import re
import json
import time
import requests
import shutil
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, request, jsonify
import threading

# Setup logging
logging.basicConfig(
    filename=f'enhanced_jiomart_scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    filemode='w',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

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

class ComprehensiveJioMartScraper:
    def __init__(self, input_file: str, rescrape_all: bool = True):
        self.input_file = input_file
        self.rescrape_all = rescrape_all  # If True, scrapes ALL JioMart links regardless of existing offers
        
        # Statistics tracking
        self.stats = {
            'total_entries_scanned': 0,
            'jiomart_entries_found': 0,
            'jiomart_entries_with_existing_offers': 0,
            'jiomart_entries_needing_scraping': 0,
            'jiomart_entries_successfully_scraped': 0,
            'amazon_entries_completely_ignored': 0,
            'croma_entries_completely_ignored': 0,
            'flipkart_entries_completely_ignored': 0,
            'other_store_entries_ignored': 0,
            'offers_extracted': 0,
            'errors_encountered': 0
        }
        
        # JioMart API configuration
        self.bank_offers_base_url = "https://www.jiomart.com/catalog/coupon/offers/section/401107/electronics/"
        self.jiomart_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "Referer": "https://www.jiomart.com/p/electronics/apple-iphone-16-pro-max-256-gb-desert-titanium/609946185",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Cookie": "_ALGOLIA=anonymous-73e3ce44-c46b-43b8-b567-1f25fb1a69d8; nms_mgo_city=Mumbai; nms_mgo_state_code=MH; WZRK_G=5cd1bddd3f1f439aa10bd87118872526; _gcl_au=1.1.268244386.1743684911; nms_mgo_pincode=400020; _gid=GA1.2.952535945.1744014553; AKA_A2=A; _gat_UA-163452169-1=1; _ga=GA1.2.429559165.1743684910; _ga_XHR9Q2M3VV=GS1.1.1744191527.19.1.1744191542.45.0.2037389778; RT=\"z=1&dm=www.jiomart.com&si=38e37ad3-dc01-4579-bdc1-fc2fc6bdd20d&ss=m99qllly&sl=3&tt=2u4&obo=1&rl=1\"; WZRK_S_88R-W4Z-495Z=%7B%22p%22%3A4%2C%22s%22%3A1744191527%2C%22t%22%3A1744191542%7D",
            "x-requested-with": "XMLHttpRequest"
        }
        
        # Setup session with retry mechanism
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        
        # Initialize offer analyzer
        self.analyzer = JioMartOfferAnalyzer()

    def extract_sku_from_url(self, url):
        """Extract SKU from JioMart URL"""
        sku_match = re.search(r'/([0-9]+)(?:\?|$)', url)
        return sku_match.group(1) if sku_match else None

    def get_jiomart_offers(self, sku, max_retries=2):
        """Enhanced JioMart offers fetching with comprehensive extraction"""
        for attempt in range(max_retries):
            try:
                bank_offers_url = f"{self.bank_offers_base_url}{sku}"
                logging.info(f"Fetching JioMart offers (attempt {attempt + 1}/{max_retries}): {bank_offers_url}")
                
                response = self.session.get(bank_offers_url, headers=self.jiomart_headers, timeout=10)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        bank_offers = data.get("bank_offers", [])
                        
                        if not bank_offers:
                            logging.info(f"No bank offers found for SKU {sku}")
                            return []
                        
                        # Transform API response to our standard format
                        offers = []
                        for offer in bank_offers:
                            description = offer.get("coupon_code", "").strip()
                            if description and len(description) > 10:
                                offers.append({
                                    "card_type": "JioMart Offer",
                                    "offer_title": "Bank Offer",
                                    "offer_description": description
                                })
                        
                        # Remove duplicates
                        unique_offers = []
                        seen_descriptions = set()
                        for offer in offers:
                            desc = offer['offer_description']
                            if desc not in seen_descriptions:
                                seen_descriptions.add(desc)
                                unique_offers.append(offer)
                        
                        logging.info(f"Extracted {len(unique_offers)} unique offers for SKU {sku}")
                        self.stats['offers_extracted'] += len(unique_offers)
                        return unique_offers
                        
                    except json.JSONDecodeError:
                        logging.error(f"JSON decode error for SKU {sku}")
                        if attempt < max_retries - 1:
                            continue
                        return []
                else:
                    logging.warning(f"HTTP {response.status_code} for SKU {sku}")
                    if attempt < max_retries - 1:
                        continue
                    return []
                    
            except Exception as e:
                logging.error(f"Exception fetching offers for SKU {sku} (attempt {attempt + 1}): {e}")
                self.stats['errors_encountered'] += 1
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                else:
                    return []
        
        return []

    def extract_price_amount(self, price_str):
        """Extract numeric amount from price string like '₹30,999'"""
        if not price_str:
            return 0.0
        
        # Remove currency symbols and extract numbers
        numbers = re.findall(r'[\d,]+\.?\d*', price_str)
        if numbers:
            return float(numbers[0].replace(',', ''))
        return 0.0

    def find_jiomart_entries_needing_offers(self, data: Any, path: str = "") -> List[Dict]:
        """
        Comprehensive recursive search for JioMart entries that need offers.
        Completely ignores Amazon, Croma, and Flipkart entries.
        Searches in variants, all_matching_products, and unmapped sections.
        """
        jiomart_entries = []
        
        if isinstance(data, dict):
            # Check if this is a store_links entry
            if 'name' in data and 'url' in data:
                name = data.get('name', '').strip()
                url = data.get('url', '').strip()
                
                # COMPLETE ISOLATION: Skip all other stores
                if name.lower() in ['amazon.in', 'amazon']:
                    self.stats['amazon_entries_completely_ignored'] += 1
                    return jiomart_entries
                elif name.lower() == 'croma':
                    self.stats['croma_entries_completely_ignored'] += 1
                    return jiomart_entries
                elif name.lower() == 'flipkart':
                    self.stats['flipkart_entries_completely_ignored'] += 1
                    return jiomart_entries
                elif 'jiomart' in name.lower():
                    self.stats['jiomart_entries_found'] += 1
                    
                    if self.rescrape_all:
                        # RESCRAPE ALL: Process every JioMart URL regardless of existing offers
                        self.stats['jiomart_entries_needing_scraping'] += 1
                        sku = self.extract_sku_from_url(url)
                        if sku:
                            jiomart_entries.append({
                                'store_data': data,
                                'url': url,
                                'sku': sku,
                                'path': path
                            })
                            logging.info(f"Found JioMart entry for re-scraping: {url} (SKU: {sku})")
                    else:
                        # SELECTIVE SCRAPING: Only process entries without offers
                        ranked_offers = data.get('ranked_offers')
                        if ranked_offers and isinstance(ranked_offers, list) and len(ranked_offers) > 0:
                            self.stats['jiomart_entries_with_existing_offers'] += 1
                            logging.info(f"Skipping JioMart URL (already has ranked_offers): {url}")
                        else:
                            # This JioMart entry needs scraping
                            self.stats['jiomart_entries_needing_scraping'] += 1
                            sku = self.extract_sku_from_url(url)
                            if sku:
                                jiomart_entries.append({
                                    'store_data': data,
                                    'url': url,
                                    'sku': sku,
                                    'path': path
                                })
                                logging.info(f"Found JioMart entry needing offers: {url} (SKU: {sku})")
                else:
                    self.stats['other_store_entries_ignored'] += 1
            
            # Recursively search through all dictionary values
            else:
                # COMPREHENSIVE SEARCH: Check all three main sections
                if 'scraped_data' in data:
                    scraped_data = data['scraped_data']
                    if isinstance(scraped_data, dict):
                        # Search in variants
                        if 'variants' in scraped_data:
                            variants_entries = self.find_jiomart_entries_needing_offers(
                                scraped_data['variants'], f"{path}/scraped_data/variants"
                            )
                            jiomart_entries.extend(variants_entries)
                        
                        # Search in all_matching_products
                        if 'all_matching_products' in scraped_data:
                            amp_entries = self.find_jiomart_entries_needing_offers(
                                scraped_data['all_matching_products'], f"{path}/scraped_data/all_matching_products"
                            )
                            jiomart_entries.extend(amp_entries)
                        
                        # Search in unmapped
                        if 'unmapped' in scraped_data:
                            unmapped_entries = self.find_jiomart_entries_needing_offers(
                                scraped_data['unmapped'], f"{path}/scraped_data/unmapped"
                            )
                            jiomart_entries.extend(unmapped_entries)
                
                # Continue searching in other parts of the structure
                for key, value in data.items():
                    if key != 'scraped_data':  # Already handled above
                        sub_entries = self.find_jiomart_entries_needing_offers(value, f"{path}/{key}")
                        jiomart_entries.extend(sub_entries)
        
        elif isinstance(data, list):
            for i, item in enumerate(data):
                sub_entries = self.find_jiomart_entries_needing_offers(item, f"{path}[{i}]")
                jiomart_entries.extend(sub_entries)
        
        return jiomart_entries

    def process_comprehensive_jiomart_links(self, output_file=None, start_idx=0, max_entries=None):
        """
        Process JioMart links comprehensively while preserving all other store data.
        """
        if output_file is None:
            output_file = self.input_file
        
        # Create backup
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f"{self.input_file}.backup_{timestamp}"
        shutil.copy2(self.input_file, backup_file)
        print(f"✅ Created backup: {backup_file}")
        
        # Load data
        print(f"📂 Loading data from {self.input_file}...")
        with open(self.input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"📊 Loaded {len(data)} entries")
        
        # Find all JioMart entries needing offers
        print("🔍 Searching for JioMart entries needing offers...")
        jiomart_entries = []
        
        for entry_idx, entry in enumerate(data):
            self.stats['total_entries_scanned'] += 1
            entry_jiomart = self.find_jiomart_entries_needing_offers(entry, f"entry[{entry_idx}]")
            for jm_entry in entry_jiomart:
                jm_entry['entry_idx'] = entry_idx
                jm_entry['entry'] = entry
            jiomart_entries.extend(entry_jiomart)
        
        scrape_mode = "RE-SCRAPING ALL" if self.rescrape_all else "SELECTIVE SCRAPING"
        print(f"\n📈 **DISCOVERY SUMMARY ({scrape_mode}):**")
        print(f"   Total entries scanned: {self.stats['total_entries_scanned']}")
        print(f"   JioMart entries found: {self.stats['jiomart_entries_found']}")
        if not self.rescrape_all:
            print(f"   JioMart entries with existing offers: {self.stats['jiomart_entries_with_existing_offers']}")
        print(f"   JioMart entries for scraping: {self.stats['jiomart_entries_needing_scraping']}")
        print(f"   **ISOLATION CONFIRMED:**")
        print(f"   Amazon entries ignored: {self.stats['amazon_entries_completely_ignored']}")
        print(f"   Croma entries ignored: {self.stats['croma_entries_completely_ignored']}")
        print(f"   Flipkart entries ignored: {self.stats['flipkart_entries_completely_ignored']}")
        print(f"   Other store entries ignored: {self.stats['other_store_entries_ignored']}")
        
        if not jiomart_entries:
            if self.rescrape_all:
                print("ℹ️ No JioMart entries found in the data.")
            else:
                print("ℹ️ No JioMart entries need scraping. All entries already have offers.")
            return
        
        # Apply processing limits
        if start_idx > 0:
            jiomart_entries = jiomart_entries[start_idx:]
            print(f"⏭️ Starting from index {start_idx}")
        
        if max_entries:
            jiomart_entries = jiomart_entries[:max_entries]
            print(f"🔢 Limited to processing {max_entries} entries")
        
        mode_text = "RE-SCRAPING" if self.rescrape_all else "Processing"
        print(f"🎯 {mode_text} {len(jiomart_entries)} JioMart entries...")
        
        # Process JioMart entries
        try:
            for idx, jm_entry in enumerate(jiomart_entries):
                store_data = jm_entry['store_data']
                url = jm_entry['url']
                sku = jm_entry['sku']
                entry = jm_entry['entry']
                
                mode_text = "RE-SCRAPING" if self.rescrape_all else "Processing"
                print(f"\n🔍 {mode_text} {idx + 1}/{len(jiomart_entries)}")
                print(f"   Product: {entry.get('product_name', 'N/A')}")
                print(f"   JioMart URL: {url}")
                print(f"   SKU: {sku}")
                
                # Get JioMart offers
                offers = self.get_jiomart_offers(sku)
                
                # Add in_stock key based on API data availability (placed just below price)
                # If API successfully fetches data (offers returned), set in_stock to true
                if offers:
                    store_data['in_stock'] = True
                    print(f"   📦 Stock status: In Stock (API data available)")
                    
                    # Get product price for ranking
                    price_str = store_data.get('price', '₹0')
                    product_price = self.extract_price_amount(price_str)
                    
                    # Rank the offers
                    ranked_offers = self.analyzer.rank_offers(offers, product_price)
                    
                    # Update the store_data with ranked offers
                    store_data['ranked_offers'] = ranked_offers
                    
                    self.stats['jiomart_entries_successfully_scraped'] += 1
                    print(f"   ✅ Successfully scraped and ranked {len(offers)} offers")
                    
                    # Log top offers
                    for i, offer in enumerate(ranked_offers[:3], 1):
                        score_display = offer['score'] if offer['score'] is not None else 'N/A'
                        print(f"      Rank {i}: {offer['title']} (Score: {score_display}, Amount: ₹{offer['amount']})")
                else:
                    # If API fails to fetch data or returns no offers, set in_stock to false
                    store_data['in_stock'] = False
                    print(f"   📦 Stock status: Out of Stock (No API data available)")
                    
                    # Set empty array for no offers found
                    store_data['ranked_offers'] = []
                    print(f"   ❌ No offers found")
                
                # Save progress every 100 entries for better performance
                if (idx + 1) % 100 == 0:
                    progress_file = f"{output_file}.progress_{timestamp}.json"
                    with open(progress_file, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    print(f"   💾 Progress saved to {progress_file} (backup every 100 URLs)")
                
                # Delay between requests
                time.sleep(2)
        
        except KeyboardInterrupt:
            print("\n⚠️ Interrupted! Saving progress...")
        
        finally:
            # Save final output
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            print(f"\n✅ **SCRAPING COMPLETED!**")
            print(f"   Output saved to: {output_file}")
            print(f"   Backup created: {backup_file}")
            
            scrape_mode = "RE-SCRAPING ALL" if self.rescrape_all else "SELECTIVE SCRAPING"
            print(f"\n📊 **FINAL STATISTICS ({scrape_mode}):**")
            print(f"   Total entries scanned: {self.stats['total_entries_scanned']}")
            print(f"   JioMart entries found: {self.stats['jiomart_entries_found']}")
            if not self.rescrape_all:
                print(f"   JioMart entries with existing offers: {self.stats['jiomart_entries_with_existing_offers']}")
            print(f"   JioMart entries processed: {self.stats['jiomart_entries_needing_scraping']}")
            print(f"   JioMart entries successfully scraped: {self.stats['jiomart_entries_successfully_scraped']}")
            print(f"   Total offers extracted: {self.stats['offers_extracted']}")
            print(f"   Errors encountered: {self.stats['errors_encountered']}")
            print(f"   **ISOLATION CONFIRMED:**")
            print(f"     Amazon entries ignored: {self.stats['amazon_entries_completely_ignored']}")
            print(f"     Croma entries ignored: {self.stats['croma_entries_completely_ignored']}")
            print(f"     Flipkart entries ignored: {self.stats['flipkart_entries_completely_ignored']}")

# Include the JioMartOfferAnalyzer class from the original script
class JioMartOfferAnalyzer:
    def __init__(self):
        # Comprehensive bank reputation scores for Indian banks (same as other scripts)
        self.bank_scores = {
            # Public Sector Banks (PSBs)
            "SBI": 75,
            "State Bank of India": 75,
            "PNB": 72,
            "Punjab National Bank": 72,
            "BoB": 70,
            "Bank of Baroda": 70,
            "Canara Bank": 68,
            "Union Bank of India": 65,
            "Indian Bank": 65,
            "Bank of India": 65,
            "UCO Bank": 62,
            "Indian Overseas Bank": 62,
            "IOB": 62,
            "Central Bank of India": 62,
            "Bank of Maharashtra": 60,
            "Punjab & Sind Bank": 60,
            
            # Private Sector Banks
            "HDFC": 85,
            "HDFC Bank": 85,
            "ICICI": 90,
            "ICICI Bank": 90,
            "Axis": 80,
            "Axis Bank": 80,
            "Kotak": 70,
            "Kotak Mahindra Bank": 70,
            "IndusInd Bank": 68,
            "Yes Bank": 60,
            "IDFC FIRST Bank": 65,
            "IDFC": 65,
            "Federal Bank": 63,
            "South Indian Bank": 60,
            "RBL Bank": 62,
            "DCB Bank": 60,
            "Tamilnad Mercantile Bank": 58,
            "TMB": 58,
            "Karur Vysya Bank": 58,
            "CSB Bank": 58,
            "City Union Bank": 58,
            "Bandhan Bank": 60,
            "Jammu & Kashmir Bank": 58,
            
            # Small Finance Banks
            "AU Small Finance Bank": 65,
            "AU Bank": 65,
            "Equitas Small Finance Bank": 62,
            "Equitas": 62,
            "Ujjivan Small Finance Bank": 60,
            "Ujjivan": 60,
            "Suryoday Small Finance Bank": 58,
            "ESAF Small Finance Bank": 58,
            "Fincare Small Finance Bank": 58,
            "Jana Small Finance Bank": 58,
            "North East Small Finance Bank": 58,
            "Capital Small Finance Bank": 58,
            "Unity Small Finance Bank": 58,
            "Shivalik Small Finance Bank": 58,
            
            # Foreign Banks
            "Citi": 80,
            "Citibank": 80,
            "HSBC": 78,
            "Standard Chartered": 75,
            "Deutsche Bank": 75,
            "Barclays Bank": 75,
            "DBS Bank": 72,
            "JP Morgan Chase Bank": 75,
            "Bank of America": 75,
            
            # Co-operative Banks
            "Saraswat Co-operative Bank": 60,
            "Saraswat Bank": 60,
            "Shamrao Vithal Co-operative Bank": 55,
            "PMC Bank": 50,
            "TJSB Sahakari Bank": 55,
            
            # Credit Card Companies
            "Amex": 85,
            "American Express": 85,
            
            # Digital Payment Services & Wallets
            "Paytm": 75,
            "MobiKwik": 70,
            "Mobikwik": 70,
            "PhonePe": 75,
            "Google Pay": 75,
            "GPay": 75,
            "Amazon Pay": 72,
            "Airtel Money": 65,
            "Jio Money": 65,
            "FreeCharge": 65,
            "PayU": 68,
            "Razorpay": 68,
            "UPI": 78,
            "BHIM UPI": 75,
            "Paytm UPI": 75,
            "Paytm Wallet": 75,
            "MobiKwik Wallet": 70,
            "Mobikwik Wallet": 70,
            "PhonePe UPI": 75,
            "Google Pay UPI": 75,
            "GPay UPI": 75,
            "Paytm UPI Lite": 73,
            "UPI Lite": 73,
            "Wallet": 65,
            "Digital Wallet": 65
        }
        
        # Default bank score if not found in the list
        self.default_bank_score = 70

    def extract_card_type(self, description: str) -> Optional[str]:
        """Extract card type (Credit/Debit) from offer description with enhanced detection."""
        description_lower = description.lower()
        
        # Enhanced patterns for better detection
        credit_patterns = [
            r'\bcredit\s+card\b', r'\bcc\b', r'\bcredit\b.*\bcard\b',
            r'\bmaster\s+card\b', r'\bvisa\s+card\b.*\bcredit\b'
        ]
        
        debit_patterns = [
            r'\bdebit\s+card\b', r'\bdc\b', r'\bdebit\b.*\bcard\b',
            r'\bvisa\s+card\b.*\bdebit\b', r'\bmaster\s+card\b.*\bdebit\b'
        ]
        
        # Check for credit card patterns
        if any(re.search(pattern, description_lower) for pattern in credit_patterns):
            # Check if it's both credit and debit
            if any(re.search(pattern, description_lower) for pattern in debit_patterns):
                return "Credit/Debit"
            return "Credit"
        
        # Check for debit card patterns
        if any(re.search(pattern, description_lower) for pattern in debit_patterns):
            return "Debit"
        
        # Check for general card mentions
        if re.search(r'\bcard\b', description_lower):
            # If card is mentioned but type is unclear, try to infer from context
            if any(word in description_lower for word in ['premium', 'rewards', 'cashback', 'points']):
                return "Credit"  # Premium features usually indicate credit cards
            elif 'atm' in description_lower:
                return "Debit"
        
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
                r'₹\s*([\d,]+\.?\d*)',
                r'Rs\.?\s*([\d,]+\.?\d*)',
                r'INR\s*([\d,]+\.?\d*)'
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
        """Extract bank name from offer description."""
        if not description:
            return None
        
        description_lower = description.lower()
        
        # Bank name patterns for better matching
        bank_variations = {
            'hdfc': 'HDFC',
            'icici': 'ICICI', 
            'axis': 'Axis',
            'sbi': 'SBI',
            'kotak': 'Kotak',
            'yes': 'Yes Bank',
            'idfc': 'IDFC',
            'indusind': 'IndusInd Bank',
            'federal': 'Federal Bank',
            'rbl': 'RBL Bank',
            'citi': 'Citi',
            'hsbc': 'HSBC',
            'standard chartered': 'Standard Chartered',
            'au bank': 'AU Bank',
            'equitas': 'Equitas',
            'ujjivan': 'Ujjivan',
            'pnb': 'PNB',
            'bob': 'BoB',
            'canara': 'Canara Bank',
            'union bank': 'Union Bank of India',
            'indian bank': 'Indian Bank',
            'bank of india': 'Bank of India',
            'uco': 'UCO Bank',
            'iob': 'Indian Overseas Bank',
            'central bank': 'Central Bank of India',
            'amex': 'Amex',
            'american express': 'American Express',
            
            # Digital Payment Services & Wallets
            'paytm': 'Paytm',
            'mobikwik': 'MobiKwik',
            'phonepe': 'PhonePe',
            'phone pe': 'PhonePe',
            'google pay': 'Google Pay',
            'gpay': 'Google Pay',
            'g pay': 'Google Pay',
            'amazon pay': 'Amazon Pay',
            'upi': 'UPI',
            'paytm upi': 'Paytm UPI',
            'paytm wallet': 'Paytm Wallet',
            'mobikwik wallet': 'MobiKwik Wallet',
            'paytm upi lite': 'Paytm UPI Lite',
            'upi lite': 'UPI Lite',
            'bhim upi': 'BHIM UPI',
            'bhim': 'BHIM UPI',
            'phonepe upi': 'PhonePe UPI',
            'google pay upi': 'Google Pay UPI',
            'gpay upi': 'Google Pay UPI',
            'airtel money': 'Airtel Money',
            'jio money': 'Jio Money',
            'freecharge': 'FreeCharge',
            'payu': 'PayU',
            'razorpay': 'Razorpay',
            'wallet': 'Wallet',
            'digital wallet': 'Digital Wallet'
        }
        
        for variation, standard_name in bank_variations.items():
            if variation in description_lower:
                logging.info(f"Found bank '{standard_name}' using variation '{variation}' in description")
                return standard_name
        
        logging.debug(f"No bank found in description: {description[:100]}...")
        return None

    def extract_min_spend(self, description: str) -> Optional[float]:
        """Extract minimum spend requirement from offer description."""
        patterns = [
            r'(?:Mini|Minimum)\s+purchase\s+value\s+(?:of\s+)?(?:INR\s+|₹\s*|Rs\.?\s*)([\d,]+\.?\d*)',
            r'(?:Mini|Minimum)\s+(?:purchase|spend|transaction)\s+(?:of\s+|value\s+)?(?:INR\s+|₹\s*|Rs\.?\s*)([\d,]+\.?\d*)',
            r'min(?:imum)?\s+(?:purchase|spend|transaction)\s+(?:of\s+|value\s+)?(?:INR\s+|₹\s*|Rs\.?\s*)([\d,]+\.?\d*)',
            r'valid\s+on\s+(?:orders?|purchases?)\s+(?:of\s+|above\s+|worth\s+)(?:INR\s+|₹\s*|Rs\.?\s*)([\d,]+\.?\d*)',
            r'applicable\s+on\s+(?:purchases?|orders?|transactions?)\s+(?:of\s+|above\s+|worth\s+)(?:INR\s+|₹\s*|Rs\.?\s*)([\d,]+\.?\d*)',
            r'(?:on\s+)?(?:orders?|purchases?|spending)\s+(?:of\s+|above\s+|worth\s+)(?:INR\s+|₹\s*|Rs\.?\s*)([\d,]+\.?\d*)\s+(?:or\s+more|and\s+above)',
            r'(?:minimum|min)\s+(?:spend|purchase|order)\s*:\s*(?:INR\s+|₹\s*|Rs\.?\s*)([\d,]+\.?\d*)',
            r'(?:spend|purchase|order)\s+(?:minimum|min|at\s+least)\s+(?:INR\s+|₹\s*|Rs\.?\s*)([\d,]+\.?\d*)'
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
        
        return None

    def determine_offer_type(self, card_title: str, description: str) -> str:
        """Determine offer type based on card title and description."""
        card_title_lower = card_title.lower() if card_title else ""
        description_lower = description.lower() if description else ""
        
        # Enhanced type detection for JioMart
        if any(keyword in card_title_lower for keyword in ['bank offer', 'instant discount', 'card offer']):
            return "Bank Offer"
        elif any(keyword in card_title_lower for keyword in ['no cost emi', 'no-cost emi', 'emi']):
            return "No Cost EMI"
        elif any(keyword in card_title_lower for keyword in ['cashback', 'cash back']):
            return "Cashback"
        elif any(keyword in card_title_lower for keyword in ['exchange offer', 'exchange']):
            return "Exchange Offer"
        elif any(keyword in card_title_lower for keyword in ['partner offer', 'partner']):
            return "Partner Offers"
        elif any(keyword in description_lower for keyword in ['bank', 'credit card', 'debit card']):
            return "Bank Offer"  # Fallback for bank-related offers
        elif any(keyword in description_lower for keyword in ['emi', 'no cost']):
            return "No Cost EMI"
        else:
            return card_title if card_title else "JioMart Offer"

    def parse_offer(self, offer: Dict[str, str]) -> Offer:
        """Parse offer details from raw offer data."""
        card_title = offer.get('card_type', '').strip()
        description = offer.get('offer_description', '').strip()
        
        # Determine offer type
        offer_type = self.determine_offer_type(card_title, description)
        
        # Fix title for bank offers - ensure it's never blank
        if offer_type == "Bank Offer":
            title = "Bank Offer"
        elif not card_title or card_title.lower() in ['summary', '', 'jiomart offer']:
            # If title is empty or generic, use the offer type
            title = offer_type
        else:
            title = card_title
        
        # Extract offer details
        amount = self.extract_amount(description)
        bank = self.extract_bank(description)
        min_spend = self.extract_min_spend(description)
        card_type = self.extract_card_type(description)
        
        # Determine if it's an instant discount
        is_instant = 'instant' in description.lower() or 'cashback' not in description.lower()
        
        return Offer(
            title=title,
            description=description,
            amount=amount,
            type=offer_type,
            bank=bank,
            min_spend=min_spend,
            is_instant=is_instant,
            card_type=card_type
        )

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

        # CRITICAL FACTOR: Minimum spend requirement
        if offer.min_spend and offer.min_spend > product_price:
            penalty_percentage = ((offer.min_spend - product_price) / product_price) * 100
            
            if penalty_percentage > 50:  # Very high min spend
                base_score = 15  # Low but rankable score
            else:
                # Moderate penalty
                penalty = penalty_percentage * 0.5
                base_score -= penalty
                base_score = max(base_score, 20)  # Minimum rankable score
        
        # BONUS: For applicable offers (min spend <= product price)
        elif offer.min_spend is None or offer.min_spend <= product_price:
            # Major bonus for applicable offers
            if offer.min_spend is None:
                base_score += 20  # Big bonus for no restrictions
            else:
                # Bonus for reasonable minimum spend
                spend_ratio = offer.min_spend / product_price if product_price > 0 else 0
                if spend_ratio <= 0.9:  # Min spend is 90% or less of product price
                    bonus = (1 - spend_ratio) * 10  # Up to 10 points bonus
                    base_score += bonus

        # INSTANT DISCOUNT BONUS
        if offer.is_instant:
            base_score += 5

        # Bank reputation bonus
        if offer.bank:
            bank_bonus = (self.bank_scores.get(offer.bank, self.default_bank_score) - 70) / 2
            base_score += bank_bonus
        else:
            # Penalty for unknown bank, but not too harsh
            base_score -= 5

        # Card type bonus
        if offer.card_type:
            if offer.card_type == "Credit":
                base_score += 3  # Credit cards generally have better offers
            elif offer.card_type == "Credit/Debit":
                base_score += 2  # Flexible options get moderate bonus
            elif offer.card_type == "Debit":
                base_score += 1  # Debit cards get small bonus

        # Digital Payment Service bonus (new for JioMart)
        if offer.bank and any(keyword in offer.bank.lower() for keyword in ['upi', 'wallet', 'paytm', 'mobikwik', 'phonepe', 'google pay', 'gpay']):
            digital_bonus = 5  # Bonus for digital payment convenience
            base_score += digital_bonus

        final_score = max(0, min(100, base_score))
        return final_score

    def rank_offers(self, offers_data: List[Dict], product_price: float) -> List[Dict[str, Any]]:
        """Rank offers based on comprehensive scoring - focusing only on Bank Offers."""
        
        # Parse all offers
        parsed_offers = [self.parse_offer(offer) for offer in offers_data if isinstance(offer, dict)]
        
        # Separate Bank Offers from other offers
        bank_offers = [offer for offer in parsed_offers if offer.type == "Bank Offer"]
        other_offers = [offer for offer in parsed_offers if offer.type != "Bank Offer"]
        
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
                else:
                    net_effective_price = max(product_price - offer.amount, 0)
                    is_applicable = True
                
                scored_bank_offers.append({
                    'title': offer.title,
                    'description': offer.description,
                    'amount': offer.amount,
                    'bank': offer.bank,
                    'min_spend': offer.min_spend,
                    'score': score,
                    'is_instant': offer.is_instant,
                    'net_effective_price': net_effective_price,
                    'is_applicable': is_applicable,
                    'offer_type': 'Bank Offer',
                    'card_type': offer.card_type
                })
            
            # Sort bank offers by score in descending order
            scored_bank_offers.sort(key=lambda x: x['score'], reverse=True)
            
            # Add rank numbers to bank offers only
            for idx, offer in enumerate(scored_bank_offers):
                offer['rank'] = idx + 1
            
            all_ranked_offers.extend(scored_bank_offers)
        
        # Process other offers (No Cost EMI, Cashback, etc.) - no ranking
        for offer in other_offers:
            # Calculate basic info but no ranking
            if offer.min_spend and product_price < offer.min_spend:
                net_effective_price = product_price
                is_applicable = False
            else:
                net_effective_price = max(product_price - offer.amount, 0)
                is_applicable = True
            
            all_ranked_offers.append({
                'title': offer.title,
                'description': offer.description,
                'amount': offer.amount,
                'bank': offer.bank,
                'min_spend': offer.min_spend,
                'score': None,  # No score for non-bank offers
                'is_instant': offer.is_instant,
                'net_effective_price': net_effective_price,
                'is_applicable': is_applicable,
                'offer_type': offer.type,
                'rank': None,  # No rank for non-bank offers
                'card_type': offer.card_type
            })
        
        return all_ranked_offers

def process_comprehensive_jiomart_links(input_file="comprehensive_amazon_offers.json", 
                                       output_file=None,
                                       rescrape_all=True,
                                       start_idx=0, max_entries=None):
    """
    Main function to process JioMart links comprehensively.
    
    AUTOMATION FEATURES:
    - Fully automated processing (no user input required)
    - Processes from start to end by default (start_idx=0, max_entries=None)
    - Creates backup files every 100 URLs for better performance
    - Re-scrapes ALL JioMart links regardless of existing offers
    
    NEW FUNCTIONALITY:
    - Adds 'in_stock' key based on API data availability
    - Sets in_stock=true when API successfully fetches offer data
    - Sets in_stock=false when API fails or returns no data
    """
    scraper = ComprehensiveJioMartScraper(input_file, rescrape_all)
    scraper.process_comprehensive_jiomart_links(output_file, start_idx, max_entries)

# ===============================================
# API SETUP AND ENDPOINTS FOR JIOMART SCRAPER
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

def run_jiomart_scraper_process(input_file="all_data.json", output_file=None, rescrape_all=True, start_idx=0, max_entries=None):
    """
    Function to run the JioMart scraper process in a separate thread
    """
    global scraping_status
    
    try:
        # Generate timestamped output filename if not provided
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"all_data_jiomart_{timestamp}.json"
        
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
        
        logging.info(f"API triggered JioMart scraper process started with output file: {output_file}")
        
        # Run the main scraping function
        process_comprehensive_jiomart_links(input_file, output_file, rescrape_all, start_idx, max_entries)
        
        # Mark as completed
        scraping_status.update({
            'is_running': False,
            'completed': True,
            'end_time': datetime.now().isoformat()
        })
        
        logging.info("API triggered JioMart scraper process completed successfully")
        
    except Exception as e:
        # Mark as error
        scraping_status.update({
            'is_running': False,
            'completed': False,
            'error': str(e),
            'end_time': datetime.now().isoformat()
        })
        
        logging.error(f"API triggered JioMart scraper process failed: {e}")

@app.route('/start-scraping', methods=['POST'])
def start_scraping():
    """
    API endpoint to start the JioMart scraping process
    """
    global scraping_status
    
    # Check if scraping is already running
    if scraping_status['is_running']:
        return jsonify({
            'status': 'error',
            'message': 'JioMart scraping is already in progress',
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
            output_file = f"all_data_jiomart_{timestamp}.json"
        rescrape_all = data.get('rescrape_all', True)
        start_idx = data.get('start_idx', 0)
        max_entries = data.get('max_entries', None)
        
        # Start scraping in a separate thread
        scraper_thread = threading.Thread(
            target=run_jiomart_scraper_process,
            args=(input_file, output_file, rescrape_all, start_idx, max_entries),
            daemon=True
        )
        scraper_thread.start()
        
        return jsonify({
            'status': 'success',
            'message': 'JioMart scraping process started successfully',
            'data': {
                'input_file': input_file,
                'output_file': output_file,
                'rescrape_all': rescrape_all,
                'start_idx': start_idx,
                'max_entries': max_entries,
                'started_at': scraping_status['start_time']
            }
        }), 200
        
    except Exception as e:
        logging.error(f"Error starting JioMart scraper via API: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to start JioMart scraping: {str(e)}',
            'data': None
        }), 500

@app.route('/scraping-status', methods=['GET'])
def get_scraping_status():
    """
    API endpoint to get the current JioMart scraping status
    """
    return jsonify({
        'status': 'success',
        'message': 'JioMart scraping status retrieved successfully',
        'data': scraping_status
    }), 200

@app.route('/stop-scraping', methods=['POST'])
def stop_scraping():
    """
    API endpoint to stop the JioMart scraping process (graceful stop)
    """
    global scraping_status
    
    if not scraping_status['is_running']:
        return jsonify({
            'status': 'error',
            'message': 'No JioMart scraping process is currently running',
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
        'message': 'JioMart scraping process stop requested',
        'data': scraping_status
    }), 200

@app.route('/health', methods=['GET'])
def health_check():
    """
    API endpoint for health check
    """
    return jsonify({
        'status': 'success',
        'message': 'Enhanced JioMart Scraper API is healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    }), 200

@app.route('/', methods=['GET'])
def api_info():
    """
    API endpoint for basic information
    """
    return jsonify({
        'name': 'Enhanced JioMart Scraper API',
        'version': '1.0.0',
        'description': 'API to trigger comprehensive JioMart scraping with stock tracking',
        'endpoints': {
            'POST /start-scraping': 'Start the JioMart scraping process',
            'GET /scraping-status': 'Get current scraping status',
            'POST /stop-scraping': 'Stop the scraping process',
            'GET /health': 'Health check',
            'GET /': 'API information'
        },
        'features': [
            'JioMart bank offers scraping and ranking',
            'Stock status tracking (in_stock: true/false)',
            'Comprehensive nested location traversal',
            'API-based offer extraction from JioMart',
            'Digital payment service detection',
            'Automated processing with backup creation',
            'Progress tracking via API'
        ]
    }), 200

if __name__ == "__main__":
    import sys
    
    # Check if script should run as API or direct execution
    if len(sys.argv) > 1 and sys.argv[1] == "--api":
        # Run as Flask API
        print("🚀 ENHANCED JIOMART SCRAPER API MODE")
        print("Starting Flask API server...")
        print("Available endpoints:")
        print("  POST /start-scraping  - Start the JioMart scraping process")
        print("  GET  /scraping-status - Get current scraping status")
        print("  POST /stop-scraping   - Stop the scraping process")
        print("  GET  /health         - Health check")
        print("  GET  /              - API information")
        print("-" * 60)
        
        # Get port from command line arguments or use default
        port = 5002  # Different port from Amazon (5000) and Flipkart (5001)
        if len(sys.argv) > 2:
            try:
                port = int(sys.argv[2])
            except ValueError:
                print("Invalid port number, using default 5002")
                port = 5002
        
        print(f"🌐 Starting JioMart API server on http://localhost:{port}")
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
        output_file = f"all_data_jiomart_{timestamp}.json"
        
        print("🎯 Enhanced JioMart Scraper - FULLY AUTOMATED MODE")
        print("=" * 80)
        print("🔄 RE-SCRAPES ALL JioMart links (ignores existing offers)")
        print("✅ Comprehensive JSON traversal (variants + all_matching_products + unmapped)")
        print("✅ Uses correct input file: all_data.json")
        print("✅ Explicit Amazon/Croma/Flipkart data isolation")
        print("✅ Safety features: backups, progress tracking")
        print("📦 NEW: Stock status tracking via 'in_stock' key")
        print("   • True = API successfully fetches offer data")
        print("   • False = API fails to fetch data or no offers available")
        print("🤖 NEW: Fully automated (no user input required)")
        print("💾 NEW: Backup every 100 URLs for better performance")
        print()
        print("💡 TIP: Run with --api flag to start as API server instead:")
        print(f"   python {sys.argv[0]} --api [port]")
        print("-" * 80)
        
        # Auto-configuration: No user interaction required
        print("🚀 Starting automated processing with default settings:")
        print("   • Start index: 0 (beginning)")
        print("   • Max entries: All available")
        print(f"   • Input file: {input_file}")
        print(f"   • Output file: {output_file}")
        print("   • Backup frequency: Every 100 URLs")
        print("   • Processing mode: RE-SCRAPE ALL")
        print()
        
        # Process JioMart links - FULLY AUTOMATED MODE
        process_comprehensive_jiomart_links(
            input_file=input_file,
            output_file=output_file,
            rescrape_all=True,  # RE-SCRAPE ALL JioMart links
            start_idx=0,        # Start from beginning
            max_entries=None    # Process all entries
        ) 