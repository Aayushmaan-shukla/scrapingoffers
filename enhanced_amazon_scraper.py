import os
import re
import json
import time
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from flask import Flask, request, jsonify
import threading

# Setup logging
logging.basicConfig(
    filename='enhanced_amazon_scraper.log',
    filemode='a',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

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
        
        # Get page soup for element checking
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Initialize variables
        price = None
        in_stock = True  # Default to in stock
        availability = "Available"  # Default availability
        
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
            'extracted_at': datetime.now().isoformat()
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
            'error': str(e)
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
        # Comprehensive bank reputation scores for Indian banks
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
            "American Express": 85
        }
        
        # Enhanced bank name patterns for better matching
        self.bank_name_patterns = {
            # Common abbreviations and variations
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
            "Ujjivan": ["Ujjivan", "Ujjivan Bank"],
            "PNB": ["PNB", "Punjab National Bank"],
            "BoB": ["BoB", "Bank of Baroda", "Baroda"],
            "Canara": ["Canara", "Canara Bank"],
            "Union Bank": ["Union Bank", "Union Bank of India"],
            "Indian Bank": ["Indian Bank"],
            "Bank of India": ["Bank of India"],
            "UCO Bank": ["UCO", "UCO Bank"],
            "Iob": ["IOB", "Indian Overseas Bank"],
            "Central Bank": ["Central Bank", "Central Bank of India"],
            "Bank of Maharashtra": ["Bank of Maharashtra", "Maharashtra Bank"],
            "Amex": ["Amex", "American Express"],
            "DBS": ["DBS", "DBS Bank"]
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
        
        # Normalize common synonyms to Credit/Debit
        if any(phrase in description_lower for phrase in [
            'credit & debit', 'credit and debit', 'credit/debit', 'credit or debit', 'both credit and debit'
        ]):
            return "Credit/Debit"

        # Check for credit card patterns
        credit_match = any(re.search(pattern, description_lower) for pattern in credit_patterns)
        # Check for debit card patterns
        debit_match = any(re.search(pattern, description_lower) for pattern in debit_patterns)

        if credit_match and debit_match:
            return "Credit/Debit"
        if credit_match:
            return "Credit"
        if debit_match:
            return "Debit"

        # If card is mentioned but no explicit credit/debit, treat as Credit/Debit to avoid misses
        has_card_word = re.search(r'\bcard(s)?\b', description_lower) is not None
        has_bank_offer_word = 'bank offer' in description_lower or ('bank' in description_lower and 'offer' in description_lower)
        mentions_provider = any(p.lower() in description_lower for p in self.card_providers)
        if has_card_word or has_bank_offer_word or mentions_provider:
            return "Credit/Debit"

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
                    return bank_key
        
        # If no pattern match, try direct bank scores dictionary
        sorted_banks = sorted(self.bank_scores.keys(), key=len, reverse=True)
        for bank in sorted_banks:
            if bank.lower() in description_lower:
                logging.info(f"Found bank '{bank}' through direct matching in description")
                return bank
        
        # Enhanced pattern matching for common bank variations
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
            'american express': 'American Express'
        }
        
        for variation, standard_name in bank_variations.items():
            if variation in description_lower:
                logging.info(f"Found bank '{standard_name}' using variation '{variation}' in description")
                return standard_name
        
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
        # Replace any ambiguous or 'both' type with explicit "Credit/Debit"
        if card_type and card_type.strip().lower() in { 'both', 'credit & debit', 'credit and debit', 'credit/debit', 'credit or debit' }:
            card_type = "Credit/Debit"
        card_provider = self.extract_card_provider(description)
        
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
                    bank_info = f"using your {offer.bank} {offer.card_type.lower()} card"
                elif offer.bank:
                    bank_info = f"using your {offer.bank} card"
                elif offer.card_type:
                    bank_info = f"using your {offer.card_type.lower()} card"
                
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
                    note_parts.append(f"However, if you reach the minimum spend using your {offer.bank} {offer.card_type.lower()} card, you could save ₹{offer.amount:,.0f}!")
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

def create_chrome_driver():
    """
    Create and configure a new Chrome driver session.
    """
    options = uc.ChromeOptions()
    
    print("🤖 Running in headless mode (server mode)")
    options.add_argument('--headless=new')  # Use new headless mode
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_argument('--window-size=1920,1080')
    
    # Additional options for better compatibility
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    return uc.Chrome(options=options)

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
    5. Appends processed URLs to visited_urls.txt to avoid re-processing
    6. BROWSER SESSION MANAGEMENT: Creates fresh Chrome session for each link
    """
    
    # Load the JSON data
    print(f"📖 Loading data from {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"✅ Loaded {len(data)} entries")
    
    # Setup visited URLs tracking with new functionality
    visited_urls_file = manage_visited_urls_file("visited_urls.txt")
    visited_urls = load_visited_urls(visited_urls_file)
    
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
    
    # Setup Chrome driver and analyzer
    driver = create_chrome_driver()
    analyzer = OfferAnalyzer()
    
    # Session management variables  
    SESSION_REFRESH_PER_LINK = True  # Recreate session after each processed link
    
    try:
        for idx, link_data in enumerate(amazon_store_links):
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
            
            # Check if URL has already been visited/scraped
            if amazon_url in visited_urls:
                print(f"   ⏭️  URL already scraped, skipping to preserve existing offers")
                continue
            
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
            
            # Extract price and availability information
            price_availability_info = extract_price_and_availability(driver, amazon_url)
            
            # Set in_stock status based on extracted information
            store_link['in_stock'] = price_availability_info.get('in_stock', True)
            
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
            
            # Get bank offers
            offers = get_bank_offers(driver, amazon_url)
            
            if offers:
                # Get product price for ranking
                price_str = store_link.get('price', '₹0')
                product_price = extract_price_amount(price_str)
                
                # Rank the offers
                ranked_offers = analyzer.rank_offers(offers, product_price)
                
                # Update the store_link with ranked offers
                store_link['ranked_offers'] = ranked_offers
                
                print(f"   ✅ Found and ranked {len(offers)} bank offers")
                
                # Log the ranking summary
                for i, offer in enumerate(ranked_offers[:3], 1):
                    score_display = offer['score'] if offer['score'] is not None else 'N/A'
                    print(f"      🏆 Rank {i}: {offer['title']} (Score: {score_display}, Amount: ₹{offer['amount']})")
            else:
                print(f"   ❌ No offers found")
                store_link['ranked_offers'] = []
            
            # Add URL to visited list after successful processing
            append_visited_url(amazon_url, visited_urls_file)
            visited_urls.add(amazon_url)
            
            # Session refreshed after each link - no counter needed
            
            # Save progress every 100 entries (optimized backup frequency)
            if (idx + 1) % 100 == 0:
                backup_file = f"{output_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(backup_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                print(f"   💾 Progress saved to {backup_file} (every 100 URLs)")
            
            # Small delay between requests
            time.sleep(2)
    
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted! Saving progress...")
    
    finally:
        driver.quit()
        
        # Save final output
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Final output saved to {output_file}")
        
        # Enhanced Summary
        total_processed = sum(1 for link_data in amazon_store_links 
                            if link_data['store_link'].get('ranked_offers'))
        total_offers = sum(len(link_data['store_link'].get('ranked_offers', [])) 
                         for link_data in amazon_store_links)
        
        print(f"\n📊 COMPREHENSIVE SUMMARY:")
        print(f"   🎯 EXTRACTION STATS:")
        print(f"      Found Amazon links in variants: {extractor.stats['amazon_links_variants']}")
        print(f"      Found Amazon links in all_matching_products: {extractor.stats['amazon_links_all_matching_products']}")
        print(f"      Found Amazon links in unmapped: {extractor.stats['amazon_links_unmapped']}")
        print(f"      Total Amazon links found: {extractor.stats['total_amazon_links']}")
        print(f"   🔄 PROCESSING STATS:")
        print(f"      Processed Amazon links: {total_processed}")
        print(f"      Total ranked offers: {total_offers}")
        print(f"      Entries with Amazon links: {extractor.stats['entries_with_amazon']}")

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
            output_file = f"all_data_amazon_{timestamp}.json"
        
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
        
        input_file = data.get('input_file', 'all_data.json')
        # Generate timestamped output filename if not provided
        output_file = data.get('output_file', None)
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"all_data_amazon_{timestamp}.json"
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
        input_file = "all_data.json"
        # Generate timestamped output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"all_data_amazon_{timestamp}.json"
        
        print("🚀 ENHANCED COMPREHENSIVE AMAZON SCRAPER WITH PRICE & AVAILABILITY TRACKING")
        print("This script finds ALL Amazon store links in deep nested JSON and adds:")
        print("  🎯 Product prices and availability status")
        print("  🏆 Ranked bank offers") 
        print("  📝 URL visit tracking (visited_urls.txt)")
        print("  🔄 Smart session management (fresh session for each link)")
        print("🎯 FEATURES: Price extraction + Availability checking + Bank offers + URL tracking + Per-link session refresh")
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
        print()
        
        process_comprehensive_amazon_store_links(input_file, output_file, start_idx, max_entries) 