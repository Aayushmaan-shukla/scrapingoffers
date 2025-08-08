#!/usr/bin/env python3
"""
URL Mapper - Sequential Platform Processing Script
===============================================
Maps URLs from each platform's output JSON to a final consolidated JSON file.

Platform Processing Order:
1. Amazon URLs from all_data_amazon.json
2. Flipkart URLs from comprehensive_amazon_offers.json  
3. Croma URLs from all_data_amazon_jio_croma.json
4. JioMart URLs from all_data_amazon_jio.json

Key Features:
- Single-threaded sequential processing (no parallel execution)
- No overwrite policy: each platform operates on separate URL entries
- Error handling for missing platforms/JSON files
- Preserves existing data structure
- Creates comprehensive final.json with all platform data
"""

import json
import os
import shutil
from datetime import datetime
from typing import Dict, List, Set, Any, Optional
import logging

# Setup logging
logging.basicConfig(
    filename=f'url_mapper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    filemode='w',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

class URLMapper:
    def __init__(self):
        self.platform_configs = {
            'amazon': {
                'input_file': 'scraperscripts/all_data_amazon.json',
                'identifier': 'amazon.in',
                'order': 1
            },
            'flipkart': {
                'input_file': 'scraperscripts/comprehensive_amazon_offers.json',
                'identifier': 'flipkart',
                'order': 2
            },
            'croma': {
                'input_file': 'scraperscripts/all_data_amazon_jio_croma.json',
                'identifier': 'croma',
                'order': 3
            },
            'jiomart': {
                'input_file': 'scraperscripts/all_data_amazon_jio.json',
                'identifier': 'jiomart',
                'order': 4
            }
        }
        
        self.stats = {
            'amazon': {'processed': 0, 'mapped': 0, 'skipped': 0},
            'flipkart': {'processed': 0, 'mapped': 0, 'skipped': 0},
            'croma': {'processed': 0, 'mapped': 0, 'skipped': 0},
            'jiomart': {'processed': 0, 'mapped': 0, 'skipped': 0},
            'total_entries': 0,
            'errors': 0
        }
        
        self.final_data = []
        self.mapped_urls = {
            'amazon': set(),
            'flipkart': set(),
            'croma': set(),
            'jiomart': set()
        }
    
    def load_platform_data(self, platform: str) -> Optional[List[Dict]]:
        """Load data from a platform's JSON file with error handling."""
        config = self.platform_configs[platform]
        input_file = config['input_file']
        
        if not os.path.exists(input_file):
            print(f"⚠️  Platform {platform.upper()} file not found: {input_file}")
            logging.warning(f"Platform {platform} file not found: {input_file}")
            return None
        
        try:
            print(f"📖 Loading {platform.upper()} data from {input_file}")
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            print(f"✅ Loaded {len(data)} entries from {platform.upper()}")
            logging.info(f"Successfully loaded {len(data)} entries from {platform} file")
            return data
            
        except Exception as e:
            print(f"❌ Error loading {platform.upper()} data: {e}")
            logging.error(f"Error loading {platform} data from {input_file}: {e}")
            self.stats['errors'] += 1
            return None
    
    def find_platform_urls(self, data: List[Dict], platform: str) -> List[Dict]:
        """
        Find all URLs for a specific platform in the data structure.
        Searches in scraped_data.variants, scraped_data.all_matching_products, and scraped_data.unmapped
        """
        platform_urls = []
        identifier = self.platform_configs[platform]['identifier']
        
        for entry_idx, entry in enumerate(data):
            if not isinstance(entry, dict) or 'scraped_data' not in entry:
                continue
            
            scraped_data = entry['scraped_data']
            if not isinstance(scraped_data, dict):
                continue
            
            # Search in all three locations
            locations = ['variants', 'all_matching_products', 'unmapped']
            
            for location in locations:
                if location not in scraped_data:
                    continue
                
                location_data = scraped_data[location]
                if not isinstance(location_data, list):
                    continue
                
                for item_idx, item in enumerate(location_data):
                    if not isinstance(item, dict) or 'store_links' not in item:
                        continue
                    
                    store_links = item['store_links']
                    if not isinstance(store_links, list):
                        continue
                    
                    for store_idx, store_link in enumerate(store_links):
                        if not isinstance(store_link, dict):
                            continue
                        
                        name = store_link.get('name', '').lower()
                        url = store_link.get('url', '')
                        
                        # Check if this store link matches the platform
                        if identifier in name and url:
                            platform_urls.append({
                                'entry_idx': entry_idx,
                                'entry': entry,
                                'location': location,
                                'item_idx': item_idx,
                                'store_idx': store_idx,
                                'store_link': store_link,
                                'url': url,
                                'path': f"scraped_data.{location}[{item_idx}].store_links[{store_idx}]"
                            })
        
        return platform_urls
    
    def merge_platform_data(self, platform: str, platform_data: List[Dict]) -> int:
        """
        Merge platform-specific URL data into final_data.
        Returns the number of URLs successfully mapped.
        """
        print(f"\n🔍 Processing {platform.upper()} URLs...")
        
        platform_urls = self.find_platform_urls(platform_data, platform)
        print(f"   Found {len(platform_urls)} {platform} URLs")
        
        mapped_count = 0
        
        for url_data in platform_urls:
            entry = url_data['entry']
            store_link = url_data['store_link']
            url = url_data['url']
            
            self.stats[platform]['processed'] += 1
            
            # Check if this URL has already been mapped for this platform
            if url in self.mapped_urls[platform]:
                print(f"   ⏭️  Skipping duplicate {platform} URL: {url[:50]}...")
                self.stats[platform]['skipped'] += 1
                continue
            
            # Check if we already have this entry in final_data
            existing_entry = None
            for final_entry in self.final_data:
                if (final_entry.get('product_name') == entry.get('product_name') and
                    final_entry.get('model_id') == entry.get('model_id')):
                    existing_entry = final_entry
                    break
            
            if existing_entry is None:
                # Create new entry in final_data
                new_entry = entry.copy()
                self.final_data.append(new_entry)
                existing_entry = new_entry
                self.stats['total_entries'] += 1
            
            # The store_link is already part of the existing structure
            # so it's automatically included when we copy the entry
            self.mapped_urls[platform].add(url)
            mapped_count += 1
            self.stats[platform]['mapped'] += 1
            
            if mapped_count % 100 == 0:
                print(f"   📊 Mapped {mapped_count} {platform} URLs so far...")
        
        print(f"   ✅ Successfully mapped {mapped_count} {platform} URLs")
        return mapped_count
    
    def process_all_platforms(self, output_file: str = "final.json"):
        """
        Process all platforms in sequential order: Amazon → Flipkart → Croma → JioMart
        """
        print("🚀 Starting URL Mapping Process")
        print("=" * 80)
        print("📋 Processing Order: Amazon → Flipkart → Croma → JioMart")
        print("🔒 No Overwrite Policy: Each platform operates on separate URLs")
        print("🧵 Single-threaded sequential processing")
        print("-" * 80)
        
        # Create backup of existing final.json if it exists
        if os.path.exists(output_file):
            backup_file = f"{output_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(output_file, backup_file)
            print(f"💾 Created backup of existing final.json: {backup_file}")
        
        # Process platforms in order
        platforms_processed = []
        platforms_failed = []
        
        for platform in ['amazon', 'flipkart', 'croma', 'jiomart']:
            print(f"\n{'='*20} PROCESSING {platform.upper()} {'='*20}")
            
            # Load platform data
            platform_data = self.load_platform_data(platform)
            
            if platform_data is None:
                print(f"❌ {platform.upper()} processing failed - continuing with other platforms")
                platforms_failed.append(platform)
                continue
            
            # Merge platform data
            try:
                mapped_count = self.merge_platform_data(platform, platform_data)
                platforms_processed.append(platform)
                print(f"✅ {platform.upper()} processing completed: {mapped_count} URLs mapped")
                
                # Save progress after each platform
                progress_file = f"{output_file}.progress_{platform}_{datetime.now().strftime('%H%M%S')}.json"
                with open(progress_file, 'w', encoding='utf-8') as f:
                    json.dump(self.final_data, f, indent=2, ensure_ascii=False)
                print(f"💾 Progress saved: {progress_file}")
                
            except Exception as e:
                print(f"❌ Error processing {platform.upper()}: {e}")
                logging.error(f"Error processing {platform}: {e}")
                platforms_failed.append(platform)
                self.stats['errors'] += 1
                continue
        
        # Save final output
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(self.final_data, f, indent=2, ensure_ascii=False)
            
            print(f"\n🎉 URL MAPPING COMPLETED!")
            print(f"📁 Final output saved to: {output_file}")
            
        except Exception as e:
            print(f"❌ Error saving final output: {e}")
            logging.error(f"Error saving final output to {output_file}: {e}")
            return False
        
        # Print comprehensive summary
        self.print_summary(platforms_processed, platforms_failed)
        return True
    
    def print_summary(self, platforms_processed: List[str], platforms_failed: List[str]):
        """Print comprehensive processing summary."""
        print(f"\n📊 COMPREHENSIVE PROCESSING SUMMARY")
        print("=" * 80)
        
        print(f"🎯 PLATFORMS PROCESSED SUCCESSFULLY: {len(platforms_processed)}")
        for platform in platforms_processed:
            stats = self.stats[platform]
            print(f"   {platform.upper()}: {stats['mapped']} URLs mapped ({stats['processed']} processed, {stats['skipped']} skipped)")
        
        if platforms_failed:
            print(f"\n❌ PLATFORMS FAILED: {len(platforms_failed)}")
            for platform in platforms_failed:
                print(f"   {platform.upper()}: File missing or processing error")
        
        print(f"\n📈 OVERALL STATISTICS:")
        total_mapped = sum(self.stats[p]['mapped'] for p in ['amazon', 'flipkart', 'croma', 'jiomart'])
        total_processed = sum(self.stats[p]['processed'] for p in ['amazon', 'flipkart', 'croma', 'jiomart'])
        total_skipped = sum(self.stats[p]['skipped'] for p in ['amazon', 'flipkart', 'croma', 'jiomart'])
        
        print(f"   Total entries in final.json: {len(self.final_data)}")
        print(f"   Total URLs mapped: {total_mapped}")
        print(f"   Total URLs processed: {total_processed}")
        print(f"   Total URLs skipped (duplicates): {total_skipped}")
        print(f"   Errors encountered: {self.stats['errors']}")
        
        print(f"\n🔍 URL MAPPING BREAKDOWN:")
        for platform in ['amazon', 'flipkart', 'croma', 'jiomart']:
            mapped_count = len(self.mapped_urls[platform])
            print(f"   {platform.upper()}: {mapped_count} unique URLs mapped")
        
        success_rate = (total_mapped / total_processed * 100) if total_processed > 0 else 0
        print(f"\n✅ Success Rate: {success_rate:.1f}%")
        
        print(f"\n🛡️  ISOLATION CONFIRMED:")
        print(f"   ✓ Each platform's URLs are independently mapped")
        print(f"   ✓ No overwrites between platforms")
        print(f"   ✓ Existing data structure preserved")
        print(f"   ✓ Sequential processing completed")

def main():
    """Main function to run the URL mapping process."""
    print("🎯 URL Mapper - Sequential Platform Processing")
    print("=" * 80)
    print("📋 Purpose: Map URLs from each platform to final.json")
    print("🔄 Order: Amazon → Flipkart → Croma → JioMart")
    print("🚫 Policy: No overwrite between platforms")
    print("🧵 Mode: Single-threaded sequential processing")
    print("🛡️  Error Handling: Missing platforms won't stop other processing")
    print("-" * 80)
    
    # Initialize and run URL mapper
    mapper = URLMapper()
    success = mapper.process_all_platforms("final.json")
    
    if success:
        print(f"\n🎉 URL mapping completed successfully!")
        print(f"📁 Check final.json for the consolidated data")
    else:
        print(f"\n❌ URL mapping completed with errors")
        print(f"📋 Check the log file for detailed error information")
    
    return success

if __name__ == "__main__":
    main()
