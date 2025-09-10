"""
ECB Publications Scraper

Simplified scraper using Playwright + BeautifulSoup to extract publication links
from the ECB press releases page, focusing only on dl-wrapper div content.
"""

import asyncio
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
import logging


class ECBScraper:
    def __init__(self, max_publications: int = 100):
        self.publications: List[Dict[str, str]] = []
        self.max_publications = max_publications
        self.logger = logging.getLogger(__name__)
        self.month_names = {
            'January': '01', 'February': '02', 'March': '03', 'April': '04',
            'May': '05', 'June': '06', 'July': '07', 'August': '08',
            'September': '09', 'October': '10', 'November': '11', 'December': '12'
        }

    def parse_date(self, date_text: str) -> Optional[str]:
        """Parse date text to YYYY-MM-DD format"""
        if not date_text:
            return None
        
        date_text = date_text.strip()
        
        # Pattern 1: DD Month YYYY
        match = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', date_text)
        if match:
            day, month, year = match.groups()
            month_num = self.month_names.get(month, None)
            if month_num:
                return f"{year}-{month_num}-{day.zfill(2)}"
        
        # Pattern 2: YYYY-MM-DD (already correct format)
        match = re.search(r'(\d{4})-(\d{2})-(\d{2})', date_text)
        if match:
            return match.group(0)
        
        print(f"Warning: Could not parse date: '{date_text}'")
        return None

    def extract_publications_from_html(self, html: str) -> List[Dict[str, str]]:
        """Extract publications from HTML using BeautifulSoup"""
        soup = BeautifulSoup(html, 'lxml')
        publications = []
        
        # Find the dl-wrapper div
        dl_wrapper = soup.find('div', class_='dl-wrapper')
        if not dl_wrapper:
            print("No dl-wrapper div found in HTML")
            return publications
        
        print(f"Found dl-wrapper div")
        
        # Find all dl elements within dl-wrapper
        dl_elements = dl_wrapper.find_all('dl')
        print(f"Found {len(dl_elements)} dl elements in dl-wrapper")
        
        for dl in dl_elements:
            # Check if we've reached the limit
            if len(publications) >= self.max_publications:
                print(f"Reached maximum limit of {self.max_publications} publications")
                break
                
            # Get dt and dd elements
            dt_elements = dl.find_all('dt')
            dd_elements = dl.find_all('dd')
            
            # Process each dt/dd pair
            for i, dt in enumerate(dt_elements):
                # Check limit again within the inner loop
                if len(publications) >= self.max_publications:
                    break
                    
                date_text = dt.get_text(strip=True)
                
                # Find corresponding dd element
                if i < len(dd_elements):
                    dd = dd_elements[i]
                    
                    
                    
                    # Find all links in this dd
                    links = dd.find_all('a', href=True)
                    
                    for link in links:
                        # Check limit for each link
                        if len(publications) >= self.max_publications:
                            break
                            
                        title = link.get_text(strip=True)
                        # Skip if link is inside a div with class "accordion"
                        accordion_parent = link.find_parent('div', class_='accordion')
                        if accordion_parent:
                            print("Skipping dl element inside accordion div")
                            continue
                        url = link['href']
                        
                        # Make URL absolute if it's relative
                        if url.startswith('/'):
                            url = 'https://www.ecb.europa.eu' + url
                        
                        if title and url:
                            parsed_date = self.parse_date(date_text)
                            if parsed_date:
                                publications.append({
                                    'date': parsed_date,
                                    'title': title.replace('|', '-'),  # Remove pipes
                                    'url': url,
                                    'original_date_text': date_text
                                })
                                print(f"Found ({len(publications)}/{self.max_publications}): {parsed_date} - {title[:50]}...")
        
        print(f"Extracted {len(publications)} publications from dl-wrapper (limit: {self.max_publications})")
        return publications

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.INFO),
        retry=retry_if_exception_type((Exception,))
    )
    async def launch_browser(self, playwright):
        """Launch browser with tenacity retry logic"""
        print("Launching browser...")
        return await playwright.chromium.launch(headless=True)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.INFO),
        retry=retry_if_exception_type((Exception,))
    )
    async def navigate_to_page(self, page, url):
        """Navigate to page with tenacity retry logic"""
        print(f"Navigating to: {url}")
        return await page.goto(url, wait_until="networkidle", timeout=60000)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.INFO),
        retry=retry_if_exception_type((Exception,))
    )
    async def wait_for_dl_wrapper(self, page):
        """Wait for dl-wrapper element with tenacity retry logic"""
        print("Waiting for dl-wrapper to appear...")
        result = await page.wait_for_selector('.dl-wrapper', timeout=30000)
        print("dl-wrapper found")
        return result

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.INFO),
        retry=retry_if_exception_type((Exception,))
    )
    async def get_page_height(self, page):
        """Get page height with retry logic"""
        return await page.evaluate("document.body.scrollHeight")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.INFO),
        retry=retry_if_exception_type((Exception,))
    )
    async def scroll_to_bottom(self, page):
        """Scroll to bottom with retry logic"""
        return await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=8),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.INFO),
        retry=retry_if_exception_type((Exception,))
    )
    async def get_page_content(self, page):
        """Get page content with retry logic"""
        return await page.content()

    async def scrape_with_scroll(self) -> str:
        """Scrape the page with scroll handling to load more content"""
        url = "https://www.ecb.europa.eu/press/pubbydate/html/index.en.html?name_of_publication=Press%20release"
        
        async with async_playwright() as p:
            # Launch browser with tenacity retry
            browser = await self.launch_browser(p)
            page = await browser.new_page()
            
            # Block images and other resources for faster loading
            await page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "font", "media"] else route.continue_())
            
            # Navigate to page with tenacity retry
            await self.navigate_to_page(page, url)
            
            # Wait for the content to load with tenacity retry
            try:
                await self.wait_for_dl_wrapper(page)
            except Exception as e:
                print(f"Warning: Could not find dl-wrapper after retries: {e}")
                print("Proceeding anyway...")
            
            # Additional wait for dynamic content
            await page.wait_for_timeout(5000)
            
            # Scroll to load more content and parse after every 5 successful scrolls
            print(f"Scrolling to load more content (target: {self.max_publications} publications)...")
            previous_height = 0
            failed_scroll_attempts = 0
            successful_scrolls = 0
            max_failed_scrolls = 10
            publications_found = 0
            
            while failed_scroll_attempts < max_failed_scrolls and publications_found < self.max_publications:
                try:
                    # Get current page height with tenacity retry
                    current_height = await self.get_page_height(page)
                    
                    if current_height == previous_height:
                        failed_scroll_attempts += 1
                        print(f"No new content loaded (attempt {failed_scroll_attempts}/{max_failed_scrolls})")
                    else:
                        failed_scroll_attempts = 0
                        successful_scrolls += 1
                        print(f"Successful scroll #{successful_scrolls} - Page height: {current_height}")
                    
                    # Scroll to bottom with tenacity retry
                    await self.scroll_to_bottom(page)
                    await page.wait_for_timeout(2000)
                    
                    # Parse content after every 5 successful scrolls
                    if successful_scrolls > 0 and successful_scrolls % 5 == 0:
                        print(f"\n--- Parsing content after {successful_scrolls} successful scrolls ---")
                        try:
                            current_html = await self.get_page_content(page)
                            temp_publications = self.extract_publications_from_html(current_html)
                            publications_found = len(temp_publications)
                            
                            print(f"Found {publications_found} publications so far")
                            
                            if publications_found >= self.max_publications:
                                print(f"Reached target of {self.max_publications} publications, stopping scroll")
                                break
                            
                            print("Continuing to scroll for more content...\n")
                        except Exception as e:
                            print(f"Warning: Could not parse content during scroll: {e}")
                            print("Continuing scroll anyway...")
                    
                    previous_height = current_height
                    
                except Exception as e:
                    print(f"Error during scroll operation: {e}")
                    failed_scroll_attempts += 1
                    if failed_scroll_attempts >= max_failed_scrolls:
                        break
            
            if failed_scroll_attempts >= max_failed_scrolls:
                print(f"Stopped scrolling after {max_failed_scrolls} failed attempts")
            
            print("Final extraction of HTML content...")
            
            # Get the full page HTML with tenacity retry
            html_content = await self.get_page_content(page)
            
            await browser.close()
            return html_content

    async def run(self) -> str:
        """Main scraper execution"""
        print("ECB Publications Scraper")
        print("=" * 40)
        
        try:
            # Get HTML content with scrolling
            html_content = await self.scrape_with_scroll()
            
            # Extract publications using BeautifulSoup
            self.publications = self.extract_publications_from_html(html_content)
            
            # Remove duplicates and sort
            unique_pubs = []
            seen_urls = set()
            
            for pub in self.publications:
                if pub['url'] not in seen_urls:
                    unique_pubs.append(pub)
                    seen_urls.add(pub['url'])
            
            # Sort by date (newest first)
            unique_pubs.sort(key=lambda x: x['date'], reverse=True)
            
            self.publications = unique_pubs
            
            print(f"\nFound {len(self.publications)} unique publications")
            
            # Save to file
            filename = self.save_to_file()
            return filename
            
        except Exception as e:
            print(f"Error during scraping: {e}")
            raise

    def save_to_file(self) -> str:
        """Save publications to file"""
        if not self.publications:
            print("No publications to save")
            return ""
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ecb_publications_{timestamp}.txt"
        filepath = Path(__file__).parent / filename
        
        # Format: YYYY-MM-dd | publication name | publication link
        lines = []
        for pub in self.publications:
            line = f"{pub['date']} | {pub['title']} | {pub['url']}"
            lines.append(line)
        
        content = '\n'.join(lines)
        
        # Save to file
        filepath.write_text(content, encoding='utf-8')
        
        print(f"\nSaved {len(self.publications)} publications to {filename}")
        
        # Show preview
        print("\nPreview (first 5 entries):")
        for i, pub in enumerate(self.publications[:5]):
            print(f"{i+1}. {pub['date']} | {pub['title'][:60]}...")
        
        return str(filepath)


async def main():
    """Main function"""
    # Configure logging for tenacity
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    scraper = ECBScraper(max_publications=100)  # Limit to 100 publications
    try:
        filepath = await scraper.run()
        print(f"\nScraping completed successfully!")
        print(f"Results saved to: {Path(filepath).name}")
        print(f"Publications collected: {len(scraper.publications)} (max: {scraper.max_publications})")
    except Exception as e:
        print(f"Scraping failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
