"""
PDF Downloader for ECB Publications

This script reads ECB publication files and downloads/converts them to PDF format
with corresponding JSON metadata files.
"""

import os
import re
import json
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import aiohttp
from playwright.async_api import async_playwright
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log


class ECBPDFDownloader:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.browser = None
        self.downloads_dir = Path(__file__).parent / "downloads"
        self.downloads_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
        
        # Category mapping for JSON metadata
        self.category_mapping = {
            "press release": "Press release",
            "monetary policy": "Monetary policy statement", 
            "economic bulletin": "Economic Bulletin",
            "financial stability": "Financial Stability Review",
            "speech": "Speech",
            "interview": "Interview",
            "blog": "Blog",
            "report": "Report",
            "statistics": "Statistical release"
        }

    def sanitize_filename(self, title: str, max_length: int = 200) -> str:
        """Sanitize title for use as filename"""
        # Replace special characters with underscore
        special_chars = r'[;:\'"{}^%~#|<>\\[\]\s/]'
        clean_title = re.sub(special_chars, '_', title)
        
        # Remove multiple consecutive underscores
        clean_title = re.sub(r'_+', '_', clean_title)
        
        # Remove leading/trailing underscores
        clean_title = clean_title.strip('_')
        
        # Limit length
        if len(clean_title) > max_length:
            clean_title = clean_title[:max_length].rstrip('_')
        
        return clean_title

    def generate_filename(self, date: str, title: str, counter: int = 0) -> str:
        """Generate filename according to naming convention"""
        clean_title = self.sanitize_filename(title)
        
        if counter > 0:
            filename = f"{date}_{clean_title}_At{counter}"
        else:
            filename = f"{date}_{clean_title}"
        
        return filename

    def check_duplicate_filename(self, base_filename: str, extension: str) -> str:
        """Check for duplicate filenames and add counter if needed"""
        counter = 0
        filename = f"{base_filename}.{extension}"
        filepath = self.downloads_dir / filename
        
        while filepath.exists():
            counter += 1
            date_part, *title_parts = base_filename.split('_', 1)
            if title_parts:
                title_part = title_parts[0]
                # Remove existing _At suffix
                title_part = re.sub(r'_At\d+$', '', title_part)
                filename = f"{date_part}_{title_part}_At{counter}.{extension}"
            else:
                filename = f"{base_filename}_At{counter}.{extension}"
            filepath = self.downloads_dir / filename
        
        return filename

    def determine_category(self, title: str, url: str) -> str:
        """Determine publication category based on title and URL"""
        title_lower = title.lower()
        url_lower = url.lower()
        
        if "monetary policy" in title_lower or "/mopo/" in url_lower:
            return "Monetary policy statement"
        elif "economic bulletin" in title_lower:
            return "Economic Bulletin"
        elif "financial stability" in title_lower:
            return "Financial Stability Review"
        elif "speech" in url_lower or "key" in url_lower:
            return "Speech"
        elif "interview" in url_lower:
            return "Interview"
        elif "blog" in url_lower:
            return "Blog"
        elif "statistics" in title_lower or "/stats/" in url_lower:
            return "Statistical release"
        elif "press" in url_lower or "/pr/" in url_lower:
            return "Press release"
        else:
            return "Report"

    def generate_metadata(self, date: str, title: str, url: str, filename: str, creator: str = "") -> Dict:
        """Generate JSON metadata for the publication"""
        category = self.determine_category(title, url)
        
        # Parse date and add timezone
        try:
            pub_date = datetime.strptime(date, "%Y-%m-%d")
            pub_date = pub_date.replace(hour=9, minute=0, second=0)  # Default to 9 AM
            pub_date = pub_date.replace(tzinfo=timezone.utc)  # UTC timezone
        except:
            pub_date = datetime.now(timezone.utc)
        
        created_at = datetime.now(timezone.utc)
        
        metadata = {
            "dataset_name": "Central Bank EUR",
            "dataset_code": "CB_EUR_ECB",
            "source_uri": url,
            "created_at": created_at.isoformat(),
            "creator": creator,  # Creator name extracted from HTML or empty
            "publisher": "European Central Bank",
            "publication_date": pub_date.isoformat(),
            "publication_title": title,
            "ingest_source": "CB_EUR_ECB_LDR",
            "custom_attributes": {
                "category": category,
                "language": "English"
            },
            "raw_attributes": {}
        }
        
        return metadata

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.INFO),
        retry=retry_if_exception_type((Exception,))
    )
    async def init_browser(self):
        """Initialize browser for HTML to PDF conversion with retry logic"""
        print("Initializing browser...")
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=True)
        print("✓ Browser initialized successfully")

    async def init_session(self):
        """Initialize HTTP session for downloads"""
        import ssl
        
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Create connector with SSL context
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=300),  # 5 minute timeout
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=8),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.INFO),
        retry=retry_if_exception_type((aiohttp.ClientError, aiohttp.ServerTimeoutError, OSError))
    )
    async def download_pdf_with_aiohttp(self, url: str, filename: str) -> bool:
        """Download PDF using aiohttp with tenacity retry"""
        async with self.session.get(url, ssl=False) as response:
            if response.status == 200:
                filepath = self.downloads_dir / filename
                with open(filepath, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)
                return True
            else:
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status
                )

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=2, max=6),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.INFO),
        retry=retry_if_exception_type((Exception,))
    )
    def download_pdf_with_requests(self, url: str, filename: str) -> bool:
        """Fallback download using requests with tenacity retry"""
        import requests
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        response = requests.get(url, verify=False, timeout=300)
        if response.status_code == 200:
            filepath = self.downloads_dir / filename
            with open(filepath, 'wb') as f:
                f.write(response.content)
            return True
        else:
            raise requests.HTTPError(f"HTTP {response.status_code}")

    async def download_pdf(self, url: str, filename: str) -> bool:
        """Download PDF file directly with multiple retry strategies"""
        print(f"Downloading PDF: {filename}")
        
        try:
            # Primary method: aiohttp with tenacity retry
            await self.download_pdf_with_aiohttp(url, filename)
            print(f"✓ Downloaded: {filename}")
            return True
        except Exception as e:
            print(f"✗ aiohttp download failed: {e}")
            
            # Fallback method: requests with tenacity retry
            try:
                print(f"Trying fallback download method for {filename}...")
                self.download_pdf_with_requests(url, filename)
                print(f"✓ Downloaded via fallback method: {filename}")
                return True
            except Exception as fallback_error:
                print(f"✗ All download methods failed for {filename}: {fallback_error}")
                return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.INFO),
        retry=retry_if_exception_type((Exception,))
    )
    async def navigate_and_extract_creator(self, page, url: str) -> str:
        """Navigate to page and extract creator with retry logic"""
        # Navigate to page
        await page.goto(url, wait_until="networkidle", timeout=60000)
        
        # Wait for content to load
        await page.wait_for_timeout(3000)
        
        # Extract creator name from HTML
        creator_name = ""
        try:
            html_content = await page.content()
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Look for div.author-details > div.name
            author_details = soup.find('div', class_='author-details')
            if author_details:
                name_div = author_details.find('div', class_='name')
                if name_div:
                    creator_name = name_div.get_text(strip=True)
                    print(f"Found creator: {creator_name}")
            
            if not creator_name:
                print("No creator information found")
                
        except Exception as e:
            print(f"Warning: Could not extract creator info: {e}")
        
        return creator_name

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.INFO),
        retry=retry_if_exception_type((Exception,))
    )
    async def generate_pdf_from_page(self, page, filename: str):
        """Generate PDF from page with retry logic"""
        filepath = self.downloads_dir / filename
        await page.pdf(
            path=str(filepath),
            format='A4',
            print_background=True,
            margin={
                'top': '1in',
                'bottom': '1in',
                'left': '0.5in',
                'right': '0.5in'
            }
        )

    async def html_to_pdf(self, url: str, filename: str) -> Tuple[bool, str]:
        """Convert HTML page to PDF and extract creator name with retry logic"""
        try:
            print(f"Converting HTML to PDF: {filename}")
            
            page = await self.browser.new_page()
            
            # Navigate and extract creator with retry
            creator_name = await self.navigate_and_extract_creator(page, url)
            
            # Generate PDF with retry
            await self.generate_pdf_from_page(page, filename)
            
            await page.close()
            print(f"✓ Converted to PDF: {filename}")
            return True, creator_name
            
        except Exception as e:
            print(f"✗ Error converting {filename}: {e}")
            return False, ""

    def save_metadata(self, metadata: Dict, json_filename: str):
        """Save metadata to JSON file"""
        json_filepath = self.downloads_dir / json_filename
        with open(json_filepath, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        print(f"✓ Saved metadata: {json_filename}")

    def read_publications_file(self, filepath: str) -> List[Tuple[str, str, str]]:
        """Read publications from text file"""
        publications = []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and ' | ' in line:
                        parts = line.split(' | ', 2)
                        if len(parts) == 3:
                            date, title, url = parts
                            publications.append((date, title, url))
            
            print(f"Read {len(publications)} publications from {filepath}")
            return publications
            
        except Exception as e:
            print(f"Error reading file {filepath}: {e}")
            return []

    async def process_publication(self, date: str, title: str, url: str):
        """Process a single publication"""
        print(f"\nProcessing: {title}")
        print(f"URL: {url}")
        
        # Generate base filename
        base_filename = self.generate_filename(date, title)
        
        # Determine if PDF or HTML
        is_pdf = url.lower().endswith('.pdf')
        
        # Generate unique filename
        pdf_filename = self.check_duplicate_filename(base_filename, 'pdf')
        json_filename = self.check_duplicate_filename(base_filename, 'json')
        
        # Process the publication
        success = False
        creator_name = ""
        
        if is_pdf:
            success = await self.download_pdf(url, pdf_filename)
        else:
            success, creator_name = await self.html_to_pdf(url, pdf_filename)
        
        # Generate metadata if successful
        if success:
            metadata = self.generate_metadata(date, title, url, pdf_filename, creator_name)
            self.save_metadata(metadata, json_filename)
        
        return success

    async def process_publications_file(self, filepath: str):
        """Process all publications from a file"""
        publications = self.read_publications_file(filepath)
        
        if not publications:
            print("No publications found to process")
            return
        
        print(f"\nProcessing {len(publications)} publications...")
        print("=" * 60)
        
        successful = 0
        failed = 0
        
        for i, (date, title, url) in enumerate(publications, 1):
            print(f"\n[{i}/{len(publications)}]", end=" ")
            
            try:
                success = await self.process_publication(date, title, url)
                if success:
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"✗ Error processing publication: {e}")
                failed += 1
            
            # Small delay between requests
            await asyncio.sleep(1)
        
        print(f"\n" + "=" * 60)
        print(f"Processing complete!")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Files saved to: {self.downloads_dir}")

    async def close(self):
        """Close resources"""
        if self.session:
            await self.session.close()
        if self.browser:
            await self.browser.close()

    async def run(self, publications_file: str):
        """Main execution method"""
        try:
            await self.init_session()
            await self.init_browser()
            await self.process_publications_file(publications_file)
        finally:
            await self.close()


def find_latest_publications_file() -> Optional[str]:
    """Find the most recent ecb_publications_*.txt file"""
    current_dir = Path(__file__).parent
    pattern = "ecb_publications_*.txt"
    
    files = list(current_dir.glob(pattern))
    if not files:
        print(f"No files matching pattern '{pattern}' found")
        return None
    
    # Sort by modification time (newest first)
    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    return str(latest_file)


async def main():
    """Main function"""
    # Configure logging for tenacity
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("ECB PDF Downloader")
    print("=" * 40)
    
    # Find latest publications file
    publications_file = find_latest_publications_file()
    
    if not publications_file:
        print("Please ensure you have an ecb_publications_*.txt file in the current directory")
        return 1
    
    print(f"Using publications file: {Path(publications_file).name}")
    
    downloader = ECBPDFDownloader()
    await downloader.run(publications_file)
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
