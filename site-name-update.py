import aiohttp
import asyncio
import pandas as pd
import logging
import os
import json
import time
import random
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sdwan_site_rename.log'),
        logging.StreamHandler()
    ]
)


class RateLimiter:
    """Rate limiter to ensure we don't exceed SD-WAN vManage API limits"""
    
    def __init__(self, max_calls_per_minute: int = 80):
        """
        Initialize rate limiter
        
        Args:
            max_calls_per_minute: Maximum API calls per minute (default 80 for safety margin)
        """
        self.max_calls = max_calls_per_minute
        self.calls = []
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        """Wait if necessary to stay within rate limits"""
        async with self.lock:
            now = time.time()
            self.calls = [call_time for call_time in self.calls if now - call_time < 60]
            
            if len(self.calls) >= self.max_calls:
                sleep_time = 60 - (now - self.calls[0]) + 0.1
                logging.warning(f"Rate limit reached. Waiting {sleep_time:.1f} seconds...")
                await asyncio.sleep(sleep_time)
                now = time.time()
                self.calls = [call_time for call_time in self.calls if now - call_time < 60]
            
            self.calls.append(now)


class SDWANAsyncClient:
    def __init__(self, vmanage_url: str, username: str, password: str, max_concurrent: int = 1):
        """
        Initialize SD-WAN vManage async API client
        
        Args:
            vmanage_url: Base URL of vManage
            username: API username
            password: API password
            max_concurrent: Maximum number of concurrent requests (default: 1 to avoid deadlocks)
        """
        self.vmanage_url = vmanage_url.rstrip('/')
        self.username = username
        self.password = password
        self.token = None
        self.jsessionid = None
        self.session = None
        self.headers = {'Content-Type': 'application/json'}
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = RateLimiter(max_calls_per_minute=80)
        self.retry_attempts = 3
        self.base_retry_delay = 2
        
        if max_concurrent > 1:
            logging.warning(f"⚠️  MAX_CONCURRENT_REQUESTS set to {max_concurrent}")
            logging.warning("    SD-WAN may experience deadlocks with concurrent updates")
            logging.warning("    Recommend setting MAX_CONCURRENT_REQUESTS=1")
        
    async def authenticate(self) -> bool:
        """Authenticate to vManage and get session token"""
        try:
            connector = aiohttp.TCPConnector(ssl=False)
            cookie_jar = aiohttp.CookieJar(unsafe=True)
            
            self.session = aiohttp.ClientSession(
                connector=connector,
                cookie_jar=cookie_jar
            )
            
            login_url = f"{self.vmanage_url}/j_security_check"
            login_data = f"j_username={self.username}&j_password={self.password}"
            
            logging.info("Authenticating to vManage...")
            
            await self.rate_limiter.acquire()
            
            async with self.session.post(
                login_url,
                data=login_data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                
                if response.status not in [200, 302]:
                    logging.error(f"Authentication failed with status: {response.status}")
                    return False
            
            # Extract JSESSIONID
            cookies = self.session.cookie_jar.filter_cookies(self.vmanage_url)
            
            for cookie in cookies.values():
                if cookie.key == 'JSESSIONID':
                    self.jsessionid = cookie.value
                    break
            
            if not self.jsessionid:
                logging.error("No JSESSIONID cookie received")
                return False
            
            # Get XSRF token
            token_url = f"{self.vmanage_url}/dataservice/client/token"
            
            async with self.session.get(
                token_url,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as token_response:
                
                if token_response.status == 200:
                    self.token = await token_response.text()
                    self.token = self.token.strip()
                    
                    self.headers['X-XSRF-TOKEN'] = self.token
                    self.headers['Cookie'] = f'JSESSIONID={self.jsessionid}'
                    
                    logging.info("✓ Successfully authenticated to SD-WAN vManage")
                    return True
                else:
                    logging.error(f"Failed to get token: {token_response.status}")
                    return False
                
        except Exception as e:
            logging.error(f"✗ Authentication failed: {e}")
            return False
    
    async def get_all_sites(self, session: aiohttp.ClientSession) -> List[Dict]:
        """
        Get all sites from SD-WAN vManage network hierarchy
        
        Args:
            session: aiohttp ClientSession (can be None, will use self.session)
            
        Returns:
            List of site dictionaries with uuid, name, siteId, parentUuid, etc.
        """
        all_sites = []
        session_to_use = session if session else self.session
        
        try:
            url = f"{self.vmanage_url}/dataservice/v1/network-hierarchy"
            
            logging.info("Fetching network hierarchy from SD-WAN vManage...")
            
            await self.rate_limiter.acquire()
            
            async with session_to_use.get(
                url,
                headers=self.headers,
                ssl=False,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                
                content_type = response.headers.get('Content-Type', '')
                
                if 'text/html' in content_type:
                    logging.error("Received HTML instead of JSON - authentication may have expired")
                    return all_sites
                
                response.raise_for_status()
                data = await response.json()
                
                # Handle both list and dict responses
                if isinstance(data, list):
                    sites = data
                elif isinstance(data, dict) and 'data' in data:
                    sites = data['data']
                else:
                    logging.error(f"Unexpected response structure")
                    return all_sites
                
                logging.info(f"Found {len(sites)} total sites in network hierarchy")
                
                # Parse sites
                for site in sites:
                    uuid = site.get('uuid') or site.get('id', '')
                    name = site.get('name', '')
                    
                    # Get data object
                    site_data = site.get('data', {})
                    
                    # Extract fields from data object
                    hierarchy_id = site_data.get('hierarchyId', {})
                    site_id = hierarchy_id.get('siteId') if isinstance(hierarchy_id, dict) else None
                    parent_uuid = site_data.get('parentUuid', '')
                    label = site_data.get('label', 'SITE')
                    location = site_data.get('location', 'Undisclosed')
                    
                    description = site.get('description', 'Auto-Generated Site')
                    hierarchy_path = site.get('hierarchyPath', '')
                    
                    if uuid and name:
                        site_info = {
                            'uuid': uuid,
                            'name': name,
                            'siteId': site_id,
                            'parentUuid': parent_uuid,
                            'label': label,
                            'location': location,
                            'description': description,
                            'hierarchyId': hierarchy_id,
                            'hierarchyPath': hierarchy_path,
                            'directChildCount': site.get('directChildCount', 0)
                        }
                        all_sites.append(site_info)
                
                logging.info(f"Total sites extracted: {len(all_sites)}")
                
                if all_sites:
                    logging.info("Sample sites:")
                    for idx, site in enumerate(all_sites[:3]):
                        parent_info = f"Parent: {site['parentUuid'][:8]}..." if site['parentUuid'] else "Parent: None"
                        logging.info(f"  {idx+1}. {site['name']} (Site ID: {site['siteId']}, {parent_info})")
                
                return all_sites
                    
        except aiohttp.ClientError as e:
            logging.error(f"Error fetching network hierarchy: {e}")
            return all_sites
        except Exception as e:
            logging.error(f"Unexpected error fetching sites: {e}")
            return all_sites
    
    async def update_site_name(
        self, 
        session: aiohttp.ClientSession,
        site: Dict,
        new_name: str
    ) -> tuple:
        """
        Update site name in SD-WAN vManage network hierarchy
        
        Args:
            session: aiohttp ClientSession
            site: Site dictionary with all required fields
            new_name: New name for the site
            
        Returns:
            Tuple of (success: bool, uuid: str, current_name: str, new_name: str)
        """
        async with self.semaphore:
            
            session_to_use = session if session else self.session
            
            uuid = site['uuid']
            current_name = site['name']
            site_id = site['siteId']
            parent_uuid = site['parentUuid']
            label = site.get('label', 'SITE')
            location = site.get('location', 'Undisclosed')
            description = site.get('description', 'Auto-Generated Site')
            hierarchy_id = site.get('hierarchyId', {})
            
            url = f"{self.vmanage_url}/dataservice/v1/network-hierarchy/{uuid}"
            
            # Prepare payload
            payload = {
                "data": {
                    "hierarchyId": hierarchy_id if hierarchy_id else {"siteId": site_id},
                    "label": label,
                    "location": location
                },
                "description": description,
                "name": new_name
            }
            
            # Add parentUuid if it exists
            if parent_uuid:
                payload["data"]["parentUuid"] = parent_uuid
            
            logging.info(f"Updating site: {current_name} → {new_name}")
            logging.info(f"  Site ID: {site_id}")
            
            # Retry loop with exponential backoff
            for attempt in range(1, self.retry_attempts + 1):
                try:
                    await self.rate_limiter.acquire()
                    
                    async with session_to_use.put(
                        url,
                        headers=self.headers,
                        json=payload,
                        ssl=False,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        
                        response_text = await response.text()
                        
                        # Handle 429 Too Many Requests
                        if response.status == 429:
                            retry_after = int(response.headers.get('Retry-After', 60))
                            logging.warning(f"⚠️  Rate limited. Waiting {retry_after} seconds...")
                            await asyncio.sleep(retry_after)
                            continue
                        
                        # Handle 400 - check for deadlock
                        if response.status == 400:
                            try:
                                error_data = json.loads(response_text)
                                error_code = error_data.get('error', {}).get('code', '')
                                error_details = error_data.get('error', {}).get('details', '')
                                
                                # Database deadlock error
                                if 'NWHAR0003' in error_code or 'acquire UpdateLock' in error_details:
                                    if attempt < self.retry_attempts:
                                        backoff_delay = self.base_retry_delay * (2 ** (attempt - 1)) + random.uniform(0, 3)
                                        logging.warning(f"⚠️  Database deadlock. Retrying in {backoff_delay:.1f}s...")
                                        await asyncio.sleep(backoff_delay)
                                        continue
                                
                                logging.error(f"✗ API Error: {error_data.get('error', {}).get('message', 'Unknown')}")
                                if attempt >= self.retry_attempts:
                                    return (False, uuid, current_name, new_name)
                            except:
                                logging.error(f"✗ HTTP 400: {response_text[:500]}")
                                if attempt >= self.retry_attempts:
                                    return (False, uuid, current_name, new_name)
                            
                            if attempt < self.retry_attempts:
                                backoff_delay = self.base_retry_delay * (2 ** (attempt - 1))
                                await asyncio.sleep(backoff_delay)
                                continue
                        
                        if response.status not in [200, 202, 204]:
                            if attempt < self.retry_attempts:
                                backoff_delay = self.base_retry_delay * (2 ** (attempt - 1))
                                logging.warning(f"⚠️  HTTP {response.status}. Retrying in {backoff_delay}s...")
                                await asyncio.sleep(backoff_delay)
                                continue
                            else:
                                logging.error(f"✗ Failed to update site {uuid} ({current_name})")
                                return (False, uuid, current_name, new_name)
                        
                        response.raise_for_status()
                        logging.info(f"✓ Successfully updated site")
                        
                        await asyncio.sleep(1)
                        return (True, uuid, current_name, new_name)
                        
                except aiohttp.ClientError as e:
                    if attempt < self.retry_attempts:
                        backoff_delay = self.base_retry_delay * (2 ** (attempt - 1))
                        logging.warning(f"⚠️  Error on attempt {attempt}. Retrying in {backoff_delay}s...")
                        await asyncio.sleep(backoff_delay)
                    else:
                        logging.error(f"✗ Failed after {self.retry_attempts} attempts: {e}")
                        return (False, uuid, current_name, new_name)
                except Exception as e:
                    logging.error(f"✗ Unexpected error: {e}")
                    return (False, uuid, current_name, new_name)
            
            return (False, uuid, current_name, new_name)
    
    async def test_single_site_update(
        self, 
        session: aiohttp.ClientSession,
        site: Dict,
        new_name: str
    ) -> bool:
        """Test updating a single site"""
        logging.info("\n" + "="*80)
        logging.info("TESTING SINGLE SITE UPDATE")
        logging.info("="*80)
        
        uuid = site['uuid']
        current_name = site['name']
        site_id = site['siteId']
        
        logging.info(f"Test Site: {current_name}")
        logging.info(f"  UUID: {uuid}")
        logging.info(f"  Site ID: {site_id}")
        logging.info(f"  New Name: {new_name}")
        
        success, _, _, _ = await self.update_site_name(session, site, new_name)
        
        if success:
            logging.info("✓ Test update successful!")
        else:
            logging.error("✗ Test update failed!")
        
        logging.info("="*80 + "\n")
        return success
    
    async def update_sites_batch(
        self, 
        sites_to_update: List[Dict], 
        name_mapping: Dict[str, str]
    ) -> Dict:
        """Update multiple sites with rate limiting"""
        results = {
            'successful_updates': 0,
            'failed_updates': 0,
            'total_processed': 0
        }
        
        tasks = []
        
        for site in sites_to_update:
            current_name = site['name']
            new_name = name_mapping.get(current_name)
            
            if new_name and new_name != current_name:
                task = self.update_site_name(None, site, new_name)
                tasks.append(task)
        
        if not tasks:
            logging.warning("No sites to update")
            return results
        
        estimated_time = len(tasks) / (80 / 60) if self.max_concurrent > 1 else len(tasks) * 2 / 60
        logging.info(f"\nStarting update of {len(tasks)} sites")
        logging.info(f"  Concurrency: {self.max_concurrent}")
        logging.info(f"  Estimated time: {estimated_time:.1f} minutes")
        
        completed = 0
        for coro in asyncio.as_completed(tasks):
            success, uuid, current_name, new_name = await coro
            completed += 1
            
            if success:
                results['successful_updates'] += 1
            else:
                results['failed_updates'] += 1
            
            results['total_processed'] = completed
            
            if completed % 10 == 0 or completed == len(tasks):
                logging.info(f"Progress: {completed}/{len(tasks)} sites processed")
        
        return results
    
    async def close(self):
        """Close the persistent session"""
        if self.session:
            await self.session.close()


def load_mapping_spreadsheet(file_path: str) -> Dict[str, str]:
    """Load spreadsheet with current and target names"""
    try:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
        
        if df.shape[1] < 2:
            logging.error("Spreadsheet must have at least 2 columns")
            return {}
        
        df = df.iloc[:, :2]
        df.columns = ['current_name', 'target_name']
        df = df.dropna()
        
        mapping = dict(zip(df['current_name'].str.strip(), df['target_name'].str.strip()))
        
        logging.info(f"Loaded {len(mapping)} name mappings from spreadsheet")
        
        if mapping:
            logging.info("Sample mappings:")
            for idx, (curr, target) in enumerate(list(mapping.items())[:3]):
                logging.info(f"  {idx+1}. '{curr}' → '{target}'")
        
        return mapping
        
    except Exception as e:
        logging.error(f"Error loading spreadsheet: {e}")
        return {}


def get_credentials_from_env() -> tuple:
    """Retrieve credentials from environment variables"""
    load_dotenv()
    
    vmanage_url = os.getenv('VMANAGE_URL')
    username = os.getenv('VMANAGE_USERNAME')
    password = os.getenv('VMANAGE_PASSWORD')
    max_concurrent = int(os.getenv('MAX_CONCURRENT_REQUESTS', '1'))
    
    if not all([vmanage_url, username, password]):
        missing = []
        if not vmanage_url:
            missing.append('VMANAGE_URL')
        if not username:
            missing.append('VMANAGE_USERNAME')
        if not password:
            missing.append('VMANAGE_PASSWORD')
        
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    logging.info(f"Configuration loaded:")
    logging.info(f"  URL: {vmanage_url}")
    logging.info(f"  Max concurrent requests: {max_concurrent}")
    
    return vmanage_url, username, password, max_concurrent


async def main_async():
    """Main async execution function"""
    
    results = {
        'total_sites': 0,
        'sites_to_update': 0,
        'successful_updates': 0,
        'failed_updates': 0,
        'skipped': 0
    }
    
    client = None
    
    try:
        logging.info("="*80)
        logging.info("SD-WAN vManage Site Rename Script")
        logging.info("="*80)
        
        logging.info("\nLoading configuration from environment variables...")
        VMANAGE_URL, USERNAME, PASSWORD, MAX_CONCURRENT = get_credentials_from_env()
        
        SPREADSHEET_PATH = os.getenv('SPREADSHEET_PATH', 'site_mapping.xlsx')
        logging.info(f"  Spreadsheet path: {SPREADSHEET_PATH}")
        
        TEST_MODE = os.getenv('TEST_MODE', 'false').lower() == 'true'
        if TEST_MODE:
            logging.info("  ⚠️  TEST MODE ENABLED - Will only update first matching site")
        
    except ValueError as e:
        logging.error(f"Configuration error: {e}")
        return
    except Exception as e:
        logging.error(f"Unexpected error loading configuration: {e}")
        return
    
    # Load name mapping
    logging.info("\nLoading name mapping from spreadsheet...")
    name_mapping = load_mapping_spreadsheet(SPREADSHEET_PATH)
    
    if not name_mapping:
        logging.error("No valid mappings found in spreadsheet. Exiting.")
        return
    
    # Initialize client
    logging.info("\nInitializing SD-WAN vManage client...")
    client = SDWANAsyncClient(
        VMANAGE_URL, 
        USERNAME, 
        PASSWORD,
        max_concurrent=MAX_CONCURRENT
    )
    
    # Authenticate
    logging.info("\nAuthenticating to SD-WAN vManage...")
    if not await client.authenticate():
        logging.error("Failed to authenticate. Exiting.")
        if client:
            await client.close()
        return
    
    # Get all sites
    logging.info("\nFetching all sites from network hierarchy...")
    all_sites = await client.get_all_sites(None)
    
    results['total_sites'] = len(all_sites)
    
    if not all_sites:
        logging.error("No sites retrieved. Exiting.")
        await client.close()
        return
    
    logging.info(f"\nFound {results['total_sites']} total sites")
    
    # Find Global UUID for sites without parentUuid
    global_uuid = None
    for site in all_sites:
        if site['name'] == 'Global':
            global_uuid = site['uuid']
            logging.info(f"Found Global UUID: {global_uuid}")
            break
    
    # Filter sites to update
    sites_to_update = []
    
    for site in all_sites:
        current_name = site['name']
        
        # Skip Global
        if current_name == 'Global':
            continue
        
        if current_name in name_mapping:
            target_name = name_mapping[current_name]
            
            if current_name == target_name:
                logging.debug(f"Site '{current_name}' unchanged, skipping")
                results['skipped'] += 1
                continue
            
            if not site.get('uuid'):
                logging.warning(f"No UUID for site '{current_name}', skipping")
                results['failed_updates'] += 1
                continue
            
            # Handle missing parentUuid
            if not site.get('parentUuid'):
                if global_uuid:
                    logging.info(f"Site '{current_name}' has no parentUuid, using Global UUID")
                    site['parentUuid'] = global_uuid
                else:
                    logging.warning(f"No parentUuid for site '{current_name}' and Global not found, skipping")
                    results['failed_updates'] += 1
                    continue
            
            sites_to_update.append(site)
            results['sites_to_update'] += 1
        else:
            results['skipped'] += 1
    
    if not sites_to_update:
        logging.info("\nNo sites need to be updated.")
        if results['sites_to_update'] == 0:
            logging.info("None of the sites match names in your spreadsheet.")
            logging.info("\nSites found in vManage:")
            non_global = [s for s in all_sites if s['name'] != 'Global']
            for idx, site in enumerate(non_global[:10]):
                logging.info(f"  {idx+1}. {site['name']}")
            if len(non_global) > 10:
                logging.info(f"  ... and {len(non_global) - 10} more")
        await client.close()
        return
    
    logging.info(f"\n{len(sites_to_update)} sites will be updated")
    
    # TEST MODE
    if TEST_MODE:
        test_site = sites_to_update[0]
        test_new_name = name_mapping[test_site['name']]
        
        success = await client.test_single_site_update(None, test_site, test_new_name)
        
        if success:
            logging.info("\n✓ Test successful! Set TEST_MODE=false to update all sites.")
        else:
            logging.error("\n✗ Test failed! Review errors above.")
        
        await client.close()
        return
    
    # Update sites
    logging.info("\n" + "="*80)
    logging.info("Starting site updates...")
    logging.info("="*80)
    
    update_results = await client.update_sites_batch(sites_to_update, name_mapping)
    
    results['successful_updates'] = update_results['successful_updates']
    results['failed_updates'] += update_results['failed_updates']
    
    # Summary
    logging.info("\n" + "="*80)
    logging.info("EXECUTION SUMMARY")
    logging.info("="*80)
    logging.info(f"Total sites retrieved:    {results['total_sites']}")
    logging.info(f"Sites in mapping:         {results['sites_to_update']}")
    logging.info(f"Successful updates:       {results['successful_updates']}")
    logging.info(f"Failed updates:           {results['failed_updates']}")
    logging.info(f"Sites skipped:            {results['skipped']}")
    logging.info("="*80)
    
    if results['failed_updates'] > 0:
        logging.warning("\n⚠️  Some updates failed. Check the log file for details.")
    
    if results['successful_updates'] > 0:
        logging.info("\n✓ Site rename operation completed successfully!")
    
    await client.close()


def main():
    """Wrapper to run async main function"""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()