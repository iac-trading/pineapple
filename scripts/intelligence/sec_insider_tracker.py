import os
import json
import asyncio
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from nats.aio.client import Client as NATS

# Configuration
SEC_RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count=100&output=atom"
USER_AGENT = "Axio-Quant Trading Platform (contact: trader@axio-quant.com)"
NATS_URL = os.environ["NATS_URL"]
MIN_PURCHASE_VALUE = 50000.0 # $50k threshold for interesting trades

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SEC-INSIDER] %(message)s")
logger = logging.getLogger("SECInsiderTracker")

class SECInsiderTracker:
    def __init__(self):
        self.nc = NATS()
        self.instance_id = os.getenv("INSTANCE_ID", "sec-insider-001")

    async def start(self):
        await self.nc.connect(servers=[NATS_URL])
        logger.info(f"Connected to NATS at {NATS_URL}")
        
        # Discovery listener
        await self.nc.subscribe("factory.control.ping", cb=self.on_ping)

    async def on_ping(self, msg):
        status = {
            "instance_id": self.instance_id,
            "strategy": "BP-72",
            "symbol": "SEC-FEED",
            "status": "RUNNING",
            "ts": datetime.now().isoformat()
        }
        await self.nc.publish(msg.reply, json.dumps(status).encode())
        
    async def process_latest_filings(self):
        """Main loop/task to fetch and process latest Form 4 filings."""
        logger.info("Fetching latest Form 4 filings from SEC...")
        try:
            headers = {"User-Agent": USER_AGENT}
            response = requests.get(SEC_RSS_URL, headers=headers)
            if response.status_code != 200:
                logger.error(f"Failed to fetch SEC RSS: {response.status_code}")
                return

            root = ET.fromstring(response.content)
            # Namespace for Atom
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            
            entries = root.findall('atom:entry', ns)
            logger.info(f"Found {len(entries)} recent filings.")

            for entry in entries:
                title = entry.find('atom:title', ns).text
                link = entry.find('atom:link', ns).attrib['href']
                
                # Link is to the landing page, we need the XML link
                # e.g. https://www.sec.gov/Archives/edgar/data/12345/00012345-24-000001-index.htm
                # We need the directory index to find the .xml file
                xml_url = await self.find_xml_url(link)
                if xml_url:
                    await self.parse_form4_xml(xml_url)

        except Exception as e:
            logger.exception(f"Error processing filings: {e}")

    async def find_xml_url(self, landing_url):
        """Convert landing page URL to the main XML filing URL."""
        # Landing: .../00012345-24-000001-index.htm
        # XML: .../00012345-24-000001.txt (Old way) or a specific XML file
        # Most recent ones have a clear XML file link in the directory
        base_dir = landing_url.replace("-index.htm", "")
        # The directory index looks like: https://www.sec.gov/Archives/edgar/data/12345/0001234524000001/
        # But a safer way is to just append .xml to the accession number if it's the primary doc
        # Actually, the RSS often has the .txt link which contains the XML inside.
        # Or we can scrape the landing page for the .xml file.
        
        try:
            headers = {"User-Agent": USER_AGENT}
            resp = requests.get(landing_url, headers=headers)
            # Find the first .xml file in the table (usually doc1.xml or similar)
            # This is a bit brittle, but standard for SEC scrapers
            if ".xml" in resp.text:
                # Very simple heuristic: find the first .xml link in the Archives directory
                import re
                match = re.search(r'href="(/Archives/edgar/data/[^"]+\.xml)"', resp.text)
                if match:
                    return "https://www.sec.gov" + match.group(1)
        except Exception:
            pass
        return None

    async def parse_form4_xml(self, xml_url):
        """Fetch and parse a Form 4 XML filing."""
        try:
            headers = {"User-Agent": USER_AGENT}
            resp = requests.get(xml_url, headers=headers)
            root = ET.fromstring(resp.content)
            
            issuer = root.find('issuer')
            ticker = issuer.find('issuerSymbol').text
            
            owner = root.find('reportingOwner')
            owner_name = owner.find('reportingOwnerId/rptOwnerName').text
            relationship = owner.find('reportingOwnerRelationship')
            is_ceo = relationship.find('isDirector') is not None # Simplified
            
            # Transactions
            transactions = root.findall('.//nonDerivativeTransaction')
            for tx in transactions:
                coding = tx.find('transactionCoding/transactionCode').text
                if coding == 'P': # Purchase
                    shares = float(tx.find('transactionAmounts/transactionShares/value').text)
                    price = float(tx.find('transactionAmounts/transactionPricePerShare/value').text)
                    value = shares * price
                    
                    if value >= MIN_PURCHASE_VALUE:
                        logger.info(f"🎯 INSIDER PURCHASE: {owner_name} bought ${value:,.2f} of {ticker}")
                        await self.publish_signal(ticker, owner_name, value, price, shares, xml_url)
                        
        except Exception:
            pass # Skip malformed or irrelevant filings

    async def publish_signal(self, ticker, owner, value, price, shares, url):
        payload = {
            "strategy": "SEC-72",
            "symbol": ticker,
            "insider": owner,
            "value_usd": value,
            "price": price,
            "shares": shares,
            "source_url": url,
            "ts": datetime.now().isoformat()
        }
        await self.nc.publish("intelligence.insider", json.dumps(payload).encode())

if __name__ == "__main__":
    tracker = SECInsiderTracker()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tracker.start())
    loop.run_until_complete(tracker.process_latest_filings())
