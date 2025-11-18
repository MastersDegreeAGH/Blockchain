import requests
import time
import json
from datetime import datetime

# --- Configuration ---

START_ADDRESSES = [
    '13AM4VW2dhxYgXeQepoHkHSQuy6NgaEb94',
    '12t9YDPgwueZ9NyMgw519p7AA8isjr6SMw',
    '115p7UMMngoj1pMvkpHijcRdfJNXj6LrLn'
]

MIN_AMOUNT = 0.2  # BTC
MAX_DEPTH = 10
MAX_OUTGOING = 50  # Skip crawling deeper if more than this number of outgoing addresses
MAX_TRANSACTIONS = 100
DATE_FROM = datetime(2017, 8, 3)
DATE_TO = datetime(2017, 8, 10)
SAVE_EVERY = 10  # Save results every N transactions

# --- Storage for results ---
results = []
visited_transactions = set()

# --- Helper functions ---
def timestamp_in_range(ts):
    dt = datetime.utcfromtimestamp(ts)
    return DATE_FROM <= dt <= DATE_TO

def get_address_info(address):
    url = f'https://www.walletexplorer.com/api/1/address?address={address}&from=0&count=1000'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None

def get_address_label(address):
    url = f'https://www.walletexplorer.com/api/1/address-lookup?address={address}'
    response = requests.get(url)
    if response.status_code != 200:
        return None
    result = response.json()
    if 'label' not in result:
        return None
    else:
        return result['label']

def get_tx_info(txid):
    url = f'https://www.walletexplorer.com/api/1/tx?txid={txid}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None

def save_results():
    with open('btc_crawl_final.json', 'w') as f:
        json.dump(results, f, indent=4)
    print(f"Saved {len(results)} transactions so far.")

def crawl_address(address, depth=0):
    if depth > MAX_DEPTH:
        return
    
    data = get_address_info(address)
    label = get_address_label(address)
    if not data or not data.get('found'):
        return
    
    
    txs = data.get('txs', [])
    
    # Stop crawling if too many transactions (unless starting)
    if len(txs) > MAX_TRANSACTIONS and depth > 0:
        print(f"Skipping address {address} (too many transactions: {len(txs)}), possibly an exchange")
        return

    
    for tx in txs:
        txid = tx['txid']

        if txid in visited_transactions:
            print(f"Skipping transaction {txid} (already parsed)")
            continue

        visited_transactions.add(txid)

        # Check outgoing transactions (amount_sent > 0) and date range
        if tx['amount_sent'] - tx['amount_received'] < MIN_AMOUNT:
            continue
        if not timestamp_in_range(tx['time']):
            continue
        
        tx_info = get_tx_info(txid)
        if not tx_info or not tx_info.get('found'):
            continue

        outgoing_addresses = [out['address'] for out in tx_info.get('out', [])]

        # Save transaction info
        partial_result = {
            'from_address': address,
            'txid': txid,
            'amount_sent': tx['amount_sent'],
            'time': tx['time'],
            'outgoing_addresses': outgoing_addresses
        }

        if label != None:
            partial_result['label'] = label

        results.append(partial_result)

        # Save periodically
        if len(results) % SAVE_EVERY == 0:
            save_results()

        print(f"Crawled {address} -> {txid} (depth {depth})")

        # Skip deeper crawling if too many outgoing addresses
        if len(outgoing_addresses) > MAX_OUTGOING:
            print(f"Skipping deeper crawl for {txid} (too many outgoing addresses: {len(outgoing_addresses)})")
            continue

        # Recursively crawl outgoing addresses, unless label (exchange) found
        if label != None:
            print(f"Transaction from exchange: {label}, stopping crawl")
            return


        for out_address in outgoing_addresses:
            crawl_address(out_address, depth + 1)

# --- Start crawling ---
for addr in START_ADDRESSES:
    crawl_address(addr)

# --- Save final results ---
save_results()
print(f"Crawl finished. {len(results)} transactions saved.")
