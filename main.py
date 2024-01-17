import httpx
import asyncio
import logging
import pandas as pd
import sys
import os


RELAYS = {
    "flashbots": "https://0xac6e77dfe25ecd6110b8e780608cce0dab71fdd5ebea22a16c0205200f2f8e2e3ad3b71d3499c54ad14d6c21b41a37ae@boost-relay.flashbots.net",
    "bloxroute max profit": "https://0x8b5d2e73e2a3a55c6c87b8b6eb92e0149a125c852751db1422fa951e42a09b82c142c3ea98d0d9930b056a3bc9896b8f@bloxroute.max-profit.blxrbdn.com",
    "bloxroute regulated": "https://0xb0b07cd0abef743db4260b0ed50619cf6ad4d82064cb4fbec9d3ec530f7c5e6793d9f286c4e082c0244ffb9f2658fe88@bloxroute.regulated.blxrbdn.com",
    "ultrasound": "https://0xa1559ace749633b997cb3fdacffb890aeebdb0f5a3b6aaa7eeeaf1a38af0a8fe88b9e4b1f61f236d2e64d95733327a62@relay.ultrasound.money",
    "agnostic": "https://0xa7ab7a996c8584251c8f925da3170bdfd6ebc75d50f5ddc4050a6fdc77f2a3b5fce2cc750d0865e05d7228af97d69561@agnostic-relay.net",
    "aestus": "https://0xa15b52576bcbf1072f4a011c0f99f9fb6c66f3e1ff321f11f461d15e31b1cb359caa092c71bbded0bae5b5ea401aab7e@aestus.live"
}

for lib in ['httpcore', 'httpx']:
    lib_logger = logging.getLogger(lib)
    lib_logger.setLevel(logging.ERROR)

logging.basicConfig(level=logging.INFO)


def save_to_parquet(bids_data, filename):
    if not os.path.exists("outputs"):
        os.makedirs("outputs")

    filename = f"outputs/{filename}.parquet"

    df = pd.DataFrame(bids_data)

    df.to_parquet(filename)
    logging.info(f"Saved {len(bids_data)} records to {filename}")

async def gather_with_concurrency(n, *coros):
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro
    return await asyncio.gather(*(sem_coro(c) for c in coros))


async def get_relay_bids_for_slot(relay_url, block_number, r_name, max_retries=3, retry_delay=5):
    relay_name = r_name
    for attempt in range(max_retries):
        async with httpx.AsyncClient() as client:
            url = f"{relay_url}/relay/v1/data/bidtraces/builder_blocks_received?block_number={block_number}"
            try:
                response = await client.get(url)
                response.raise_for_status()
                if response.status_code == 200:
                    if len(response.json()) > 0:
                        resp = response.json()
                        for resp_bid in resp:
                            resp_bid['relay_name'] = relay_name
                        return resp
                    else:
                        return None
            except httpx.HTTPError as e:
                # Handle HTTP errors here (e.g., log or raise a custom exception)
                logging.error(f"HTTP error occurred for URL: {url}, Error: {e}")
                await asyncio.sleep(retry_delay)
            except Exception as e:
                # Handle other exceptions (e.g., network issues)
                logging.error(f"An error occurred for URL: {url}, Error: {e}")
                await asyncio.sleep(retry_delay)

    return None


async def get_block_metadata(block_number):
    bids, tasks = [], []

    for key, value in RELAYS.items():
        tasks.append(get_relay_bids_for_slot(value, block_number, key))
    results = await gather_with_concurrency(10, *tasks)

    for result in results:
        if result:
            bids.extend(result)

    logging.debug(f"Got {len(bids)} relay bids for {block_number} block")

    if bids:
        save_to_parquet(bids, block_number)
        return bids
    else:
        logging.info(f"No relay bids for block {block_number}")
        return None


async def get_bids(from_block, to_block):
    try:
        list_block_numbers = range(from_block, to_block)

        logging.info(f"Requesting relay bids metadata from {RELAYS.keys()} for blocks {list_block_numbers}")
        tasks = [get_block_metadata(block_data) for block_data in list_block_numbers]
        await gather_with_concurrency(CONCURRENT_REQ, *tasks)

    except Exception as e:
        logging.error(f'Issue fetching relay bids {str(e)}')
        return None


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python script.py from_block to_block")
        sys.exit(1)

    from_block = int(sys.argv[1])
    to_block = int(sys.argv[2])

    CONCURRENT_REQ = 100

    asyncio.run(get_bids(from_block, to_block))
