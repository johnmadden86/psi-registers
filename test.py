from scrape_old import assistant, get_pharm_ids, run
import aiohttp
import asyncio
import itertools

async def main():
    ids_, last_pg = await run(assistant)
    print(ids_)
    tasks = [asyncio.ensure_future(run(assistant, page=i, last_pg=last_pg)) for i in range(1, last_pg)]
    ids_2 = itertools.chain(*(await asyncio.gather(*tasks)))
    print(ids_2)
    ids_.extend(ids_2)
    print(ids_)
asyncio.run(main())
exit()
