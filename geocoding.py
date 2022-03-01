import asyncio
import json

import aiohttp

from scrape import today

GMAPS_API_KEY = 'AIzaSyCBkoFFqIi_OkBRnxLK-61x_il92D8hILc'
API_KEY = 'e83b1443a30f31f933faf938040fd156'
map_quest = {
    'Consumer Key': 'TeCojqP2Qu7t7Znu1cnImGg1sw3GcA67',
    'Consumer Secret': 'x0q2Be0bIvvjK6qA'
}


# noinspection HttpUrlsUsage
BASE_URL = 'http://api.positionstack.com/v1/forward'
map_quest_base_url = 'http://www.mapquestapi.com/geocoding/v1/address'

last_scrape_date = today
with open(f'data/pharmacy-data-{last_scrape_date}.json') as file:
    pharmacies = json.load(file)
with open(f'data/pharmacy-data-with-locations-2022-02-03.json') as file:
    pharmacies_with_locations = json.load(file)

pharmacies_without_locations = [pharmacy for pharmacy in pharmacies_with_locations if 'Location' not in pharmacy.keys()]
for p in pharmacies_with_locations:
    print(p['Location'])
ireland_bounding_box = f'-9.97708574059,51.6693012559,-6.03298539878,55.1316222195'
print(len(pharmacies_with_locations))
print(len(pharmacies_without_locations))
print(len(pharmacies))
exit()

async def fetch(session, pharmacy):
    query = f'{pharmacy["Name"]}, {pharmacy["Address"]}'
    params = {
        'key': map_quest['Consumer Key'],
        'location': query,
        'boundingBox': ireland_bounding_box
    }
    reply = await session.get(map_quest_base_url, params=params)
    json_ = await reply.json()
    data = json_['results'][0]['locations'][0]
    try:
        # r = next(result for result in data if result['adminArea1'] == 'IE')
        location = data['latLng']
        pharmacy['Location'] = location
        pharmacies_with_locations.append(pharmacy)
    except StopIteration:
        print(data[0]['adminArea1'], data[0]['adminArea3'], data[0]['adminArea4'])


async def loop_manager():
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.ensure_future(fetch(session, pharmacy)) for pharmacy in pharmacies]
        await asyncio.gather(*tasks)


def run():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(loop_manager())
    print(len(pharmacies_with_locations))
    with open(f'data/pharmacy-data-with-locations-{last_scrape_date}.json', 'w') as json_file:
        json.dump(pharmacies_with_locations, json_file, indent=2)


if __name__ == '__main__':
    run()
