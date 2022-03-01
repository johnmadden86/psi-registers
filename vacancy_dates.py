import json
from scrape import today
from datetime import datetime, timedelta
last_scrape_date = today
with open(f'data/pharmacy-data-{last_scrape_date}.json') as file:
    pharmacies = json.load(file)
with open(f'No SV.json') as file:
    foi_no_supervising = json.load(file)

print(len(pharmacies))
no_supervising = [pharmacy for pharmacy in pharmacies if pharmacy['Supervising Pharmacist'] is None]
for foi_pharmacy in foi_no_supervising:
    date_removed = foi_pharmacy['Date removed']
    try:
        pharmacy = next(p for p in pharmacies if foi_pharmacy['Reg'] == p['PSI Registration Number'])
    except StopIteration:
        pharmacy = None
        print(foi_pharmacy , 1)
    try:
        vacant_since = pharmacy.get('Supervising Pharmacist Vacant Since', None)
    except AttributeError:
        vacant_since = None
        print(foi_pharmacy, 0)


    if vacant_since is not None and vacant_since != date_removed:
        vacant_since_TD = datetime.strptime(vacant_since, '%Y-%m-%d')
        date_removed_TD = datetime.strptime(date_removed, '%Y-%m-%d')
        if vacant_since_TD == date_removed_TD:
            print(pharmacy['Name'], vacant_since, date_removed, (vacant_since_TD - date_removed_TD) / timedelta(days=1))
            pharmacy['Superintendent Pharmacist Vacant Since'] = date_removed
# with open(f'data/pharmacy-data-{last_scrape_date}.json', 'w') as json_file:
#     json.dump(pharmacies, json_file, indent=2)
