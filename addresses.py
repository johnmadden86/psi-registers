import json
counties = [
    'Carlow',
    'Cavan',
    'Clare',
    'Cork',
    'Donegal',
    'Dublin',
    'Galway',
    'Kerry',
    'Kildare',
    'Kilkenny',
    'Laois',
    'Leitrim',
    'Limerick',
    'Longford',
    'Louth',
    'Mayo',
    'Meath',
    'Monaghan',
    'Offaly',
    'Roscommon',
    'Sligo',
    'Tipperary',
    'Waterford',
    'Westmeath',
    'Wexford',
    'Wicklow',
    'Antrim',
    'Armagh',
    'Down',
    'Derry',
    'Fermanagh',
    'Tyrone',
    'Ireland'
]
dublin = ['Dublin ' + str(i) for i in range(1, 25)]
dublin.append('Dublin 6W')
counties = ['Co. ' + c for c in counties]
counties += dublin
last_scrape_date_with_addresses = '2022-01-12'
with open(f'data/pharmacist-data-{last_scrape_date_with_addresses}.json') as file:
    data = json.load(file)

data = [d for d in data if d['County'] is None or d['County'] not in counties]
for n, d in enumerate(data):
    print(f"{n}. {d['Name']}, {d['Address']}")
    # print(d['County'], type(d['County']))
exit()

data = [d for d in data if all(c not in d['Address'] for c in counties)]

for n, d in enumerate(data):
    print(f"{n}. {d['Name']}, {d['Address']}")
