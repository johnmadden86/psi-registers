import json
import csv
from collections import Counter
from scrape import today
from openpyxl import Workbook

last_scrape_date = today

workbook = Workbook()

with open(f'data/pharmacy-data-{last_scrape_date}.json') as file:
    pharmacies = json.load(file)
with open(f'data/pharmacist-data-{last_scrape_date}.json') as file:
    pharmacists = json.load(file)
names = [pharmacy['Name'] for pharmacy in pharmacies]
print(Counter(names).most_common())
allcare = [name for name in names if 'Allcare' in name]
for a in allcare:
    print(a)
exit()
chains = []
for k in ('Companies Office Registration No', 'Pharmacy Owner', 'Superintendent Pharmacist'):
    x = [ph[k] for ph in pharmacies]
    for match, count in Counter(x).most_common():
        chain = [ph for ph in pharmacies if ph[k] == match]
        dup = 1
        if k == 'Superintendent Pharmacist':
            dup = len([ph for ph in pharmacists if ph['Name'] == match])
        if match is not None and count > 1 and dup == 1:
            worksheet = workbook.create_sheet(str(match))
            for c in chain:
                chains.append(c)
                pharmacies.remove(c)
                del c['Opening Hours']
                worksheet.append(tuple(c.values()))
                print(f"{c['Name']}, {c['Address']}")
print(len(pharmacies), len(chains), len(chains) + len(pharmacies))
for n, pharmacy in enumerate(pharmacies, start=1):
    print(n, f"{pharmacy['Name']}, {pharmacy['Address']}")
print(len(pharmacies), len(chains), len(chains) + len(pharmacies))
workbook.save('chains.xlsx')
exit()
chains = set()
independents = set()
for k in ('Companies Office Registration No', 'Pharmacy Owner', 'Superintendent Pharmacist', 'Web'):
    x = [ph[k] for ph in pharmacies]
    print(x)
    for match, count in Counter(x).most_common():
        print(match, count)
        exit()
        if match is not None and count > 1:
            chain = [ph for ph in pharmacies if ph[k] == match]
            if k == 'Superintendent Pharmacist':
                dup = len([ph for ph in pharmacists if ph['Name'] == match])
                if dup == 0:
                    for p in chain:
                        chains.add(f"{p['PSI Registration Number']}: {p['Name']}, {p['Address']}")
                    print(k)
                    print(match)
                    print([f"{p['PSI Registration Number']}: {p['Name']}" for p in chain])
                    print(len(chain))
            else:
                for p in chain:
                    chains.add(f"{p['PSI Registration Number']}: {p['Name']}, {p['Address']}")
                print(k)
                print(match)
                print([f"{p['PSI Registration Number']}: {p['Name']}" for p in chain])
                print(len(chain))
        else:
            independent = [ph for ph in pharmacies if ph[k] == match]
            for ph in independent:
                independents.add(f"{ph['PSI Registration Number']}: {ph['Name']}, {ph['Address']}")
for c in chains:
    print(c)
print(len(chains))
for c in independents:
    print(c)
print(len(independents))
exit()
"""
Allcare Uniphar Cormac Loughnane
Lloyd's Hilton Health Express
Boots
Hickey's
McCauley
McCabe's
Stack's
Pure
Mulligan's
Cara
O'Sullivan's (Limerick)
McCartan
Adrian Dunne
Chemco
Phelan's
Cadden/Grogan
Murphy/Kilgallen
Meagher's
Your Local
Healthwise
McSharry
O'Sullivan's (Cork)
Kissane's
Dan McCarthy
Mari Mina
McCormack's
McGreal's
Tesco
Molloy's
Mangan/Kelly
Matt O'Flaherty
Ryan's (Offaly)
Gilmartin (Louth)
Rochford (Clare)
O'Regan (Dublin)
McElwee (Donegal/Laois)
Morgan (Dublin)
Horgan (Cork)
Smith (Dublin)
Denis Walsh (Galway)
First Plus (Cork)
Keane (Westmeath)
McNally (Meath/Cavan)

"""