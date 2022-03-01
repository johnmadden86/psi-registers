from datetime import datetime, timedelta
import json
from scrape import today, assistants, pharmacies, pharmacists
import csv
from collections import Counter

from tweepy import API, OAuthHandler, TweepError

API_KEY = 'jQLQN938HSv853j4dsZ3OQswC'
API_SECRET = 'Q5u6tMfmyUNdSntSBEumjE98i5UloFGdzG6fBSeLjisnsMRni3'
ACCESS_TOKEN = '1151253184058396672-t05qMJuabZc0seVVLKCPerzWNlAMz6'
ACCESS_TOKEN_SECRET = 'ZZYoT5Es0wkGcsdoiX7OZKaPDQw1HldrawXAA22VTktD1'


def create_api_connection_and_authenticate():
    try:
        auth = OAuthHandler(API_KEY, API_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        api.verify_credentials()
        print("Authentication OK")
    except TweepError:
        print("Error during authentication")
    else:
        return api


def create_thread_with_intro(api, tweets, intro):
    intro_tweet = api.update_status(intro)
    tweet_id = intro_tweet.id
    for tweet in tweets:
        new_tweet = api.update_status(tweet, tweet_id)
        tweet_id = new_tweet.id


last_scrape_date = today
last_scrape_date_with_addresses = '2022-01-12'
# second_last_scrape_date = '2021-09-13'

import googlemaps
import os
api_key = os.environ.get('GMAPS_API_KEY')
gmaps = googlemaps.Client(key=api_key)

with open('gms-contracts.json') as file:
    gms_contracts = json.load(file)
superintendents = [pharmacy['Superintendent Pharmacist'] for pharmacy in pharmacies]
# print(Counter(superintendents).most_common())
# for pharmacy in pharmacies[0:1]:
#     search_term = f"{pharmacy['Name']}, {pharmacy['Address']}"
#     geolocation = gmaps.geocode(search_term)
#     print(geolocation, type(geolocation))
# exit()
# with open(f'data/pharmacy-data-{last_scrape_date}.json', 'w') as json_file:
#     json.dump(pharmacies, json_file, indent=2)
# exit()

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


# for pharmacist in pharmacists + assistants:
#     if all(county not in pharmacist['Address'] for county in counties):
#         print(f'{pharmacist["Name"]}, {pharmacist["Address"]}')
# exit()


#
#
# gms_contracts = list(map(lambda p: int(p['GMS number']) < 90000, gms_contracts))
# gms_contracts = list(filter(lambda p: int(p['GMS number']) < 90000, gms_contracts))
# print(len(gms_contracts))
# gms_contracts = set(map(lambda p: int(p['GMS number']), gms_contracts))
# print(len(gms_contracts))
# gms_contracts = []
# with open('gms-contracts.csv', 'w', newline='') as file:
#     dict_writer = csv.DictWriter(file, gms_contracts[0].keys())
#     dict_writer.writeheader()
#     dict_writer.writerows(gms_contracts)
# with open('gms-contracts.json', 'w') as json_file:
#     json.dump(gms_contracts, json_file, indent=2)


# dates = list(map(lambda x: f"{x['Date Registered'][-4:-1]}0s", pharmacists))
# for date in sorted(set(dates)):
#     print(date, dates.count(date))
# exit()

# def remove_middle_name(n):
#     try:
#         first, *middle, last = n.split()
#         return f"{first} {last}"
#     except AttributeError:
#         return None


# def save_all():
    # with open(f'data/pharmacy-data-{last_scrape_date}.json', 'w') as json_file:
    #     json.dump(pharmacies, json_file, indent=2)

    # with open(f'data/assistant-data-{last_scrape_date}.json', 'w') as json_file:
    #     json.dump(assistants, json_file, indent=2)


# pharmacies = sorted(pharmacies, key=lambda k: k['Opening Hours']['Weekly Hours'], reverse=True)
# for pharmacy in pharmacies:
#     print(f"{pharmacy['Name']}, {pharmacy['Address']}. ({pharmacy['Opening Hours']['Weekly Hours']})")
# exit()


def find_pharmacy(pharmacies_, town, day, open_=None, closed_=None):
    pharmacies_ = filter(lambda x:
                         town in x['Address'] and
                         (open_ == x['Opening Hours'][day]['Open'] or not open_) and
                         (closed_ == x['Opening Hours'][day]['Closed'] or not closed_),
                         pharmacies_)
    for pharmacy_ in pharmacies_:
        print(f"{pharmacy_['Name']} {pharmacy_['Address']}")


community_pharmacies = [p['Hospital'] == p['Other'] for p in pharmacies]
# hospital_pharmacies = [p['Hospital'] is True for p in pharmacies]
# other_pharmacies = [p['Other'] is True for p in pharmacies]
# print(len(community_pharmacies), len(hospital_pharmacies), len(other_pharmacies), len(pharmacies))
# exit()
number_of_community_pharmacies = len(pharmacies)
no_supervising = list(filter(lambda p: p['Supervising Pharmacist'] is None, pharmacies))
no_superintendent = list(filter(lambda p: p['Superintendent Pharmacist'] is None, pharmacies))
no_superintendent_or_supervising = list(filter(lambda p: p['Superintendent Pharmacist'] is None
                                                         and p['Supervising Pharmacist'] is None, pharmacies))
# no_supervising.sort(key=lambda x: 'zzz' if x['Superintendent Pharmacist'] is None else x['Superintendent Pharmacist'])
no_supervising.sort(key=lambda x: x['Supervising Pharmacist Vacant Since'])
no_superintendent.sort(key=lambda x: x['Superintendent Pharmacist Vacant Since'])
# no_supervising.sort(key=lambda x: x['Registration Number'])
# low_hours = list(filter(lambda p: 30 <= p['Opening Hours']['Weekly Hours'] < 40, pharmacies))
number_of_community_pharmacies_without_supervising_pharmacist = len(no_supervising)
ratio = round(100 * number_of_community_pharmacies_without_supervising_pharmacist / number_of_community_pharmacies, 2)

first_tweet = f"No supervising pharmacist at {number_of_community_pharmacies_without_supervising_pharmacist}" \
              f" of Ireland's {number_of_community_pharmacies} pharmacies ({ratio}%)"
print(first_tweet)

thread = []
for num, pharmacy in enumerate(no_supervising, start=1):
    reg_no = pharmacy['PSI Registration Number']
    name = pharmacy['Name']
    address = pharmacy['Address']
    owner = pharmacy['Pharmacy Owner']
    superintendent = pharmacy.get('Superintendent Pharmacist')
    supervisor = pharmacy.get('Supervising Pharmacist')
    address_of_owner = pharmacy.get('Address of Owner', None)
    weekly_hours = pharmacy['Opening Hours']['Weekly Hours']
    supervising_vacant_since = pharmacy.get('Supervising Pharmacist Vacant Since')
    superintendent_vacant_since = pharmacy.get('Superintendent Pharmacist Vacant Since')
    try:
        # date = pharmacy['No Supervising Pharmacist Since']
        # days = (datetime.today() - datetime.strptime(pharmacy['No Supervising Pharmacist Since'], '%Y-%m-%d')).days

        # tweet = f"{name}, {address}. ({owner}) - Superintendent: {superintendent}"
        # tweet = f"{reg_no}: {name}, {address}."
        # tweet = f"{reg_no}: {name}, {address}. ({owner}) [{days} day{'s' if days != 1 else ''}] - Superintendent: {superintendent}"
        tweet = f"{reg_no}: {name}, {address}. ({supervising_vacant_since})"  # [{superintendent}]"
    except KeyError:
        tweet = f"{reg_no}: {name}, {address}. ({owner}) - Superintendent: {superintendent}"
    # if days > 30:
    thread.append(tweet)
    # tweet = f"{reg_no}: {name}, {address}. ({weekly_hours})"
    # tweet = f"{reg_no}: {name}, {address}. ({reg_no in dates})"
    # print(tweet)

# thread.sort()
for n, t in enumerate(thread, start=1):
    print(t)

hours = map(lambda x: x['Opening Hours']['Weekly Hours'], pharmacies)


# hours = sorted(Counter(hours).most_common())
# print(sum(hours))


def more_than_three_years(pharmacist):
    three_years = timedelta(days=365 * 3)
    date_registered = datetime.strptime(pharmacist['Date Registered'], '%d/%m/%Y')
    date_today = datetime.strptime(today, '%Y-%m-%d')
    return date_today - date_registered > three_years


print(f'{len(pharmacists)} pharmacists and {len(assistants)} assistants for {len(pharmacies)} pharmacies')
print(f'{round(len(pharmacists) / len(pharmacies), 3)} pharmacists per pharmacy')
print(f'{round(100 * len(list(filter(lambda x: more_than_three_years(x), pharmacists))) / len(pharmacists), 2)}%'
      f' registered for 3 years or longer')
# exit()

# find_pharmacy(pharmacies, "Kilkenny", "Tuesday", closed_="23:00")
# find_pharmacy(pharmacies, "Carlow", "Tuesday", closed_="23:00")
# find_pharmacy(pharmacies, "Waterford", "Tuesday", closed_="21:00")
# find_pharmacy(pharmacies, "Waterford", "Tuesday", closed_="22:00")
# find_pharmacy(pharmacies, "Laois", "Tuesday", closed_="23:00")
# find_pharmacy(pharmacies, "Tipperary", "Tuesday", closed_="23:00")
# find_pharmacy(pharmacies, "Kildare", "Tuesday", closed_="23:00")
# find_pharmacy(pharmacies, "Wexford", "Tuesday", closed_="23:00")
# find_pharmacy(pharmacies, "Portlaoise", "Tuesday", "09:00", "19:00")
# find_pharmacy(pharmacies, "Stradbally", "Friday")
# find_pharmacy(pharmacies, "Cork", "Sunday", open_="10:00", closed_="17:00")

# pharmacists.sort(key=lambda x: (datetime.strptime(x['Date Registered'], '%d/%m/%Y'), x['Registration Number']))
# save_all()

# n = 1
print('##############')

print(f"No superintendent pharmacist at {len(no_superintendent)}" \
      f" of Ireland's {number_of_community_pharmacies}  pharmacies")
for pharmacy in no_superintendent:
    print(f"{pharmacy['PSI Registration Number']}: {pharmacy['Name']}, {pharmacy['Address']}"
          f" ({pharmacy['Superintendent Pharmacist Vacant Since']})")

print(f"No superintendent or supervising pharmacist at {len(no_superintendent_or_supervising)}" \
      f" of Ireland's {number_of_community_pharmacies}  pharmacies")
for pharmacy in no_superintendent_or_supervising:
    print(f"{pharmacy['PSI Registration Number']}: {pharmacy['Name']}, {pharmacy['Address']}")
# print('##############')
# for pharmacy in pharmacies:
#     print(f"{pharmacy['Pharmacy Owner']}#{pharmacy['Pharmacy Address']}#{pharmacy['Registration Number']}")
#

exit()
