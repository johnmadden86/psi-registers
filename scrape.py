import asyncio
import json
import math
import time
from datetime import date, datetime, timedelta
from itertools import chain

import bs4
from aiohttp import ClientSession, ClientPayloadError
from colorama import Fore
from tqdm import tqdm

BASE_URL = 'https://registrations.thepsi.ie/search-register/'
PARSER = 'lxml'

today = str(date.today())
start_global = time.perf_counter()

semaphore_pause = 1000


class DataType:
    def __init__(self, type_, json_):
        self.type = type_
        self.json = f'{json_}-data'

    def __str__(self):
        return self.json[:-5]


assistant = DataType(2, 'assistant')
pharmacist = DataType(1, 'pharmacist')
pharmacy = DataType(0, 'pharmacy')


def load_last_scrape(data_type):
    """
    Load data from last scrape
    :param data_type: assistant, pharmacist or pharmacy
    :return: data from last scrape
    """
    data = None
    i = 1
    while not data:
        last_scrape_date = str(date.today() - timedelta(days=i))
        try:
            with open(f'data/{data_type.json}-{last_scrape_date}.json') as file:
                data = json.load(file)
        except FileNotFoundError:
            i += 1
    return data


def pages_plus_two_percent(data_list):
    """
    Add 2% to number of pages of register on last scrape (to allow for an increase)
    :param data_list: data from last scrape
    :return: pages + 2% (as int)
    """
    return int((math.ceil(len(data_list)) / 9) * 1.02) + 1


assistants = load_last_scrape(assistant)
assistant.pages = pages_plus_two_percent(assistants)
pharmacists = load_last_scrape(pharmacist)
pharmacist.pages = pages_plus_two_percent(pharmacists)
pharmacies = load_last_scrape(pharmacy)
pharmacy.pages = pages_plus_two_percent(pharmacies)


async def fetch_register_page(sem, session, data_type, page):
    """
    Controls GET requests sent
    :param sem: Semaphore value, to avoid overloading the website
    :param session: current asyncio client session
    :param data_type: assistant, pharmacist or pharmacy
    :param page: page of register to retrieve
    :return: html reply
    """
    async with sem:
        params = {'type': data_type, 'page': page}
        reply = await session.get(BASE_URL, params=params)
        html = await reply.text()
        return html


def get_page(soup):
    """
    Returns page number
    :param soup: html converted to bs4 soup
    :return: current page, last page (as integers)
    """
    p = soup.select('.pagination-description')[0]
    p = p.string.split()
    return int(p[1]), int(p[-1])


def get_address(soup, s77r):
    """
    Extract address from html (incl. comments)
    :param soup: html converted to bs4 soup
    :param s77r: section 77 registration (people only)
    :return: cleaned up address string + bool (people) or None (pharmacy)
    """
    bs_comment = soup.find(text=lambda text: isinstance(text, bs4.Comment))
    try:  # case pharmacist, assistant (pre 12/1/22) {big privacy leak!}
        parsed_soup = bs4.BeautifulSoup(bs_comment, PARSER)
        address = parsed_soup.small.string
    except TypeError:  # case pharmacy
        address = soup.small.string
        s77r = None

    address = address.replace('\r\n', ', ')  # remove new line white space
    address = address.replace(' ,', ',')  # remove space before comma
    address = ' '.join(address.split())  # no double space
    if len(address) == 0:
        address = None
    return address, s77r


def is_hospital_pharmacy(pharmacy_name):
    """
    Check if a pharmacy is in a hospital
    :param pharmacy_name: the pharmacy's name
    :return: True if the pharmacy is in a hospital, otherwise False
    """
    if 'allcare' in pharmacy_name.lower():  # Hospital, Co. Limerick
        return False
    hospital_terms = ['h.s.e.', 'department', 'clinic', 'hospice', 'hospital']
    return any(term for term in hospital_terms if term in pharmacy_name.lower())


def is_other_pharmacy(pharmacy_name):
    """
    Check if a pharmacy is a dedicated veterinary or other pharmacy
    :param pharmacy_name: the pharmacy's name
    :return: True if the pharmacy is in an 'other' pharmacy, otherwise False
    """
    other_terms = ['animal', 'stock health', 'tcp', 'baxter healthcare', 'avec']
    return any(term for term in other_terms if term in pharmacy_name.lower())


def value_converter(key_value_list):
    """
    Map strings to an integer, True, False, or None if applicable
    :param key_value_list: input key-value pair in list form
    :return: integer value, True, False, None, or unaltered string
    """
    try:  # case integer
        return int(key_value_list[1])
    except TypeError:  # case string
        return key_value_list[1]
    except IndexError:  # case None
        return None
    except ValueError:  # case bool or None
        y = ['YES', 'Y']
        n = ['NO', 'N']
        none = ['N/A']
        upper = key_value_list[1].upper()
        if upper in y:
            return True
        elif upper in n:
            return False
        elif upper in none:
            return None
        return key_value_list[1]


def write_to_json(dump_data, file_name):
    """
    Write retrieved data to a json file
    :param file_name: name of json file
    :param dump_data: a list of the data objects retrieved
    """
    with open(file_name, 'w') as output_file:
        json.dump(dump_data, output_file, indent=2)


def html_to_soup(html):
    """
    Convert html to bs4 soup
    :param html: html as returned from GET request
    :return: html converted to bs4 soup
    """
    return bs4.BeautifulSoup(html, PARSER)


def section77(name):
    """
    Check for special section 77 registrations (covid-19 emergency)
    :param name: Pharmacist's name as retrieved from register
    :return: Pharmacist's name + a bool value
    """
    s77r = False
    if 'section' in name.lower():
        name = name.split('-')
        name = name[0]
        s77r = True
        if 'section' in name.lower():
            name = name[:len('SECTION 77')]
    return name.rstrip(), s77r


def get_opening_hours(value_list):
    """
    Extract a pharmacy's opening hours from the retrieved data
    :param value_list: subsection of html reply (as bs4 soup)
    :return: opening hours (as dict)
    """
    opening_hours = {}
    for v in value_list:
        [day, times] = v.split(': ')
        times = times.split(', ')
        lunch = len(times) > 1
        if not lunch:
            times = times[0].split(' to ')
            open_ = times[0]
            try:
                closed = times[1]
            except IndexError:
                open_ = None
                closed = None
            opening_hours[day] = {
                'Open': open_,
                'Closed': closed
            }
        else:
            times1 = times[0].split(' to ')
            times2 = times[1].split(' to ')
            try:
                open_, lunch1 = times1
                lunch2, closed = times2
                opening_hours[day] = {
                    'Open': open_,
                    'Lunch Closure': lunch1,
                    'Lunch Reopen': lunch2,
                    'Closed': closed
                }
            except IndexError:
                opening_hours[day] = {
                    'Open': None,
                    'Closed': None
                }
    weekly_hours = 0
    for day in opening_hours:
        hours_open = 0
        try:
            open_ = datetime.strptime(opening_hours[day]['Open'], '%H:%M')
            closed = datetime.strptime(opening_hours[day]['Closed'], '%H:%M')
            if open_ > closed > datetime(1900, 1, 1) or closed == datetime(1900, 1, 1, 10):
                closed += timedelta(hours=12)
            hours = (closed - open_) / timedelta(hours=1)
            hours %= 24
            # days_ = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
            if day != 'Bank Holidays':
                hours_open += hours
            try:
                lunch1 = datetime.strptime(opening_hours[day]['Lunch Closure'], '%H:%M')
                lunch2 = datetime.strptime(opening_hours[day]['Lunch Reopen'], '%H:%M')
                hours = (lunch2 - lunch1) / timedelta(hours=1)
                if hours > 0:  # ignore incorrect (negative) lunch hours
                    hours %= 24
                    hours_open -= hours
            except KeyError:  # no lunch
                pass
        except TypeError:  # no entry (closed)
            pass
        opening_hours[day]['Hours Open'] = hours_open
        weekly_hours += hours_open
    opening_hours['Weekly Hours'] = weekly_hours
    return opening_hours


def data_from_soup(soup):
    """
    Extract useful info from html converted to bs4 soup
    :param soup: input html soup
    :return: list of data retrieved (9 items for full page)
    """
    items = soup.find_all('div', 'search-register-result-item')
    data_object_list = []
    for item in items:
        data_object = {}
        avatar_content = item.find('div', 'avatar-content')
        name = avatar_content.h5.string
        name, s77r = section77(name)
        address, s77r = get_address(avatar_content, s77r)
        data_object['Name'] = ' '.join(name.split())
        if address:
            data_object['Address'] = address
        # noinspection SpellCheckingInspection
        labels = item.find_all('p', 'srchlbls')
        for label in labels:
            kv_list = list(label.stripped_strings)
            key = kv_list[0][:-1]  # remove trailing colon
            if key == 'Opening Hours':
                value = get_opening_hours(kv_list[1:])
            else:
                value = value_converter(kv_list)
            data_object[key] = value
        if s77r is not None:  # pharmacists, assistants (value can be False)
            data_object['Section 77 Registration'] = s77r
        else:  # pharmacies
            data_object['Hospital'] = is_hospital_pharmacy(name)
            data_object['Other'] = is_other_pharmacy(name)
            roles = ('Supervising', 'Superintendent')
            pharmacy_ = next((ph for ph in pharmacies
                              if ph['PSI Registration Number'] == data_object['PSI Registration Number']), None)
            for role in roles:
                vacant_since = f'{role} Pharmacist Vacant Since'
                if data_object[f'{role} Pharmacist'] is None:
                    try:
                        if vacant_since not in pharmacy_.keys():  # newly vacant
                            print(Fore.CYAN + f'New {role} Vacancy - '
                                              f'{data_object["PSI Registration Number"]}: {name}, {address}.'
                                              f' ({pharmacy_[f"{role} Pharmacist"]})')
                            data_object[vacant_since] = today

                        else:  # old vacancy
                            data_object[vacant_since] \
                                = pharmacy_[vacant_since]

                    except AttributeError:  # new pharmacy
                        data_object[vacant_since] = today
                        print(Fore.GREEN + f'New {role} Vacancy (New Pharmacy) - '
                                           f'{data_object["PSI Registration Number"]}: {name}, {address}')
                else:
                    try:
                        if pharmacy_[f'{role} Pharmacist'] is None:
                            print(Fore.CYAN + f'{role} Pharmacist Vacancy Filled - '
                                              f'{data_object["PSI Registration Number"]}: {name}, {address}.'
                                              f' ({data_object[f"{role} Pharmacist"]})')
                    except TypeError:  # new pharmacy (with supervising/superintendent)
                        print(Fore.WHITE + f'New Pharmacy {data_object["Name"]}, {data_object["Address"]}')
        data_object_list.append(data_object)
    return data_object_list


async def get_data(data_type):
    """
    Assembles all tasks (i.e. GET requests) into a list, and converts to useful data
    :param data_type: assistant, pharmacist or pharmacy
    :return: no return value, writes retrieved data to JSON file
    """

    async with ClientSession() as session:

        tasks = [asyncio.ensure_future(
            fetch_register_page(
                asyncio.Semaphore(semaphore_pause),
                # seems to work fine at any number, but lower if script is overloading PSI website
                session, data_type.type,
                i)) for i in range(1, data_type.pages + 1)]

        # tdqm progress bar
        t = tqdm(asyncio.as_completed(tasks), total=len(tasks), delay=2)
        for n, r in enumerate(t, start=1):
            t.set_description(f"Getting data on page {n} of {data_type} register")
            # update description to match progress bar (not quite accurate)
            await r

        html_list = await asyncio.gather(*tasks)
        soup_map = (html_to_soup(h) for h in html_list)
        data_map = (data_from_soup(s) for s in soup_map)
        flat_list = list(chain.from_iterable(data_map))
        if data_type.__str__() == 'pharmacy':
            regs_new = [x['PSI Registration Number'] for x in flat_list]
            regs_old = [x['PSI Registration Number'] for x in pharmacies]
            removed = [x for x in pharmacies
                       if x['PSI Registration Number'] in regs_old
                       and x['PSI Registration Number'] not in regs_new]
            added = [x for x in flat_list
                     if x['PSI Registration Number'] in regs_new
                     and x['PSI Registration Number'] not in regs_old]
            for r in removed:
                print(f"Removed - {r['PSI Registration Number']}: {r['Name']}, {r['Address']}.")
            for a in added:
                print(f"Added - {a['PSI Registration Number']}: {a['Name']}, {a['Address']}.")
        write_to_json(flat_list, file_name=f"data/{data_type.json}-{today}.json")


def time_conv(t):
    """
        Convert time elapsed to a useful string
        :param t: time elapsed
        :return: time as a string
        """
    h = int(t // 3600)
    t %= 3600
    m = int(t // 60)
    s = int(t % 60)
    t = f"{str(h).zfill(2)}:{str(m).zfill(2)}:{str(s).zfill(2)}"
    return t


# noinspection PyTypeChecker
def run():
    """
    main loop controller
    :return: exit code (0 or 1)
    """
    try:
        loop = asyncio.get_event_loop()
        for x in (
                assistant,
                # pharmacist,
                pharmacy
        ):
            start = time.perf_counter()
            loop.run_until_complete(get_data(x))
            time_elapsed = time_conv(time.perf_counter() - start)
            print(Fore.WHITE + f'{x} data retrieved in {time_elapsed}')
            time.sleep(1)
        return 1
    except ClientPayloadError as e:
        print(Fore.LIGHTRED_EX + e)
        return 0


if __name__ == '__main__':
    exit_code = run()  # 1 - success, 0 - exception triggered
    total_time_elapsed = time_conv(time.perf_counter() - start_global)
    print(Fore.YELLOW + f'Total time taken: {total_time_elapsed}')
    exit(exit_code)
