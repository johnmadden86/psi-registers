import asyncio
import csv
import itertools
import json
import os
import platform
import time
from collections import namedtuple
from datetime import date, datetime, timedelta

from bs4 import BeautifulSoup

import aiohttp
from tqdm import tqdm

BASE_URL = 'http://public.thepsi.ie/'
PARSER = 'lxml'

DataType = namedtuple('DataType', ['tag', 'target', 'json'])
assistant = DataType(tag='PublicAssistantId', target='publicassistants', json='assistant-data')
pharmacist = DataType(tag='PublicPharmacistId', target='publicpharmacists', json='pharmacist-data')
pharmacy = DataType(tag='PublicPharmaciesId', target='publicpharmacies', json='pharmacy-data')


start_time = time.perf_counter()


def shutdown():
    """
    Shuts down the computer
    """
    print('Shutting down computer')
    if platform.system() == "Windows":
        os.system("shutdown -s -t 0")
    else:
        os.system("shutdown -h now")


async def fetch_register_page(session, url, payload):
    try:
        async with session.post(url, data=payload) as response:
            return await response.text()
    except Exception as e:
        print('fetch', e)


async def fetch_link(session, url, params):
    try:
        async with session.get(url, params=params) as response:
            return await response.text()
    except Exception as e:
        print(e)


def html_to_soup(html):
    try:
        return BeautifulSoup(html, PARSER)
    except Exception as e:
        print('Error parsing html', e)


def get_id_no(soup, tag):
    """
    Gets the id number of the object retrieved if a link is clicked in the registers
    :param soup: beautiful soup object containing clickable html link with id embedded
    :param tag: {tag} in above, depends on if searching for pharmacists/assistants/pharmacies
    :return: the id number of the object retrieved if the link is clicked
    """
    try:
        table_data = soup.find('td')  # 4 table entries, first 2 have links, only one required
        id_containing_link = table_data.a['href']
        id_first_index = id_containing_link.find(tag) + len(tag) + len('=')
        id_last_index = id_first_index
        while id_containing_link[id_last_index].isdigit():
            id_last_index += 1
        return int(id_containing_link[id_first_index: id_last_index])
    except Exception as e:
        print(e)


def get_all_ids_on_page(soup, tag):
    """
    Get all ids on a page of the register
    :param soup: beautiful soup object of the html retrieved
    :param tag: {tag} in above, depends on if searching for pharmacists/assistants/pharmacies
    :return: a list of ids on the page
    """
    try:
        table = soup.table
        table_rows = table.find_all('tr')  # rows of the table containing the id strings
        return [get_id_no(table_rows[j], tag) for j in range(1, len(table_rows))]
    except Exception as e:
        print(e)


def get_last_page(soup):
    """
        Gets the number of pages of data in the register for pharmacists/assistants/pharmacies
        :param soup: html converted to beautiful soup object for page 1 of the register
        :return: the last page number
    """
    try:
        tag = soup.find('div', 'pagination').find('h4')  # Page {x} of {y}
        last_pg = tag.string.split()[-1]
        return int(last_pg)
    except Exception as e:
        print(e)


def get_opening_hours(soup):
    """
    Get a pharmacy's opening hours
    :param soup: Nested table as retrieved during scraping
    :return: Opening hours as a dict
    """
    opening_hours = {}
    nested_table_rows = soup.find_all('tr')
    for ntr in nested_table_rows:
        table_data = ntr.find_all('td')
        if not table_data[0].string.isspace():
            day = table_data[0].string
            opening_time = table_data[1].string
            closing_time = table_data[2].string
            opening_hours[clean_up_string(day)] = {
                'Open': clean_up_string(opening_time),
                'Closed': clean_up_string(closing_time)
            }
    return opening_hours


def clean_up_string(v):
    """
    Remove leading space before commas
    Insert trailing space after commas
    Remove double spaces
    Remove leading spaces and trailing spaces
    :param v: string value
    :return: cleaned up string value (or integer if applicable). None if null value, only spaces, or empty string
    """
    try:
        if v.isspace() or len(v) == 0:
            raise AttributeError
        v = v.replace(' ,', ',')
        v = v.replace(',', ', ')
        v = v.replace('  ', ' ')
        v = ' '.join(filter(None, v.split()))
        v = str_to_int(v)
        return v
    except AttributeError:
        return None


def str_to_int(v):
    """
    Convert (if possible) string value to integer
    :param v: string value
    :return: integer (possible), unmodified string otherwise
    """
    try:
        if not isinstance(v, bool):
            return int(v)
    except (TypeError, ValueError):
        return v


def is_hospital_pharmacy(pharmacy_name):
    """
    Check if a pharmacy is in a hospital
    :param pharmacy_name: the pharmacy's name
    :return: True if the pharmacy is in a hospital, otherwise False
    """
    hospital_terms = ['h.s.e.', 'department', 'clinic', 'hospice', 'hospital']
    for h in hospital_terms:
        if h in pharmacy_name.lower():
            if 'allcare' in pharmacy_name.lower():  # Hospital, Co. Limerick
                return False
            return True
    return False


def is_animal_pharmacy(pharmacy_name):
    """
    Check if a pharmacy is a dedicated veterinary pharmacy
    :param pharmacy_name: the pharmacy's name
    :return: True if the pharmacy is in an animal pharmacy, otherwise False
    """
    animal_terms = ['animal', 'stock health']
    for a in animal_terms:
        if a in pharmacy_name.lower():
            return True
    return False


def get_ownership_type(n):
    ownership_types = {
        '1': 'Private',
        '3': 'Sole Trader',
        '4': 'Public'
    }
    try:
        return ownership_types[str(n)]
    except KeyError:
        return n


def count_hours(opening_hours):
    weekly_hours = 0
    for day in opening_hours:
        try:
            open_ = datetime.strptime(opening_hours[day]['Open'], '%H:%M')
            closed_ = datetime.strptime(opening_hours[day]['Closed'], '%H:%M')
        except TypeError:
            pass
        else:
            hours = (closed_ - open_) / timedelta(hours=1)
            hours %= 24
            opening_hours[day]['Hours Open'] = hours
            weekly_hours += hours
    opening_hours['Weekly Hours'] = weekly_hours
    return opening_hours


def check_conditions_attached(value):
    try:
        value.strong.decompose()
        # Conditions attached true where registration number is marked with <strong> red asterisk
    except AttributeError:
        return False
    else:
        return True


def yes_no_to_bool(str_):
    try:
        y = ['YES', 'Y']
        n = ['NO', 'N']
        if str_.upper() in y:
            return True
        elif str_.upper() in n:
            return False
    finally:
        return str_


def parse_data_from_soup(soup):
    """
    Scrape details from the PSI registers
    :param soup: BS object containing data
    :return: the scraped data object
    """
    # noinspection PyGlobalUndefined
    global key
    scraped_data_object = {}
    hidden = soup.find_all('input')
    for h in hidden:
        name = h.get('name')
        if name not in ('Action', 'AddSeq', 'Mode', 'target'):
            value = yes_no_to_bool(h.get('value'))
            value = clean_up_string(value)
            scraped_data_object[name] = value
    table = soup.table
    rows = table.find_all('tr', recursive=False)  # doesn't include nested table if present
    conditions = False  # Conditions Attached to Registration
    for row in rows:
        table_data = row.find_all('td')
        try:
            [key, value] = table_data
        except ValueError:  # pharmacy opening hours appear in nested table
            key, value = row.td, row.table
            opening_hours = get_opening_hours(value)
            value = count_hours(opening_hours)
        else:
            if key == 'Registration Number:':
                conditions = check_conditions_attached(value)
            if key == 'Conditions Attached to Registration:':
                value = True if value == 'Yes' else conditions
            value = clean_up_string(value.string)
        finally:
            key = key.string
            key = key.replace(':', '')  # remove colon from key for insertion into dict object

        scraped_data_object[key] = value

    try:
        hospital = is_hospital_pharmacy(scraped_data_object['Pharmacy Name'])
        animal = is_animal_pharmacy(scraped_data_object['Pharmacy Name'])
        ownership_type = get_ownership_type(scraped_data_object['PublicPharmaciesOwnershipType'])
    except KeyError:
        pass
    else:
        scraped_data_object['Hospital'] = hospital
        scraped_data_object['Animal'] = animal
        scraped_data_object['PublicPharmaciesOwnershipType'] = ownership_type

    return scraped_data_object


def write_to_json(dump_data, file_name):
    """
    Write retrieved data to a json file
    :param file_name: name of json file
    :param dump_data: a list of the data objects retrieved
    """
    with open(file_name, 'w') as output_file:
        json.dump(dump_data, output_file, indent=2)


def write_to_csv(dump_data, file_name):
    """
    Write retrieved data to a csv file
    :param dump_data: a list of the data objects retrieved
    :param file_name: name of csv file
    :return:
    """
    with open(file_name, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, dump_data[0].keys())
        dict_writer.writeheader()
        dict_writer.writerows(dump_data)


async def run(data_type, page=0, last_pg=None):
    tag, target = data_type.tag, data_type.target
    async with aiohttp.ClientSession() as session:
        post_data = {'target': target, 'Action': 'submit', 'Mode': 'search', 'PageNumber': page}
        html = await fetch_register_page(session, url=BASE_URL, payload=post_data)
        soup = html_to_soup(html)
        if last_pg is None:
            last_pg = get_last_page(soup)
        ids = get_all_ids_on_page(soup, tag)
        params = list(map(lambda id_: {'target': target, 'Mode': 'view', tag: id_}, ids))

        tasks = [asyncio.ensure_future(fetch_link(session, BASE_URL, params[i])) for i in range(len(params))]
        [await r for r in tqdm(asyncio.as_completed(tasks), total=len(tasks),
                               desc=f"Getting items on page {page + 1} of {last_pg}")]
        html_list = await asyncio.gather(*tasks)
        soup_list = map(html_to_soup, html_list)
        scraped_data_objects = map(parse_data_from_soup, soup_list)

        if page == 0:
            return list(scraped_data_objects), last_pg

        return list(scraped_data_objects)


async def get_remaining_pages(data_type, last_pg):
    tasks = [asyncio.ensure_future(run(data_type, k)) for k in range(1, last_pg)]
    [await r for r in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Getting pages 2-{last_pg}")]
    return itertools.chain.from_iterable(await asyncio.gather(*tasks))


def get_all_data(data_type):
    loop = asyncio.get_event_loop()
    data, last_page = loop.run_until_complete(run(data_type))  # get first page
    # data.extend(loop.run_until_complete(get_remaining_pages(data_type, last_page)))
    for k in range(1, last_page):
        data.extend(loop.run_until_complete(run(data_type, k, last_page)))
    write_to_json(data, f"data/{data_type.json}-{date.today()}.json")
    return data


def find_no_supervising(p):
    no_supervising = list(filter(lambda p: p['Supervising Pharmacist'] is None, p))
    with open ('no-sup.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        for p in no_supervising:
            writer.writerow([date.today()] + p.values())


def time_conv(t):
    h = int(t // 3600)
    t %= 3600
    m = int(t // 60)
    s = int(t % 60)
    t = f"{str(h).zfill(2)}:{str(m).zfill(2)}:{str(s).zfill(2)}"
    return t


if __name__ == '__main__':
    # get_all_data(assistant)
    # print(time.perf_counter() - start_time)
    # get_all_data(pharmacist)
    # print(time.perf_counter() - start_time)
    pharmacies = get_all_data(pharmacy)
    # find_no_supervising(pharmacies)
    # find_no_supervising(pharmacies)
    time_elapsed = time_conv(time.perf_counter() - start_time)
    print(time_elapsed)
    # t = time.strftime('%h:%m:%s', t)
    # shutdown()
    exit()
