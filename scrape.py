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

today = str(date.today())

start_time = time.perf_counter()

with open('no-sup.json') as json_file:
    no_sup = json.load(json_file)


def shutdown():
    """
    Shuts down the computer
    """
    if platform.system() == "Windows":
        os.system("shutdown -s -t 0")
    else:
        os.system("shutdown -h now")


async def fetch_register_page(session, url, payload):
    """
    Fetch HTML from one page of the register
    :param session: open asyncio client session
    :param url: URL to fetch
    :param payload: parameters for POST request
    :return: HTML for that page
    """
    # noinspection PyBroadException
    try:
        async with session.post(url, data=payload) as response:
            return await response.text()
    except Exception:
        pass


async def fetch_link(session, url, params):
    """
    Fetch HTML from one link
    :param session: open asyncio client session
    :param url: URL to fetch
    :param params: parameters for GET request
    :return: HTML for that link
    """
    # noinspection PyBroadException
    try:
        async with session.get(url, params=params) as response:
            return await response.text()
    except Exception:
        pass


def html_to_soup(html):
    """
    Maps HTML to BeautifulSoup object
    :param html: HTML to parse
    :return: BeautiulSoup object
    """
    # noinspection PyBroadException
    try:
        return BeautifulSoup(html, PARSER)
    except Exception:
        pass


def get_id_no(soup, tag):
    """
    Gets the id number of the object retrieved if a link is clicked in the registers
    :param soup: beautiful soup object containing clickable html link with id embedded
    :param tag: {tag} in above, depends on if searching for pharmacists/assistants/pharmacies
    :return: the id number of the object retrieved if the link is clicked
    """
    # noinspection PyBroadException,DuplicatedCode
    try:
        table_data = soup.find('td')  # 4 table entries, first 2 have links, only one required
        id_containing_link = table_data.a['href']
        id_first_index = id_containing_link.find(tag) + len(tag) + len('=')
        id_last_index = id_first_index
        while id_containing_link[id_last_index].isdigit():
            id_last_index += 1
        return int(id_containing_link[id_first_index: id_last_index])
    except Exception:
        pass


def get_all_ids_on_page(soup, tag):
    """
    Get all ids on a page of the register
    :param soup: beautiful soup object of the html retrieved
    :param tag: {tag} in above, depends on if searching for pharmacists/assistants/pharmacies
    :return: a list of ids on the page
    """
    # noinspection PyBroadException
    try:
        table = soup.table
        table_rows = table.find_all('tr')  # rows of the table containing the id strings
        return [get_id_no(table_rows[j], tag) for j in range(1, len(table_rows))]
    except Exception:
        pass


def get_last_page(soup):
    """
        Gets the number of pages of data in the register for pharmacists/assistants/pharmacies
        :param soup: html converted to beautiful soup object for page 1 of the register
        :return: the last page number
    """
    # noinspection PyBroadException
    try:
        tag = soup.find('div', 'pagination').find('h4')  # Page {x} of {y}
        last_pg = tag.string.split()[-1]
        return int(last_pg)
    except Exception:
        pass


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
            opening_time = clean_up_string(table_data[1].string)
            try:
                if int(opening_time[:2]) > 15:
                    opening_time = f"{int(opening_time[:2]) - 12}{opening_time[2:]}"
            except TypeError:
                pass
            closing_time = clean_up_string(table_data[2].string)
            try:
                if int(closing_time[:2]) < 12:
                    closing_time = f"{int(closing_time[:2]) + 12}{closing_time[2:]}"
            except TypeError:
                pass
            opening_hours[clean_up_string(day)] = {
                'Open': opening_time,
                'Closed': closing_time
            }
    return opening_hours


# noinspection DuplicatedCode
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
    """
    Map the pharmacy ownership type from a digit to a more meaningful string
    :param n: digit as per register
    :return: string, if n = 1, 3 or 4
    """
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
    """
    Count the total number of hours per week the pharmacy is open
    :param opening_hours: dict with pharmacy's opening hours
    :return: number of hours the pharmacy is open as a float
    """
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
    """
    Find if conditions are attached to a pharmacist's registration
    true where registration number is marked with <strong> red asterisk
    :param value: the string inside the HTML tag containing a pharmacist's registration number
    :return: True or False
    """
    try:
        value.strong.decompose()
    except AttributeError:
        return False
    else:
        return True


def yes_no_to_bool(str_):
    """
    Map strings 'yes' or 'no' to True or False
    :param str_: 'yes' or 'no'
    :return: True or False
    """
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
    global key, vacant_since
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
            if key.string == 'Registration Number:':
                conditions = check_conditions_attached(value)
            elif key == 'Conditions Attached to Registration:':
                value = True if value[:3] == 'Yes' else conditions
            value = clean_up_string(value.string)
        finally:
            key = key.string
            key = key.replace(':', '')  # remove colon from key for insertion into dict object
        scraped_data_object[key] = value

    try:
        scraped_data_object['Hospital'] = is_hospital_pharmacy(scraped_data_object['Pharmacy Name'])
        scraped_data_object['Animal'] = is_animal_pharmacy(scraped_data_object['Pharmacy Name'])
        scraped_data_object['PublicPharmaciesOwnershipType'] = \
            get_ownership_type(scraped_data_object['PublicPharmaciesOwnershipType'])
    except KeyError:
        pass
    else:
        reg_no = str(scraped_data_object['Registration Number'])
        if scraped_data_object['Supervising Pharmacist'] is None:
            try:
                vacant_since = no_sup[reg_no]
            except KeyError:
                vacant_since = today
                no_sup[reg_no] = today
            finally:
                scraped_data_object['No Supervising Pharmacist Since'] = vacant_since
        else:
            try:
                del no_sup[reg_no]
            except KeyError:
                pass
            else:
                print(f"Supervising pharmacist appointed: {reg_no}")

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
    """
    1. Gets one page of the register, then...
    2. Gets data from all the links on that page
    :param data_type: Pharmacists, Pharmacies, or Assistants
    :param page: The page number to fetch
    :param last_pg: The number of pages on the registers
    :return: A list of dicts of the relevant data
    """
    tag, target = data_type.tag, data_type.target

    async with aiohttp.ClientSession() as session:

        post_data = {'target': target, 'Action': 'submit', 'Mode': 'search', 'PageNumber': page}

        # POST request, return one page of register
        html = await fetch_register_page(session, url=BASE_URL, payload=post_data)

        soup = html_to_soup(html)  # map to BeautifulSoup object

        if last_pg is None:  # only for first page
            last_pg = get_last_page(soup)

        ids = get_all_ids_on_page(soup, tag)  # ids for next set of requests

        # list of params for next set of requests
        params = list(map(lambda id_: {'target': target, 'Mode': 'view', tag: id_}, ids))

        # make the GET requests (asynchronous)
        tasks = [asyncio.ensure_future(fetch_link(session, BASE_URL, params[i])) for i in range(len(params))]

        # tdqm progress bar
        [await r for r in tqdm(asyncio.as_completed(tasks), total=len(tasks),
                               desc=f"Getting items on page {page + 1} of {last_pg}")]

        html_list = await asyncio.gather(*tasks)  # gather the results
        soup_list = map(html_to_soup, html_list)  # map to BeautifulSoup object
        scraped_data_objects = map(parse_data_from_soup, soup_list)  # map relevant data to useful dict

        if page == 0:  # only for first page
            return list(scraped_data_objects), last_pg

        return list(scraped_data_objects)  # list of dicts


async def get_remaining_pages(data_type, last_pg):  # not in use
    tasks = [asyncio.ensure_future(run(data_type, k)) for k in range(1, last_pg)]
    [await r for r in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Getting pages 2-{last_pg}")]
    return itertools.chain.from_iterable(await asyncio.gather(*tasks))


def get_all_data(data_type):
    """
    Controls the event loop for the script
    :param data_type: Pharmacists, Pharmacies, or Assistants
    :return: all the data gathered
    :rtype list of dicts
    """
    loop = asyncio.get_event_loop()
    data, last_page = loop.run_until_complete(run(data_type))  # get first page
    for k in range(1, last_page):
        data.extend(loop.run_until_complete(run(data_type, k, last_page)))
    write_to_json(data, f"data/{data_type.json}-{today}.json")
    return data


def find_no_supervising(p):
    """
    Finds pharmacies where no supervising pharmacist is nominated
    :param p: list of pharmaices
    :return: none
    """
    no_supervising = list(filter(lambda ph: ph['Supervising Pharmacist'] is None, p))
    with open('no-sup.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        for p in no_supervising:
            row = [today, p.get('Registration Number'), p.get('Pharmacy Name'), p.get('Pharmacy Address'),
                   p.get('Pharmacy Owner'), p.get('Superintendent Pharmacist')]
            writer.writerow(row)
    with open('no-sup.json', 'w') as file:
        json.dump(no_sup, file, indent=2)


# noinspection DuplicatedCode
def time_conv(t):
    # noinspection DuplicatedCode
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


if __name__ == '__main__':
    # get_all_data(assistant)
    # get_all_data(pharmacist)
    pharmacies = get_all_data(pharmacy)
    find_no_supervising(pharmacies)
    time_elapsed = time_conv(time.perf_counter() - start_time)
    print(time_elapsed)
    # shutdown()
    exit()
