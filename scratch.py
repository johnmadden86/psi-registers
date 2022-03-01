import csv
import json
from scrape_old import html_to_soup, parse_data_from_soup
from collections import OrderedDict

with open('listofgpswithgmscontracts.csv', 'r') as csv_file:
    reader = csv.reader(csv_file)
    gms_numbers = set()
    gms_numbers_list = []
    imc_numbers = set()
    imc_numbers_list = []
    err = []

    doctors = set()
    for row in reader:
        try:
            gms_numbers.add(int(row[0]))
            imc_numbers.add(int(row[4]))
            gms_numbers_list.append(int(row[0]))
            imc_numbers_list.append(int(row[4]))
            # if 'Kilkenny' in row[3]:
            #     doctors.add(f"{row[1]} {row[2]}")
        except ValueError:
            err.append(row)
    print(imc_numbers_list)
    print(len(gms_numbers))
    print(len(gms_numbers_list))
    print(len(imc_numbers))
    print(len(imc_numbers_list))
    print(len(err))
    # for n, doctor in enumerate(doctors, start=1):
    #     print(n, doctor)

exit()

import requests
from datetime import datetime

x = []
for y in range(8, 20):
    date_string = None
    for m in range(1, 13):
        date = datetime(year=2000 + y, month=m, day=1)
        date_string = f'{date.strftime("%B")[:3].upper()}_{date.strftime("%Y")[-2:]}'
        url = f'http://www.greencrosspublishing.ie/attachments/IRISH_PHARMACIST_{date_string}.pdf'

        if requests.head(url).status_code == 200:
            x.append(date_string)
    print(len(x), date_string[-2:])
exit()
