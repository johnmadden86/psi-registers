import csv

with open('2016-04-28_general-election-2016-candidate-details-csv_en.csv', 'r') as csv_file:
    reader = csv.DictReader(csv_file)
    # for row in reader:
    #     row['id'] = f"{row['Candidate Id']}-{row['Constituency Number']}"

    for row in reader:
        row['Candidate Id'] = f"{row['Candidate Id']}-{row['Constituency Number']}"
        print(row)