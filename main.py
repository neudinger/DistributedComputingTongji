#!/usr/bin/env python
# 'compute' is distributed to each node running 'dispynode'


# MIT License
#
# Copyright (c) 2018 Kevin Barre
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# The average amount of outgoing calls every day by each mobile customer.
# Le nombre moyen d'appels sortants chaque jour par chaque client mobile
# <calling number, amount of calls>
def calc_customer_call(filename: str, rangefile: tuple):
    import csv
    csv.register_dialect("tabulation", delimiter='\t')
    customer_amount = {}
    journbr = 0
    actualday = ""
    with open(filename) as dayfile:
        daycsvfile = csv.reader(dayfile, dialect="tabulation")
        for line in daycsvfile:
            if actualday != line[0]:
                actualday = line[0]
                journbr += 1
        dayfile.close()

    with open(filename) as file:
        csvfile = csv.reader(file.readlines()[rangefile[0]:rangefile[1]], dialect="tabulation")
        for line in csvfile:
            if customer_amount.get(line[1]):
                customer_amount[line[1]] += 1
            else:
                customer_amount[line[1]] = 1
    for customer in customer_amount:
        customer_amount[customer] = [customer_amount[customer], journbr]
        file.close()
    return ["calc_customer_call", customer_amount]


# Proportions of the network operators (China Mobile/China Unicom/China Telecom)
# of the called parties by each type of call (local/long-distance/roaming).
# Proportions des opérateurs de réseau (China Mobile / China Unicom / China Telecom)
# des parties appelées par chaque type d'appel (local / longue distance / roaming).
# <Operator-distance, Callingoptrnumber>

def network_operator(filename: str, rangefile: tuple):
    import csv
    csv.register_dialect("tabulation", delimiter='\t')
    operator_amount = {}
    with open(filename) as file:
        csvfile = csv.reader(file.readlines()[rangefile[0]:rangefile[1]], dialect="tabulation")
        for line in csvfile:
            if operator_amount.get(line[4] + "-" + line[12]):
                operator_amount[line[4] + "-" + line[12]] += 1
            else:
                operator_amount[line[4] + "-" + line[12]] = 1
        file.close()
    return ["network_operator", operator_amount]


# Proportions of call durations in different time sections 2 by each mobile customer.
# Output record format: <calling number, proportion of call duration in TS1, proportion of call
# duration in TS2, ..., proportion of call duration in TS8>
# Time Section ID Duration
# TS1 0:00-2:59
# TS2 3:00-5:59
# TS3 6:00-8:59
# TS4 9:00-11:59
# TS5 12:00-14:59
# TS6 15:00-17:59
# TS7 18:00-20:59
# TS8 21:00-23:59
def call_proportion(filename: str, rangefile: tuple):
    from datetime import datetime
    import csv

    csv.register_dialect("tabulation", delimiter='\t')
    customer_mobile_time_total = {}
    with open(filename) as file:
        csvfile = csv.reader(file.readlines()[rangefile[0]:rangefile[1]], dialect="tabulation")
        for line in csvfile:
            start_time = datetime.strptime(line[9], "%H:%M:%S")
            end_time = datetime.strptime(line[10], "%H:%M:%S")
            duration = end_time - start_time
            if duration.days:
                duration = start_time - end_time
            nb = round(duration.seconds / 1440 / 8) + 1
            if customer_mobile_time_total.get(line[1]):
                if customer_mobile_time_total[line[1]].get("TS" + str(nb)):
                    customer_mobile_time_total[line[1]]["TS" + str(nb)] += 1
                else:
                    customer_mobile_time_total[line[1]]["TS" + str(nb)] = 1
            else:
                customer_mobile_time_total[line[1]] = {}
                customer_mobile_time_total[line[1]]["TS" + str(nb)] = 1
        file.close()
    return ["call_proportion", customer_mobile_time_total]


def writecsv(data: dict, name: str, complex: int):
    if complex == 1:
        with open(name, 'w') as csv_file:
            writer = csv.writer(csv_file)
            for key, value in data.items():
                writer.writerow([key, *value.values()])
            csv_file.close()
    elif complex == 2:
        with open(name, 'w') as csv_file:
            writer = csv.writer(csv_file)
            for key, value in data.items():
                writer.writerow([key, value[0] / value[1]])
            csv_file.close()
    else:
        with open(name, 'w') as csv_file:
            writer = csv.writer(csv_file)
            for key, value in data.items():
                writer.writerow([key, value])
            csv_file.close()


if __name__ == '__main__':
    import csv
    import sys
    import os.path
    from dask.distributed import Client

    if len(sys.argv) != 3:
        print("\"file name\" \"number of cluster\" must be specified")
        exit(code=1)
    filename = sys.argv[1]
    if not os.path.isfile(filename):
        print("{} does not exit".format(filename))
        exit(code=1)
    if not sys.argv[2].isnumeric():
        print("\"number of cluster\" must be integer")
        exit(code=1)

    nbofcluster = int(sys.argv[2])
    fromline = 0
    toline = 0
    csv.register_dialect("tabulation", delimiter='\t')
    jobs = []
    clusters = []
    with open(filename) as f:
        nbline = sum(1 for nbline in csv.reader(f, dialect="tabulation"))
        f.close()
    for cluster in range(nbofcluster):
        toline = int((nbline / nbofcluster) * (cluster + 1))
        cluster = Client()
        clusters.append(cluster)
        cluster.set_metadata("clusternb", str(cluster))
        cluster.upload_file(filename)
        jobs.append(cluster.submit(network_operator, filename, (fromline, toline)))
        jobs.append(cluster.submit(calc_customer_call, filename, (fromline, toline)))
        jobs.append(cluster.submit(call_proportion, filename, (fromline, toline)))
        fromline = int(toline)

    call_proportion_data = {}
    network_operator_data = {}
    calc_customer_call_data = {}
    for job in jobs:
        result = job.result()
        if result[0] == "network_operator":
            for key, value in result[1].items():
                if network_operator_data.__contains__(key):
                    network_operator_data[key] += value
                else:
                    network_operator_data[key] = value

        elif result[0] == "calc_customer_call":
            for key, value in result[1].items():
                if calc_customer_call_data.__contains__(key):
                    calc_customer_call_data[key][0] += value[0]
                else:
                    calc_customer_call_data[key] = [value[0], value[1]]
        else:
            for value in result[1]:
                if not call_proportion_data.__contains__(value):
                    call_proportion_data[value] = {}
                for ts, nb in result[1][value].items():
                    if call_proportion_data[value].__contains__(ts):
                        call_proportion_data[value][ts] += nb
                    else:
                        call_proportion_data[value][ts] = nb

    writecsv(call_proportion_data, "call_proportion_data", 1)
    writecsv(calc_customer_call_data, "calc_customer_call_data", 2)
    writecsv(network_operator_data, "network_operator_data", 3)
