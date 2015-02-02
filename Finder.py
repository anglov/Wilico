# -*- coding: utf-8 -*-
import csv


def load_data(path):
    data = []
    with open(path, encoding="utf-8") as file:
       r = csv.reader(file,
            dialect=csv.unix_dialect)
       for row in r:
            data.append([row[0], int(row[1])])
            # print ([row[0], int(row[1])])
    return data


def suggester(input, data):
    result = []
    for item in data:
        if item[0].find(input)>-1:
            result.append(item)
    return sorted(result, reverse=True, key=lambda y: y[1])


if __name__ == '__main__':
    test = load_data("C:/wilico/count.csv")
    print (suggester("Fort",test))
