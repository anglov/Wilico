# -*- coding: utf-8 -*-
import csv
import argparse


def load_data(path):
    data = []
    with open(path, encoding="utf-8") as file:
        r = csv.reader(file,
            dialect=csv.unix_dialect)
        for row in r:
            data.append([row[0], int(row[1])])
    return data


def suggester(word, data, exactly):
    result = []
    for item in data:
        if exactly:
            if item[0] == word:
                result.append(item)
        else:
            if item[0].find(word) > -1:
                result.append(item)
    return sorted(result, reverse=True, key=lambda y: y[1])


def print_results(results, number):
    print("Counts\t Title")
    for n, l in enumerate(results):
        if n >= number:
            break
        print(str(l[1])+'\t\t'+l[0])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Finding word in base of titles of wikipedia and show links to this site')
    parser.add_argument('Words', metavar='Words', type=str, nargs='+',
                        help='Words to find in base')
    parser.add_argument('--base', default='count.csv', type=str,
                        help='directory od the base (default /count.csv)')
    parser.add_argument('--no', type=int, default=10,
                        help='numer of showed links in output ( default 10 )')
    parser.add_argument('--exa', action='store_const',
                        const=True, default=False,
                        help='exactly phase (default is finding all titles containing that phase)')
    args = parser.parse_args()
    base = load_data(args.base)
    for w in args.Words:
        print('Results for '+w)
        print_results(suggester(w, base, args.exa), args.no)