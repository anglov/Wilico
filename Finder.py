# -*- coding: utf-8 -*-
from collections import defaultdict
import csv
import argparse


class Finder():
    def __load_data(self, path):
        data = []
        with open(path, encoding="utf-8") as file:
            r = csv.reader(file, dialect=csv.unix_dialect)
            data = list(tuple(row) for row in r)
        return data

    def suggest(self, word):
        if self.exactly:
            result = [x for x in self.data if x[0] == word]
        else:
            result = [x for x in self.data if x[0].find(word) > -1]
        return sorted(result, reverse=True, key=lambda y: int(y[1]))

    def print(self, words):
        for word in words:
            print('Results for ' + word)
            results = self.suggest(word)
            print("Counts\t\tTitle")
            if not results:
                print(str(0) + '\t\t' + word)
            for res in results[:self.number]:
                print(str(res[1]) + '\t\t' + res[0])
            print('End of results\n')

    def __init__(self, file, number, exactly):
        self.number = number
        self.exactly = exactly
        self.data = self.__load_data(file)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Finding word in base of titles of wikipedia and show links to this site')
    parser.add_argument('Words', metavar='Words', type=str, nargs='+',
                        help='Words to find in base')
    parser.add_argument('--file', default='count.csv', type=str,
                        help='directory od the base (default /count.csv)')
    parser.add_argument('--no', type=int, default=10,
                        help='numer of showed links in output ( default 10 )')
    parser.add_argument('--exa', action='store_const',
                        const=True, default=False,
                        help='exactly phase (default is finding all titles containing that phase)')
    args = parser.parse_args()
    finder = Finder(args.file, args.no, args.exa)
    finder.print(args.Words)