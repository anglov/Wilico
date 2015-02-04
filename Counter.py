import io
import bz2
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
import csv
import argparse

def get_links(page_contents):
    page = io.StringIO()
    for c in page_contents:
        page.write(c)
    page = page.getvalue()
    pa = re.findall("\[\[([^\[\]\|\#]+)(?:[|#][^\]]+)?\]\]", page, flags=re.UNICODE)
    # print(pa)
    return pa


def links_counter (IN):
    links_count = defaultdict(lambda: 0)
    if IN.endswith("bz2"):
        open_func = bz2.open
    else:
        open_func = open
    with open_func(IN) as f:
        context = ET.iterparse(f, events=("start", "end"))
        context = iter(context)
        event, root = context.__next__()
        for event, elem in context:
            if event == "end" and elem.tag == "text":
                if elem.text:
                    links = get_links(elem.text)
                    for m in links:
                        links_count[m.strip()] += 1
                root.clear()
    return links_count


def save_to_file(out_file, contents):
    out = []
    for i in contents.keys():
        out.append((i, contents[i]))
    with open(out_file, 'w', encoding="utf-8") as f:
        w = csv.writer(f, dialect=csv.unix_dialect)
        for item in sorted(out):
            w.writerow([item[0],item[1]])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Counting links from wikipedia')
    parser.add_argument('Input', metavar='Input', type=str, nargs='?',
                        help='file with  in format xml packed in .bz2')
    parser.add_argument('--out', default='count.csv', type=str,
                        help='directory od the output (default /count.csv)')
    args = parser.parse_args()
    result = links_counter(args.Input)
    save_to_file (args.out ,result)