import io
import bz2
import re
import lxml.etree as ET
from collections import defaultdict
import csv
import argparse
import operator
from multiprocessing import Queue, Process
from queue import Empty


class Counter():
    def _process(self, q_out, q_in):
        while True:
            item = q_in.get()
            if item == Empty:
                q_in.put(Empty)
                q_out.put(Empty)
                break
            q_out.put(self._get_links(item))

    def _reader(self, IN, q_out):
        if IN.endswith("bz2"):
            open_func = bz2.open
        else:
            open_func = open
        with open_func(IN) as f:
            context = ET.iterparse(f, events=("start", "end"))
            event, root = context.__next__()
            for event, elem in context:
                if event == "end" and elem.tag == "text":
                    if elem.text:
                        q_out.put(elem.text)
                    root.clear()
            q_out.put(Empty)

    def _get_links(self, page_contents):
        page = io.StringIO()
        for c in page_contents:
            page.write(c)
        page = page.getvalue()
        pa = re.findall("\[\[([^\[\]\|\#]+)(?:[|#][^\]]+)?\]\]", page, flags=re.UNICODE)
        return pa

    def links_counter(self, IN):
        links_count = defaultdict(lambda: 0)
        end_count = 0
        q_in, q_out = Queue(), Queue()
        processes = []
        p = Process(target=self._reader, args=(IN, q_out))
        p.start()
        for ii in range(self.children):
            p = Process(target=self._process, args=(q_in, q_out))
            p.start()
            processes.append(p)
        while True:
            item = q_in.get()
            if item == Empty:
                end_count += 1
                if end_count >= self.children:
                    break
                else:
                    continue
            for link in item:
                links_count[link.strip()] += 1
        p.join()
        for p in processes:
            p.join()
        return links_count

    def save_to_file(self, out_file, contents):
        with open(out_file, 'w', encoding="utf-8") as f:
            w = csv.writer(f, dialect=csv.unix_dialect)
            for item in sorted(contents.items(), key=operator.itemgetter(0)):
                w.writerow([item[0],item[1]])

    def __init__(self, children):
        self.children = children


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Counting links from wikipedia')
    parser.add_argument('Input', metavar='Input', type=str, nargs='?',
                        help='file with  in format xml packed in .bz2')
    parser.add_argument('--out', default='count.csv', type=str,
                        help='directory od the output (default /count.csv)')
    parser.add_argument('--workers', default=8, type=int,
                        help='Number of workers, that will be spawn')
    args = parser.parse_args()
    children = args.workers
    counter = Counter(children)
    result = counter.links_counter(args.Input)
    counter.save_to_file(args.out ,result)