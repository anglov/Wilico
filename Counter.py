import io
import bz2
import re
import lxml.etree as ET
from collections import defaultdict
import csv
import argparse
import operator
from multiprocessing import Queue, Process, Lock, Value
from queue import Empty


class Counter():
    def _process(self, q_out, q_in):
        while True:
            item = q_in.get()
            if item == Empty:
                with self.parsers_done_lock:
                    self.parsers_done.value += 1
                    if self.parsers_done.value >= self.parsers:
                        q_in.put(Empty)
                        q_out.put(Empty)
                        break
                    else:
                        continue
            q_out.put(self._get_links(item))

    def _reader(self, file_in, q_out):
        while True:
            path = file_in.get()
            if path == Empty:
                file_in.put(Empty)
                q_out.put(Empty)
                break
            if path.endswith("bz2"):
                open_func = bz2.open
            else:
                open_func = open
            with open_func(path) as f:
                context = ET.iterparse(f, events=("start", "end"))
                event, root = context.__next__()
                for event, elem in context:
                    if event == "end" and elem.tag == "text":
                        if elem.text:
                            q_out.put(elem.text)
                        root.clear()

    def _get_links(self, page_contents):
        page = io.StringIO()
        for c in page_contents:
            page.write(c)
        page = page.getvalue()
        pa = re.findall("\[\[([^\[\]\|\#]+)(?:[|#][^\]]+)?\]\]", page, flags=re.UNICODE)
        return pa

    def links_counter(self, files):
        links_count = defaultdict(lambda: 0)
        end_count = 0
        file_out, q_in, q_out = Queue(), Queue(), Queue()
        processes = []
        readers = []

        for _ in range(self.parsers):
            p = Process(target=self._reader, args=(file_out, q_out))
            p.start()
            readers.append(p)

        for _ in range(self.workers):
            p = Process(target=self._process, args=(q_in, q_out))
            p.start()
            processes.append(p)

        for file in files:
            file_out.put(file)
        file_out.put(Empty)

        while True:
            item = q_in.get()
            if item == Empty:
                end_count += 1
                if end_count >= self.workers:
                    break
                else:
                    continue
            for link in item:
                links_count[link.strip()] += 1

        for p in readers:
            p.join()
        for p in processes:
            p.join()

        return links_count

    def save_to_file(self, out_file, contents):
        with open(out_file, 'w', encoding="utf-8") as f:
            w = csv.writer(f, dialect=csv.unix_dialect)
            for item in sorted(contents.items(), key=operator.itemgetter(0)):
                w.writerow([item[0],item[1]])

    def __init__(self, workers, parsers):
        self.workers = workers
        self.parsers = parsers
        self.parsers_done = Value("i",0)
        self.parsers_done_lock = Lock()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Counting links from wikipedia')
    parser.add_argument('Input', metavar='Input', type=str, nargs='+',
                        help='file with  in format xml packed in .bz2')
    parser.add_argument('--out', default='count.csv', type=str,
                        help='directory od the output (default /count.csv)')
    parser.add_argument('--workers', default=8, type=int,
                        help='Number of workers, that will be spawn')
    parser.add_argument('--parsers', default=2, type=int,
                        help='Number of file parsers, that will be spawn.')
    args = parser.parse_args()
    workers = args.workers
    parsers = args.parsers
    counter = Counter(workers, parsers)
    result = counter.links_counter(args.Input)
    counter.save_to_file(args.out ,result)