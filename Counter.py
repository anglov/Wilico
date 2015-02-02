import io
import bz2
import re
from xml.dom.pulldom import parse, START_ELEMENT
from collections import defaultdict
import csv

link_re = re.compile("\[\[([^\[\]\|]+)(?:\|[^\]]+)?\]\]")


def get_links(page_contents):
    page = io.StringIO()
    for c in page_contents:
        page.write(c)
    page = page.getvalue()
    pa = re.findall("\[\[([^\[\]\|]+)(?:\|[^\]]+)?\]\]", page, flags=re.UNICODE)
    # print(pa)
    return pa


def text_from_node(node):
    """
    Przyjmuje węzeł XML i zwraca kawałki zawartego w nim tekstu
    """
    for ch in node.childNodes:
        if ch.nodeType == node.TEXT_NODE:
            yield ch.data

def links_counter (IN):
    links_count = defaultdict(lambda : 0)
    open_func = open
    if IN.endswith("bz2"):
        open_func = bz2.open
    with open_func(IN) as f:
        doc = parse(f)
        for event, node in doc:
            if event == START_ELEMENT and node.tagName == 'page':
                doc.expandNode(node)
                text = node.getElementsByTagName('text')[0]
                links = get_links(text_from_node(text))
                for m in links:
                    links_count[m]+=1

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
    test = links_counter("C:/zaj3/enwiki-20140903-pages-articles_part_2.xml.bz2")
    # print (test)
    save_to_file ("C:/wilico/count.csv",test)