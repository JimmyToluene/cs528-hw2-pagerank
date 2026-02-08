import re
import os

def parse_html(text):
    """Pure parsing logic — no I/O, easy to test"""
    links = re.findall(r'<a HREF="(\d+)\.html"', text)
    return links

def read_local_files(directory):
    """Read from local directory"""
    outgoing = {}
    for fname in os.listdir(directory):
        if fname.endswith('.html'):
            page_id = fname.replace('.html', '')
            with open(os.path.join(directory, fname), 'r') as f:
                text = f.read()
            outgoing[page_id] = parse_html(text)
    return outgoing

def display_outgoing(outgoing, limit=50000):
    """Display first few entries for sanity check"""
    for i, (page_id, links) in enumerate(sorted(outgoing.items(), key=lambda x: int(x[0]))):
        if i >= limit:
            break
        print(f"Page {page_id}: {len(links)} outgoing links → {links}")


outgoing = read_local_files("./generated_htmls")  # wherever you generated the files
display_outgoing(outgoing)