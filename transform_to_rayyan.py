#!/usr/bin/env python3
# Changes based on 202003207 release
# Need to write something here ...
# Changes based on 202003207 release
# Change has_full_text --> has_pdf_parse
# Add has_pmc_xml_parse and cord_uid to notes
# Changes based on 20200519 release
# +s2_id column
# Changes based on 20200512 release
#+mag_id,who_covidence_id,pdf_json_files,pmc_json_files
#-Microsoft Academic Paper ID,WHO #Covidence,has_pdf_parse,has_pmc_xml_parse,full_text_file
# Changes based on 202003207 release
# Need to write something here ...
# Changes based on 202003207 release
# Change has_full_text --> has_pdf_parse
# Add has_pmc_xml_parse and cord_uid to notes

USAGE = '''\
Converts open COVID-19 dataset to rayyan compatible form
usage: python transform_to_rayyan.py <input_file.csv> <output_file_prefix>\
'''

import csv
import sys
import ast
import numpy
import itertools

import multiprocessing as mp

from math import ceil
from dateparser import parse as normalparse
from daterangeparser import parse as rangeparse

if len(sys.argv) != 3:
  print(USAGE)
  print("Incorrect number of arguments!")
  exit(1)

#DX_DOI_PREFIX = 'http://dx.doi.org/'

NUM_CORES = 8
NUM_LINES_TO_PROCESS_PER_CHUNK = 50000
OUTPUT_FIELDS = ['title', 'abstract', 'url', 'pmc_id', 'pubmed_id', 'year', 'month', 'day', 'authors', 'journal', 'notes']

INPUT_FILE = sys.argv[1]
OUTPUT_PREFIX = sys.argv[2]

def transform_row_to_rayyan(irow):
    orow = {}

    orow['title'] = irow['title']
    orow['abstract'] = irow['abstract']
    #orow['url'] = DX_DOI_PREFIX + irow['doi']
    orow['url'] = irow['url']
    orow['pmc_id'] = irow['pmcid']
    orow['pubmed_id'] = irow['pubmed_id']

    publish_time = irow['publish_time'].strip()
    try:
      # First, try parsing as a daterange.
      # This should catch most date formats except
      # those in 'DD-MM-YY' and some other forms.
      start, end = rangeparse(publish_time)
    except:
      # If parsing as daterange fails, select
      # the first word. It's usually 'YYYY' or 'DD-MM-YY'
      # which is good enough.
      publish_time = publish_time.split(' ')[0]

    if publish_time:
      try:
        # Try another parse as daterange
        start, end = rangeparse(publish_time)
      except:
        # If that fails, then it is ''DD-MM-YY',
        # which can be picked up by normalparse.
        start = normalparse(publish_time)

      orow['year'] = start.year
      orow['month'] = start.month
      orow['day'] = start.day
    else:
      orow['year'] = ''
      orow['month'] = ''
      orow['day'] = ''

    # Inital dataset had authors in a list form.
    # Try parsing authors to see if it's a list.
    try:
      authors = ast.literal_eval(irow['authors'])
      if type(authors) == list:
        orow['authors'] = '; '.join(authors)
      else:
        raise RuntimeError
    except:
      # It's not a list, use the string as is.
      orow['authors'] = irow['authors']

    orow['journal'] = irow['journal']

    notes = []
    for col in ['cord_uid', 'sha', 'doi', 'source_x', 'license', 'mag_id', 'who_covidence_id', 'pdf_json_files','pmc_json_files','s2_id']:
      notes.append(col + ': ' + irow[col])

    orow['notes'] = '; '.join(notes)
    return orow

def worker(queue, name):
  print('%s starting...' % name)

  while True:
    item = queue.get()

    if item is None:
      # No more items, stop worker
      print('%s is done, shutting down...' % name)
      queue.task_done()
      break

    # Process input chunk
    input_index = item
    print('%s: processing chunk %s' % (name, input_index))
    input_csv = csv.DictReader(open(INPUT_FILE, 'r', encoding='utf-8', errors='ignore'), delimiter=',')

    line_to_start = input_index * NUM_LINES_TO_PROCESS_PER_CHUNK
    line_to_stop = (input_index + 1) * NUM_LINES_TO_PROCESS_PER_CHUNK

    # Write to output
    with open(OUTPUT_PREFIX + str(input_index) + '.csv', "w+") as output_file:
      output_csv = csv.DictWriter(output_file, delimiter=',', fieldnames=OUTPUT_FIELDS)
      output_csv.writerow(dict((field, field) for field in OUTPUT_FIELDS)) # Write header

      for line in itertools.islice(input_csv, line_to_start, line_to_stop):
        output_csv.writerow(transform_row_to_rayyan(line))

    queue.task_done()

if __name__ == "__main__":
  input_csv = csv.DictReader(open(INPUT_FILE, 'r', encoding='utf-8', errors='ignore'), delimiter=',')

  # Get the number of lines in the input csv to distribute work across cores.
  # This does it without reading the whole thing into memory.
  total_input_lines = sum(1 for row in input_csv)
  num_output_files =  ceil(total_input_lines / NUM_LINES_TO_PROCESS_PER_CHUNK)

  print('Number of output files that will be created: %s' % num_output_files)

  # Create a queue with the indices of the chunk of the input file to process.
  work_queue = mp.JoinableQueue()
  for output_index in range(num_output_files):
    work_queue.put(output_index)

  procs = []
  # Create worker processes
  for i in range(NUM_CORES):
    proc = mp.Process(target=worker, args=(work_queue, 'worker_' + str(i)))
    proc.daemon = True
    proc.start()
    procs.append(proc)

  work_queue.join()

  for proc in procs:
    # Send a sentinel to terminate worker
    work_queue.put(None)

  work_queue.join()

  for proc in procs:
    proc.join()

  print("Complete.")
