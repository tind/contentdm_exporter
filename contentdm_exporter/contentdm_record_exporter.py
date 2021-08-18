# Copyright (C) 2021 TIND.
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

"""
This script query a CONTENTdm collection for all bibliographic records (parent records).
It then loop through the records and query CONTENTdm for record metadata,
compound object metadata and (optional) bibliographic metadata stored on the file/page-level.
"""

import json
import requests
import math
from pathlib import Path
from defusedxml.lxml import (tostring,
                             fromstring)
from lxml.builder import E

# Settings

# URL to the CONTENTdm web services API.
MAIN_URL = 'https://server16694.contentdm.oclc.org/dmwebservices/index.php?q='

# Collection alias
ALIAS = ""

# Local path to save file
REL_PATH = "/Users/Demo/migration/my_project/"

# path to the output folder where you'll find the final xml file
MIG_OUTPUT_FOLDER = REL_PATH + "output/"

# Set num for progress_bar_chunks
NUM_PROGRESS_BAR_CHUNKS = 50

#  Don't change CHUNK_SIZE unless CONTENTdm is timing out.
CHUNK_SIZE = 100

# Don't change $start_at unless from 1 you are exporting a range of records. If you
# want to export a range, use the number of the first record in the range.
START_AT = 1

# The last record in subset, not the entire record set. Don't change LAST_REC from 0
# unless you are exporting a subset of records. If you want to export a range, use the
# number of records in the subset, e.g., if you want to export 200 records, use that value.
LAST_REC = 0

# Do we like to export the page metadata?
# Exporting the page metadata will increase the time to do the export.
# A second approach is to do it in a separate script to avoid slowing down the core export.
EXPORT_PAGE_METADATA = False

# Other variables used by the script.
compound_file_metadata = {}  # Export page metadata in a JSON file.


rec_num = 0  # Record counter


# Create a query map
# We query for as little possible info at this point since we'll be doing another query
# on each item later.

# We only want parent-level items, not pages/children. It appears that changing 'suppress'
# to 0, as documented, has no effect anyway.
query_map = {
    'alias': ALIAS,
    'searchstrings': '0',
    'fields': 'dmcreated',
    'sortby': 'dmcreated!dmrecord',
    'maxrecs': CHUNK_SIZE,
    'start': START_AT,
    'supress': 1,
    'docptr': 0,
    'suggest': 0,
    'facets': 0,
    'format': 'json'}


def query_contentdm(start_at,
                    current_chunk=None,
                    num_chunks=None):
    """
    Query CONTENTdm with the values in $query_map and return an array of records.
    """

    query_url = '{main_url}dmQuery/{alias}/{searchstrings}/{fields}/{sortby}/{maxrecs}/{start_at}/{docptr}/{suggest}/{facets}/{format}'.format(
        main_url=MAIN_URL,
        alias=query_map['alias'],
        searchstrings=query_map['searchstrings'],
        fields=query_map['fields'],
        sortby=query_map['sortby'],
        maxrecs=query_map['maxrecs'],
        start_at=start_at,
        supress=query_map['supress'],
        docptr=query_map['docptr'],
        suggest=query_map['suggest'],
        facets=query_map['facets'],
        format=query_map['format'])

    # Query CONTENTdm and return records; if failure, log problem.
    try:
        req = requests.get(query_url)
        items = json.loads(req.content)
    except:
        items = []
        pass

    return items


# Perform a preliminary query to determine how many records are in the current collection,
# and to determine the number of queries required to get all the records.
prelim_results = query_contentdm(START_AT)

# We add one chunk, then round down using sprintf().
print('Total number of records in collection: ', prelim_results['pager']['total'])
num_chunks = prelim_results['pager']['total'] / CHUNK_SIZE + 1
num_chunks = math.floor(num_chunks)

# Die if there are no records.
if not prelim_results['pager']['total']:
    exit()


def get_compound_object_info(alias, pointer, format='json'):
    """
    Gets the item's compound info. "code" contains '-2' if the item is not compound.
    The alias starts with a slash, which is stripped away to keep the query url as
    simple to read as possible.
    """
    if alias.startswith('/'):
        alias = alias[1:]

    if format == 'json':
        query_url = MAIN_URL + 'dmGetCompoundObjectInfo/' + alias + '/' + pointer + '/json'
        req = requests.get(query_url)
        compound_info = json.loads(req.content)
    elif format == 'xml':
        query_url = MAIN_URL + 'dmGetCompoundObjectInfo/' + alias + '/' + pointer + '/xml'
        req = requests.get(query_url)
        compound_info = req.text
    return compound_info


def process_compound_object(compound_info):
    """
    Processes each record in the browse results.
    """
    cpd = fromstring(compound_info.encode('utf-8'))

    has_type = False
    for elem in cpd:
        if elem.tag == 'type':
            has_type = True

    if has_type:
        string_positon = compound_info.index('?>') + 2
        compound_info = compound_info[string_positon:]

        return compound_info

    return ""


def get_item_info(alias, item_number, format='xml'):
    """
    Get the item information. Item can be parent record metadata or file/page metadata.
    """
    if alias.startswith('/'):
        alias = alias[1:]
    if format == 'xml':
        query_url = MAIN_URL + 'dmGetItemInfo/' + alias + '/' + item_number + '/xml'
        req = requests.get(query_url)
        item = req.text
        string_positon = item.index('?>') + 2
        item = item[string_positon:]
    elif format == 'json':
        query_url = MAIN_URL + 'dmGetItemInfo/' + alias + '/' + item_number + '/json'
        req = requests.get(query_url)
        item = json.loads(req.content)
    return item


def add_file_level_information(elem, results_record):
    global compound_file_metadata
    file_level_id = None
    for page_elem in elem:
        if page_elem.tag == 'pageptr':
            file_level_id = page_elem.text

    # Add file metadata inside the compound object
    if file_level_id:
        file_level_info = get_item_info(results_record['collection'],
                                        file_level_id,
                                        format='xml')
        file_level_xml = fromstring(file_level_info)
        # Strip away fields if no text or no child elements.
        pagemetadata = E.pagemetadata()
        for field in file_level_xml:
            if field.text or len(field) > 0:
                pagemetadata.append(field)
        if len(pagemetadata) > 0:
            # Append the page metadata to the page element.
            elem.append(pagemetadata)
    else:
        print('Compound object has no file level ID (pageptr)', tostring(elem))

    # Add the file metadata to a separate file (JSON). Use pageptr as the key
    if file_level_id:
        file_level_info_json = get_item_info(results_record['collection'],
                                             file_level_id,
                                             format='json')
        # Strip away empty values
        updated_file_level_info = {}
        for key, val in file_level_info_json.items():
            if val:
                updated_file_level_info[key] = val
        if updated_file_level_info:
            compound_file_metadata[file_level_id] = updated_file_level_info

    else:
        print('Compound object has no file level ID (pageptr)', tostring(elem))


def save_output_xml_to_file(collection, processed_chunks):

    # Create output folder if it does not exists.
    output_path = Path(MIG_OUTPUT_FOLDER)
    if not output_path.is_dir():
        output_path.mkdir()

    local_file_name = Path(output_path,
                           '{}_structure_{:03}.xml'.format(ALIAS,
                                                           processed_chunks))
    with open(str(local_file_name), 'wb') as xmlfile:
        xmlfile.write(tostring(collection, pretty_print=True, encoding='utf-8'))


def run_batch(total_recs, num_chunks, start_at):
    global rec_num
    global compound_file_metadata

    print("Retrieving structural file for the %s collection..." % (ALIAS,))

    processed_chunks = 1
    while processed_chunks <= num_chunks:
        # For each chunk, create a new collection xml object.
        collection = E.collection()
        print('Start at: ', start_at)

        # Query CONTENTdm for all records in a collection for the defined chunk.
        results = query_contentdm(start_at, processed_chunks, num_chunks)
        if not results:
            print("Could not connect to CONTENTdm to start retrieving chunk starting at: ",
                  start_at)
            exit()
        start_at = CHUNK_SIZE * processed_chunks + 1

        # Loop through each record in the processed chunk.
        for results_record in results['records']:
            rec_num += 1
            print(rec_num)

            # Create a new xml record object.
            record = E.record()

            # Append CONTENTdm record ID to new record object.
            cdmid = E.cdmid()
            cdmid.text = str(results_record['pointer'])
            record.append(cdmid)

            # Get bibliographic record metadata
            bib_info = get_item_info(results_record['collection'],
                                     str(results_record['pointer']),
                                     format='xml')

            # Append each field to the new record object.
            bib_xml = fromstring(bib_info)
            for field in bib_xml:
                record.append(field)

            # Get the records compound information.
            compound_info = get_compound_object_info(results_record['collection'],
                                                     str(results_record['pointer']),
                                                     'xml')
            compound_info = process_compound_object(compound_info)
            if compound_info:
                compound_xml = fromstring(compound_info)
                compound_xml.tag = 'structure'
                # Compound objects can contain metadata for each page.
                # Get the page metadata and store it inside the page object and as
                # a separate file (JSON). We can consider doing this in a separate
                # script to save some time exporting the main records.
                if EXPORT_PAGE_METADATA:
                    # Loop through the compound object and find each page.
                    # # Append the page metadata to the page element.
                    for elem in compound_xml:
                        if elem.tag == 'page':
                            add_file_level_information(elem, results_record)
                        if elem.tag == 'node':
                            for sub_elem in elem:
                                if sub_elem.tag == 'page':
                                    add_file_level_information(sub_elem, results_record)
                                if sub_elem.tag == 'node':
                                    for sub_sub_elem in sub_elem:
                                        if sub_sub_elem.tag == 'page':
                                            add_file_level_information(sub_sub_elem,
                                                                       results_record)
                # Append the compound object to the record
                record.append(compound_xml)

            # Append the record to the collection
            collection.append(record)
            if LAST_REC != 0:
                if rec_num == LAST_REC:
                    save_output_xml_to_file(collection, processed_chunks)

                    # To get out of the while loop, make
                    # processed_chunks higher than num_chunks.
                    processed_chunks = num_chunks + 1
                    break

        save_output_xml_to_file(collection, processed_chunks)

        processed_chunks += 1

    if EXPORT_PAGE_METADATA:
        with open(str(Path(MIG_OUTPUT_FOLDER, 'compound_file_metadata.json')), 'w') as f:
            f.write(json.dumps(compound_file_metadata))


if __name__ == '__main__':
    run_batch(prelim_results['pager']['total'], num_chunks, START_AT)
