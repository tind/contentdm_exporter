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
from logger import setup_logger

# Settings
# You can use the following URL to find the server number: hhttps://mycontentdmsite.com/digital/api/diagnostics
# or https://mycontentdmsite.com/utils/diagnostics.
CDM_SERVER_NUMBER = '17218'


# Collection alias
# ALIAS = "p17083coll7"
# ALIAS = "p17218coll2"
# Aliases is not used anymore, since I first query and loop through a list of aliases.

# Local path to save file
# REL_PATH = "/Users/Demo/migration/my_project/"
REL_PATH = "/home/ubuntu/migration/USI/da/"


# path to the output folder where you'll find the final xml file
OUTPUT_FOLDER = REL_PATH + "output/"

#  Don't change CHUNK_SIZE unless CONTENTdm is timing out.
CHUNK_SIZE = 100

# Don't change $start_at from 1 unless you are exporting a range of records. If you
# want to export a range, use the number of the first record in the range.
START_AT = 1

# The last record in subset, not the entire record set. Don't change LAST_REC from 0
# unless you are exporting a subset of records. If you want to export a range, use the
# number of records in the subset, e.g., if you want to export 200 records, use that value.
LAST_REC = 0

# Do we like to export the page metadata?
# Exporting the page metadata will increase the time to do the export.
# A second approach is to do it in a separate script to avoid slowing down the core export.
EXPORT_PAGE_METADATA = True

# Other variables used by the script.
# URL to the CONTENTdm web services API.
# MAIN_URL = 'https://server16944.contentdm.oclc.org/dmwebservices/index.php?q='
MAIN_URL = 'https://server{}.contentdm.oclc.org/dmwebservices/index.php?q='.format(CDM_SERVER_NUMBER)

rec_num = 0  # Record counter


def query_contentdm(query_map):
    """
    Query CONTENTdm with the values in $query_map and return an array of records.
    """

    # Create a query map
    # We query for as little possible info at this point since we'll be doing another query
    # on each item later.

    # We only want parent-level items, not pages/children. It appears that changing 'suppress'
    # to 0, as documented, has no effect anyway.

    query_url = '{main_url}dmQuery/{alias}/{searchstrings}/{fields}/{sortby}/{maxrecs}/{start_at}/{docptr}/{suggest}/{facets}/{format}'.format(
        main_url=MAIN_URL,
        alias=query_map['alias'],
        searchstrings=query_map['searchstrings'],
        fields=query_map['fields'],
        sortby=query_map['sortby'],
        maxrecs=query_map['maxrecs'],
        start_at=query_map['start_at'],
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


def get_list_of_collection_aliases():
    """
    Query CONTENTdm and return a list of collecton aliases
    """

    # GET collections
    query_url_collections = 'https://server{}.contentdm.oclc.org/dmwebservices/index.php?q=dmGetCollectionList/json'.format(CDM_SERVER_NUMBER)

    # Query CONTENTdm and return records; if failure, log problem.
    try:
        res = requests.get(query_url_collections)
        items = res.json()
    except:
        items = []
        pass

    # Create a list of aliases. Remove first letter which is a slash.
    return sorted([collection.get('alias')[1:] for collection in items])


def get_number_of_records_in_collection(alias, start_at):
    query_map = {
        'alias': alias,
        'searchstrings': '0',
        'fields': 'dmcreated',
        'sortby': 'dmcreated!dmrecord',
        'maxrecs': CHUNK_SIZE,
        'start_at': start_at,
        'supress': 1,
        'docptr': 0,
        'suggest': 0,
        'facets': 0,
        'format': 'json'}
    # Perform a preliminary query to determine how many records are in the current collection,
    # and to determine the number of queries required to get all the records.
    prelim_results = query_contentdm(query_map)

    return prelim_results['pager']['total']


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
    file_metadata = {}
    file_level_id = None
    for page_elem in elem:
        if page_elem.tag == 'pageptr':
            file_level_id = page_elem.text

    if file_level_id:
        # Add file metadata inside the compound object.
        # Use pageptr (file_level_id) as the key.
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

        # Add the file metadata to a separate file (JSON).
        file_level_info_json = get_item_info(results_record['collection'],
                                             file_level_id,
                                             format='json')
        # Strip away empty values
        updated_file_level_info = {}
        for key, val in file_level_info_json.items():
            if val:
                updated_file_level_info[key] = val
        if updated_file_level_info:
            file_metadata[file_level_id] = updated_file_level_info

    else:
        logger.warning("Compound object has no file level ID (pageptr): %s" % (tostring(elem),))

    return file_metadata


def save_output_xml_to_file(collection, alias, processed_chunks):

    # Create output folder if it does not exists.
    output_path = Path(OUTPUT_FOLDER)
    if not output_path.is_dir():
        output_path.mkdir()

    local_file_name = Path(output_path,
                           'cdexport_{}_{:03}.xml'.format(alias,
                                                          processed_chunks))
    with open(str(local_file_name), 'wb') as xmlfile:
        xmlfile.write(tostring(collection, pretty_print=True, encoding='utf-8'))


def run_batch():
    global rec_num

    # Get the list of collections by their aliases.
    collection_aliases = get_list_of_collection_aliases()

    for alias in collection_aliases:

        # We need to restart the start_at number for each collection.
        start_at = START_AT

        # Get the number of records in a collection and calculate the number of chunks.
        nb_records_in_collection = get_number_of_records_in_collection(alias, start_at)

        # We add one chunk, then round down using sprintf().
        print('Total number of records in collection {} is: {}'.format(alias, nb_records_in_collection))
        logger.info("Total number of records in collection: %s is ‰s" % (alias, nb_records_in_collection))
        num_chunks = nb_records_in_collection / CHUNK_SIZE + 1
        num_chunks = math.floor(num_chunks)

         # Go to the next collection if there are no records.
        if not nb_records_in_collection:
            continue

        compound_file_metadata = {}  # Export page metadata in a JSON file.

        print("Retrieving structural file for the %s collection..." % (alias,))

        processed_chunks = 1
        while processed_chunks <= num_chunks:
            # For each chunk, create a new collection xml object.
            collection = E.collection()
            print('Start at: ', start_at)

            query_map = {
            'alias': alias,
            'searchstrings': '0',
            'fields': 'dmcreated',
            'sortby': 'dmcreated!dmrecord',
            'maxrecs': CHUNK_SIZE,
            'start_at': start_at,
            'supress': 1,
            'docptr': 0,
            'suggest': 0,
            'facets': 0,
            'format': 'json'}

            # Query CONTENTdm for all records in a collection for the defined chunk.
            results = query_contentdm(query_map)
            if not results:
                print("No records was found. Could not connect to CONTENTdm to start retrieving chunk starting at: ",
                    start_at)
                logger.warning("No records was found. Could not connect to CONTENTdm to start retrieving chunk starting at: %s" % (start_at,))
                continue

            # We are preparing the "start_at" number we will use in the next chunk.
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
                                compound_file_metadata.update
                                (add_file_level_information(elem, results_record))
                            if elem.tag == 'node':
                                for sub_elem in elem:
                                    if sub_elem.tag == 'page':
                                        compound_file_metadata.update(
                                            add_file_level_information(sub_elem, results_record))
                                    if sub_elem.tag == 'node':
                                        for sub_sub_elem in sub_elem:
                                            if sub_sub_elem.tag == 'page':
                                                compound_file_metadata.update(
                                                    add_file_level_information(sub_sub_elem, results_record))
                    # Append the compound object to the record
                    record.append(compound_xml)

                # Append the record to the collection
                collection.append(record)
                if LAST_REC != 0:
                    if rec_num == LAST_REC:
                        save_output_xml_to_file(collection, alias, processed_chunks)

                        # To get out of the while loop, make
                        # processed_chunks higher than num_chunks.
                        processed_chunks = num_chunks + 1
                        break

            save_output_xml_to_file(collection, alias, processed_chunks)

            processed_chunks += 1

        if EXPORT_PAGE_METADATA:
            with open(str(Path(OUTPUT_FOLDER, 'compound_file_metadata_{}.json'.format(alias))), 'w') as f:
                f.write(json.dumps(compound_file_metadata))


# def create_list_of_records(total_recs, num_chunks, start_at):
#     all_records = []
#     global rec_num

#     print("Retrieving structural file for the %s collection..." % (ALIAS,))

#     processed_chunks = 1
#     while processed_chunks <= num_chunks:
#         # For each chunk, create a new collection xml object.
#         print('Start at: ', start_at)

#         # Query CONTENTdm for all records in a collection for the defined chunk.
#         results = query_contentdm(start_at)
#         if not results:
#             print("Could not connect to CONTENTdm to start retrieving chunk starting at: ",
#                   start_at)
#             exit()
#         start_at = CHUNK_SIZE * processed_chunks + 1

#         # Loop through each record in the processed chunk.
#         for results_record in results['records']:
#             rec_num += 1
#             print(rec_num)

#             # Create a new xml record object.
#             record = {}

#             # Append CONTENTdm record ID to new record object.
#             record['cdmid'] = str(results_record['pointer'])

#             # Get the records compound information.
#             compound_info = get_compound_object_info(results_record['collection'],
#                                                      str(results_record['pointer']),
#                                                      'json')

#             record['pages'] = [p.get('pageptr') for p in compound_info.get('page', []) if p.get('pageptr')]
#             all_records.append(record)

#         processed_chunks += 1

#     with open(str(Path(OUTPUT_FOLDER, 'all_records.json')), 'w') as f:
#         f.write(json.dumps(all_records))

#     return all_records


if __name__ == '__main__':
    logger = setup_logger(str(Path(OUTPUT_FOLDER, 'record_export')))
    run_batch()

    # all_records = create_list_of_records(prelim_results['pager']['total'], num_chunks, START_AT)
    # print(len(all_records))
