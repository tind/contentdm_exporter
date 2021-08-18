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
This script loops through the input XML and find all files. It then query CONTENTdm and download
the files.
"""

import requests
from pathlib import Path
from defusedxml.lxml import parse

# Settings
FILE_URL = 'https://cdm16694.contentdm.oclc.org/utils/getfile/collection/'

ALIAS = ""

# The local path
REL_PATH = "/Users/Demo/migration/my_project/"

# Path to the folder where you'll find the input xml file(s).
MIG_INPUT_FOLDER = REL_PATH + 'output/'

# Path to the output polder where the downloaded files will be stored.
MIG_OUTPUT_FOLDER = REL_PATH + "Download/"


def get_all_records_from_file(file_path):
    xml = parse(str(file_path))
    collection = xml.getroot()
    return collection


def download_file(dmrecord, output_path, filename):
    """
    Export file from CONTENTdm
    filename is the parameter in the CONTENTdm query which defines the local file
    name. It has nothing to do with what the name is on the server.
    """
    local_file_name = Path(output_path, filename)

    # If the file already exists, skip downloading it again.
    if not local_file_name.is_file():
        download_url = FILE_URL + ALIAS + '/id/' + dmrecord + '/filename/' + filename
        # Download file
        try:
            req = requests.get(download_url, timeout=3600)
            if len(req.content) < 1000:
                if req.text == 'Requested item not found':
                    print('File does not exists. Record: ', dmrecord)
                    return 'Requested item not found'
            if req.status_code == 200:
                with open(str(local_file_name), 'wb') as f:
                    f.write(req.content)
                return True
            else:
                return False
        except requests.exceptions.Timeout as e:
            print('File download timeout: ', e)
            return e
        return False
    else:
        # TIND specific usage as we import the function from another script.
        return 'local'


def get_page_info(elem):
    has_pdfpage = False
    page_recid = ''
    pathinfo = None
    for sub_elem in elem:
        if sub_elem.tag == 'pagefile':
            if sub_elem.text.endswith('.pdfpage'):
                has_pdfpage = True
            else:
                pathinfo = Path(sub_elem.text)
        elif sub_elem.tag == 'pageptr':
            page_recid = sub_elem.text
    return has_pdfpage, page_recid, pathinfo


if __name__ == '__main__':
    # Loop through all files in path, except DS_Store (MacOS specific files).
    input_folder = Path(MIG_INPUT_FOLDER)
    for file_path in sorted(input_folder.glob('*.xml')):

        collection = get_all_records_from_file(file_path)
        # Loop through records
        for i, record in enumerate(collection):
            print(i)
            # Decide about local path to download files
            dmrecord = record.xpath('dmrecord')[0].text  # We could also have used cdmid

            output_path = Path(MIG_OUTPUT_FOLDER)
            if not output_path.is_dir():
                output_path.mkdir()

            output_path = Path(MIG_OUTPUT_FOLDER, ALIAS)
            if not output_path.is_dir():
                output_path.mkdir()
            output_path = Path(output_path, '{:06}'.format(int(dmrecord)))
            if not output_path.is_dir():
                output_path.mkdir()

            # Check if the record has an element "structure" with children
            structures = record.xpath('structure')
            if structures and len(structures[0]) > 0:
                download_pdf = False
                # First, let's check that it is only one element in structure
                if len(structures) > 1:
                    print('We have multiple structure elements! ', dmrecord)

                # We have children
                # if this is a pdf compound object with '.pdfpage' children we need to handle
                # things differently
                j = 1
                for elem in structures[0]:
                    if elem.tag == 'page':
                        has_pdfpage, page_recid, pathinfo = get_page_info(elem)
                        if has_pdfpage:
                            download_pdf = True
                        else:
                            # this is a normal compound object
                            filename = '{:06}_{:06}{}'.format(int(dmrecord),
                                                              j,
                                                              pathinfo.suffix)

                            download_file(page_recid, output_path, filename)
                            j += 1

                    if elem.tag == 'node':
                        for sub_elem in elem:
                            if sub_elem.tag == 'page':
                                has_pdfpage, page_recid, pathinfo = get_page_info(sub_elem)
                                if has_pdfpage:
                                    download_pdf = True
                                else:
                                    # this is a normal compound object
                                    filename = '{:06}_{:06}{}'.format(int(page_recid),
                                                                      j,
                                                                      pathinfo.suffix)

                                    download_file(page_recid, output_path, filename)
                                    j += 1
                            if sub_elem.tag == 'node':
                                for sub_sub_elem in sub_elem:
                                    if sub_sub_elem.tag == 'page':
                                        has_pdfpage, page_recid, pathinfo = get_page_info(sub_sub_elem)
                                        if has_pdfpage:
                                            download_pdf = True
                                        else:
                                            # this is a normal compound object
                                            filename = '{:06}_{:06}{}'.format(int(dmrecord),
                                                                              j,
                                                                              pathinfo.suffix)

                                            download_file(page_recid, output_path, filename)
                                            j += 1

                if download_pdf:
                    # use the parent dmrecord to get the full pdf
                    filename = '{:06}_{:06}{}'.format(int(dmrecord),
                                                      1,
                                                      '.pdf')

                    download_file(dmrecord, output_path, filename)

            else:
                # This is a single item
                pathinfo = Path(record.xpath('find')[0].text)

                extension = pathinfo.suffix

                filename = '{:06}_{:06}{}'.format(int(dmrecord),
                                                  1,
                                                  extension)

                download_file(dmrecord, output_path, filename)
