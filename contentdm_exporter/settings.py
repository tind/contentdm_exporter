# Settings
# You can use the following URL to find the server number: https://mycontentdmsite.com/digital/api/diagnostics
# or https://mycontentdmsite.com/utils/diagnostics.
CDM_SERVER_NUMBER = "16923"

CDM_WEBSITE_URL = "https://societyofthecincinnati.contentdm.oclc.org/"

# Local path to save file
REL_PATH = "/Users/Demo/migration/my_project/"


# Collection alias
# If set to empty, the script will get the full list of collections and loop through all of them.
# Only set this variable if you plan to export a single collection.
# Used mainly for testing purposes. Example: ALIAS = "p17218coll3"
ALIAS = ""

# Use the config below if you want to export particular records from different collections.
# The format is a list of tuples with collection name and record IDs.
# Example: [('p15999coll1', '8'), ('yellowstone', '2')]
# To use this method, comment out the function run_batch()
# and uncomment the function run_export_list_of_records().
# MANUAL_EXPORT_LIST = [
#     ('p15999coll3', '8'),
#     ('yellowstone', '5845'),
#     ('BYUPhotos', '687'),
#     ('GEA', '8131'),
#     ('Jackson', '3422'),
#     ('MStar', '6590'),
#     ('p15999coll20', '37296'),
#     ('p15999coll22', '6664'),
#     ('p15999coll24', '9632'),
#     ('WomansExp', '2399')
# ]

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
EXPORT_PAGE_METADATA = True

# Do we like to export the page metadata in JSON?
# This will be in addition to exporting the page metadata in XML.
# EXPORT_PAGE_METADATA need to be set to 'True' to be able to export page metadata at all.
EXPORT_PAGE_METADATA_JSON = False