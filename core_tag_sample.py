# ### METADATA

# Connectors: 
# Description:

# ### CONFIGURATION

# Set the configuration variables below or set environmental variables of the same name and leave
# these with empty strings.  We recommend using environmental variables if possible.

# //To Script Writer// : add the environmental variable name but not the value
# //To Script Writer// : separate environmental variables by connector

config_vars = {
    # mySQL:
    "MYSQL_USERNAME": "",
    "MYSQL_PASSWORD": "",
    "MYSQL_HOST": "",
    "MYSQL_DB": "",
    "MYSQL_PORT": "",
    # google:
    "GOOGLE_APPLICATION_CREDENTIALS": ""
}


# ### CODE

# //To Script Writer//: import any other packages your script uses
import os  # noqa: E402

# //To Script Writer//: import any connectors your script uses
# from parsons import utilities, logger
from parsons import MySQL, GoogleCloudStorage, GoogleBigQuery

# Setup

# if variables specified above, sets themas environmental variables
for name, value in config_vars.items():
    if value.strip() != "":
        os.environ[name] = value

# //To Script Writer// : instantiate connectors here, eg: rs = Redshift().
mysql = MySQL()
gcs = GoogleCloudStorage()
big_query = GoogleBigQuery()

# Code
#save a full table to .csv
#i don't understand the with connection part: https://move-coop.github.io/parsons/html/databases.html#parsons.MySQL.connection
with mysql.connection() as conn:
  core_tag_table = mysql.query("SELECT * FROM core_tag", parameters=None)

# core_tag_table.to_csv(local_path="./core_tag.csv", temp_file_compression=None, encoding=None, errors='strict', write_header=True, csv_name=None,)

#Google Cloud Storage (gcs): https://move-coop.github.io/parsons/html/google.html#cloud-storage

#save to gcs, in the parson's test bucket as core_tag_blob
#set upload_table variables

#table = core_tag_table
#bucket_name = 'dc_parsons_test'
#blob_name = 'core_table_blob'

#upload table to gcs using process from GCS
#gcs.upload_table(table, bucket_name, blob_name, data_type='csv', default_acl=None)

#uploading table to gcs using to_gcs_csv from https://move-coop.github.io/parsons/html/table.html#parsons.etl.tofrom.ToFrom.to_gcs_csv
# core_tag_table.to_gcs_csv(bucket_name, blob_name, compression=None, encoding=None, errors='strict', write_header=True, public_url=False,)
#getting this error - using method above for now - AttributeError: 'Table' object has no attribute 'to_gcs_csv'


#move core_table from gcs to big_query - copy(table_obj, table_name, if_exists='fail', tmp_gcs_bucket=None, gcs_client=None, job_config=None, **load_kwargs)
#the above gcs code isn't needed, the the big query class takes care of it

#set copy variables
dataset = 'dreamcorps:dreamcorps'
table_obj = core_tag_table
table_name = 'core_tag'

big_query.copy(table_obj, table_name, if_exists='truncate', tmp_gcs_bucket='dc_parsons_test', gcs_client=gcs, job_config=None)


  

