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
from parsons import MySQL, GoogleCloudStorage

# Setup

# if variables specified above, sets themas environmental variables
for name, value in config_vars.items():
    if value.strip() != "":
        os.environ[name] = value

# //To Script Writer// : instantiate connectors here, eg: rs = Redshift().
mysql = MySQL()
gcs = GoogleCloudStorage()


# Code

with mysql.connection() as conn:

  sql = "SELECT count(*) FROM core_user"

  # mysql.query(sql, parameters=None).to_csv(local_path="./results.csv", temp_file_compression=None, encoding=None, errors='strict', write_header=True, csv_name=None,)

payload = mysql.query(sql, parameters=None)

# gcs.upload_table(bucket='dc_parsons_test', table=payload, blob_name='parsons_blob')
gcs.upload_table(payload, 'dc_parsons_test', 'parsons_blob', data_type='csv', default_acl=None)

