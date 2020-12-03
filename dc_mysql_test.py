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
}


# ### CODE

# //To Script Writer//: import any other packages your script uses
import os  # noqa: E402

# //To Script Writer//: import any connectors your script uses
# from parsons import utilities, logger
from parsons import MySQL

# Setup

# if variables specified above, sets themas environmental variables
for name, value in config_vars.items():
    if value.strip() != "":
        os.environ[name] = value

# //To Script Writer// : instantiate connectors here, eg: rs = Redshift().
mysql = MySQL()

# Code
#save a full table to .csv
with mysql.connection() as conn:
  core_tag_table = mysql.query("SELECT * FROM core_tag", parameters=None)

core_tag_table.to_csv(local_path="./core_tag.csv", temp_file_compression=None, encoding=None, errors='strict', write_header=True, csv_name=None,)

  

