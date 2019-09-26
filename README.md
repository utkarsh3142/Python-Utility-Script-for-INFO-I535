# Python-Utility-Script-for-INFO-I535
This script is used in the course to download, extract, transform and load NOAA Storm events dataset into MongoDB.
It will also create two directories - landDir and extractDir in the script path.

Commands:

1. To see its usage, you can also use the following command: 
> python project_utilities.py help

2. To download the data, you need to specify a year range after the download keyword
> python project_utilities.py download <start year> <end year>

3. To extract the CSV files from GZ files into "extractDir" directory.
> python project_utilities.py extract

4. The data comes in the CSV format and needs to be converted to JSON to be loaded into MongoDB. We can also specify the columns to keep by prividing a comma separated list of columns as the argument. The argument “chunksize” allows to stage the transformation and work with very large files. Chunk size allows to specify a number of rows that will be transformed per file in one go. For example, a file with 100,000 rows will be transformed into 4 json files if chunk size is set to 25,000.
> python project_utilities.py <comma separated list of columns> <chunksize>

To keep all the columns, you can run the command as follows:
> python project_utilities.py transform <chunksize>

5. To load the data into MongoDB use below command:
> python project_utilities.py load <hostname> <port> <database> <collection> <username> <password> 

6. To cleanup directories - landDir and extractDir, use below command:
> python project_utilities.py cleanup <dirname> 
or to cleanup both
> python project_utilities.py cleanup
