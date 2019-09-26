### This utilities script was created by Utkarsh Kumar for the IU course I535 Management, Access and Use of Big and Complext Data
### The script is distributed under the GPL 3.0 license (http://www.gnu.org/licenses/gpl-3.0.html)
### You are free to run, study, share and modify this script.

import logging
import os
import sys
import gzip
import shutil
import subprocess
import json
import getpass
import pymongo
import pip
import csv
from collections import OrderedDict
import pandas as pd


# Get python version
ver_info = sys.version_info

if ver_info.major == 3:
        import urllib.request as r
elif ver_info.major == 2:
        import urllib as r
else:
        logger.error("Unsupported python version. Please use python major version 2 or 3")
        exit


# Get full path of directory
script_path = os.path.dirname(os.path.realpath(__file__))

# Configure logging to be displayed with stdout
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class utilities():
        def init(self):
                self.script_path = script_path
                self.url = 'https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/'
                self.land_dir = self.script_path + '/landDir/'
                self.extract_dir = self.script_path + '/extractDir/'

                logger.info("Python Major Version : " + str(ver_info.major))
                logger.info("Script Path: " + self.script_path)
                logger.info("URL : " + self.url)

                if not os.path.isdir(self.land_dir):
                        logger.info("Landing Directory does not exist. Creating directory")
                        try:
                                os.mkdir(self.land_dir)
                                logger.info("Landing Directory created.")
                        except:
                                logger.error("Landing Directory creation failed.")

                if not os.path.isdir(self.extract_dir):
                        logger.info("Extraction Directory does not exist. Creating directory")
                        try:
                                os.mkdir(self.extract_dir)
                                logger.info("Extraction Directory created.")
                        except:
                                logger.error("Extraction Directory creation failed.")

                logger.info("Landing Directory : " + self.land_dir)
                logger.info("Extraction Directory : " + self.extract_dir)

        def download(self, start, end):
                """
                Function to download storm events CSV files from
                NCDC website. These files will be downloaded to
                the directory provided by the input parameter.
                Input: start,end - start and end year for files to be downloaded, inclusive
                Output: Error Code
                """

                logger.info("*********************************************************************")
                logger.info("************** Beginning file download module ***********************")
                logger.info("*********************************************************************")

                error_flag = 0

                start = int(start)
                end = int(end)

                files_dict = {}
                files_list = []

                if not start or not end:
                        logger.error("Start or End year not specified")
                        error_flag = 1
                elif start > end:
                        logger.error("Start year is greater than End year")
                        error_flag = 1
                else:
                        with open(self.script_path + '/fileslist') as f:
                                lines = f.read().splitlines()
                        for line in lines:
                                values = line.split(",")
                                files_dict[int(values[0])] = values[1].strip()

                        while start <= end:
                                files_list.append(files_dict[start])
                                start += 1

                for file in files_list:
                        logger.info('Downloading file ' + file)
                        url = self.url + file
                        file_path = self.land_dir + file
                        try:
                                r.urlretrieve(url, file_path)
                                logger.info("Successfully downloaded " + file_path)

                        except urllib.error.URLError as e:
                                error_flag = 1
                                response_data = e.read().decode("utf8", 'ignore')
                                logger.error("Error downloading file " + file + " : " + response_data)

                logger.info("*********************************************************************")
                logger.info("***************** End of file download module ***********************")
                logger.info("*********************************************************************")


                return error_flag

        def extract(self):
                """
                Function to extract the downloaded gz files
                into the extraction directory. These files are
                of CSV format.
                Input: None
                Output: Error Code
                """

                logger.info("*********************************************************************")
                logger.info("*************** Beginning file extract module ***********************")
                logger.info("*********************************************************************")

                error_flag = 0

                files_list = os.listdir(self.land_dir)
                if len(files_list) > 0:
                        for file in files_list:
                                file_path = self.land_dir + file
                                extract_path = self.extract_dir + file.rsplit('.', 1)[0]
                                logger.info('Extracting file ' + file_path)
                                try:

                                        with open(extract_path, 'wb') as f_out, gzip.open(file_path, 'rb') as f_in:
                                                shutil.copyfileobj(f_in, f_out)
                                                logger.info("Successfully extracted file " + file)

                                except subprocess.CalledProcessError as e:
                                        logger.error("Error extracting file " + file + " : " + e.output)
                                        error_flag = 1

                else:
                        logger.error("Directory is empty")
                        error_flag = 1

                logger.info("*********************************************************************")
                logger.info("****************** End of file extract module ***********************")
                logger.info("*********************************************************************")


                return error_flag


        def cleanup(self, dirname):
                """
                Function to cleanup Landing and Extraction directories.
                Input: Dirname - directory to be cleaned
                Output: Error Code / Extracted data saved into extraction folder
                """
                logger.info("*********************************************************************")
                logger.info("*************** Beginning files cleanup module **********************")
                logger.info("*********************************************************************")

                error_flag = 0

                if dirname == 'load' or dirname == '':

                        logger.info("Removing files from landing directory")

                        land_filelist = [ f for f in os.listdir(self.land_dir)]

                        if land_filelist:
                                for filename in land_filelist:
                                        try:
                                                os.remove(os.path.join(self.land_dir, filename))
                                        except:
                                                logger.error("Error removing file " + filename)
                                                error_flag = 1


                if dirname == 'extract' or dirname == '':

                        logger.info("Removing files from extraction directory")

                        extract_filelist = [ f for f in os.listdir(self.extract_dir)]

                        if extract_filelist:
                                for filename in extract_filelist:
                                        try:
                                                os.remove(os.path.join(self.extract_dir, filename))
                                        except:
                                                logger.error("Error removing file " + filename)
                                                error_flag = 1


                logger.info("*********************************************************************")
                logger.info("****************** End of file cleanup module ***********************")
                logger.info("*********************************************************************")


                return error_flag

        def transform(self,columns,chunksize):
                """
                Function to transform the data from CSV into JSON after selecting
                the user input columns.
                Input: Comma separated list of columns
                Output: Error code / Transformed data saved into extraction folder
                """

                logger.info("*********************************************************************")
                logger.info("*************** Beginning transform data module *********************")
                logger.info("*********************************************************************")

                error_flag = 0

                chunksize = int(chunksize)

                files_list = os.listdir(self.extract_dir)

                if columns:
                        columns = columns.split(',')

                for file_ in files_list:

                        if file_.endswith(".csv"):

                                logging.info("Transforming file " + file_)
                                file_path = self.extract_dir + file_
                                chunk_num = 0
                                for chunk in pd.read_csv(file_path, low_memory=False, iterator=True, chunksize=chunksize):
                                        if not columns:
                                                data_cols = chunk
                                        else:
                                                data_cols = chunk[columns]
                                        filename = file_ + str(chunk_num)
                                        data_json = data_cols.T.apply(lambda row: row[~row.isnull()].to_json())
                                        data_json_ = "[%s]" % ",".join(data_json)

                                        try:
                                                with open(self.extract_dir + filename + '.json', 'w') as f:
                                                        #json.dump(data_json_, f)
                                                        f.write(data_json_)

                                                logging.info("Successfully saved chunk file " + filename + ".json")
                                        except:
                                                logging.error("Error saving chunk file " + filename)
                                                error_flag = 1

                                        chunk_num += 1

                logger.info("*********************************************************************")
                logger.info("****************** End of transform data module *********************")
                logger.info("*********************************************************************")


                return error_flag

        def transform_(self,columns,chunksize):
                """
                Function to transform the data from CSV into JSON after selecting
                the user input columns.
                Input: Comma separated list of columns
                Output: Error code / Transformed data saved into extraction folder
                """

                logger.info("*********************************************************************")
                logger.info("*************** Beginning transform data module *********************")
                logger.info("*********************************************************************")

                error_flag = 0

                files_list = os.listdir(self.extract_dir)

                if columns:
                        columns = columns.split(',')

                for file_ in files_list:

                        if file_.endswith(".csv"):

                                logging.info("Transforming file " + file_)
                                file_path = self.extract_dir + file_

                                csvfile = open(file_path, 'r')
                                jsonfile = open(self.extract_dir + file_ + '.json', 'w')

                                reader = csv.DictReader( csvfile, columns)
                                for row in reader:
                                        json.dump(row, jsonfile)
                                        jsonfile.write('\n')

        def load(self, hostname, port, db, username, password, collection):
                """
                Function to load data into MongoDB using pymongo module
                Input: hostname, port, db, collection
                Output: Error Code / Data loaded into collection
                """

                logger.info("*********************************************************************")
                logger.info("****************** Beginning load data module ***********************")
                logger.info("*********************************************************************")

                error_flag = 0

                logger.info("MongoDB Username : " + username)
                logger.info("MongoDB Hostname : " + hostname)
                logger.info("MongoDB Port : " + port)
                logger.info("MongoDB Database : " + db)
                logger.info("MongoDB Collection : " + collection)

                logger.info("Creating MongoDB connection.")
                try:
                        mongo_client = pymongo.MongoClient(hostname, int(port), username=username, password=password, authSource=db)
                except pymongo.errors.ConnectionFailure, e:
                        logger.error("Could not connect to server: %s" % e)
                mongo_db = mongo_client[db]
                db = mongo_db[collection]

                file_list = os.listdir(self.extract_dir)

                record_count = 0

                for file in file_list:
                        if file.endswith('.json'):
                                logger.info("Beginning to load file " + file + " into MongoDB")
                                file_path = self.extract_dir + file
                                with open(file_path) as json_file:
                                        data = json.load(json_file)
                                        try:
                                                db.insert(data)
                                                logger.info("Successfully loaded data file " + file + " into MongoDB")
                                                record_count += len(data)
                                        except:
                                                logger.error("Error loading data file " + file + " into MongoDB")
                                                error_flag = 1

                logger.info("TOTAL NUMBER OF RECORDS LOADED IN MONGODB : " + str(record_count))

                logger.info("*********************************************************************")
                logger.info("******************** End of load data module ************************")
                logger.info("*********************************************************************")

                mongo_client.close()
                return error_flag





def help():
        """
        Function to display information about the script.
        """
        print("\n")
        print("Usage: python " + sys.argv[0] + " {download|extract|transform|load|cleanup|help}")
        print("download <start year> <end year> \t\t\t\t\t download storm data from NOAA's National Weather Service into landing dir in the specified year range(inclusive).")
        print("extract  \t\t\t\t\t\t\t\t extract the downloaded gz files to CSV format into extraction directory")
        print("transform <chunksize> \t\t\t\t\t\t\t transforms data from csv to json and selects all columns")
        print("transform <comma separated list of columns> <chunksize>\t\t\t transforms data from csv to json and selects given columns")
        print("load <hostname> <port> <database> <collection> <username> <password>   \t load the data (json files) into MongoDB")
        print("cleanup  \t\t\t\t\t\t\t\t delete all files from landing and extract directories")
        print("help     \t\t\t\t\t\t\t\t display help menu")
        exit

if __name__ == "__main__":

        logger.info("******************* STARTING SCRIPT " + sys.argv[0] + " *********************")
        logger.info("****************** USERNAME : " + getpass.getuser() + " ******************")


        if len(sys.argv) >= 2 and sys.argv[1] != 'help':
                project = utilities()
                project.init()

                if len(sys.argv) == 2:
                        command = sys.argv[1]
                        if command == 'extract':
                                project.extract()
                        elif command == 'cleanup':
                                project.cleanup('')
                        elif command == 'help':
                                help()
                        else:
                                print("Invalid command")
                                help()
                elif len(sys.argv) == 3 and sys.argv[1] == 'transform':
                        project.transform('',sys.argv[2])
                elif len(sys.argv) == 3 and sys.argv[1] == 'cleanup':
                        project.cleanup(sys.argv[2])
                elif len(sys.argv) == 4 and sys.argv[1] == 'transform':
                        project.transform(sys.argv[2],sys.argv[3])
                elif len(sys.argv) == 4 and sys.argv[1] == 'download':
                        project.download(sys.argv[2],sys.argv[3])
                elif len(sys.argv) == 8 and sys.argv[1] == 'load':
                        hostname = sys.argv[2]
                        port = sys.argv[3]
                        db = sys.argv[4]
                        collection = sys.argv[5]
                        username = sys.argv[6]
                        password = sys.argv[7]
                        project.load(hostname,port,db,username,password,collection)
        else:
                help()
