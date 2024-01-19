# coding: utf-8
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import yaml
from pymongo import MongoClient
from collections import OrderedDict
import certifi
ca = certifi.where()

# Function to read configuration from a YAML file
def read_config(file_path):
    with open(file_path, 'r') as file:
        config_data = yaml.safe_load(file)
    return config_data

config_file_path = 'config.yaml' 
config = read_config(config_file_path)

# Accessing values from the config
mongo_connection_string = config['database']['mongo_connection_string']
db_name = config['database']['name']
collection_from = config['database']['collection_from']
collection_task_2 = config['database']['collection_task_2']

prospectus_base_url = config['prospectus_base_url']
backend_load = config['backend_load']
frontend_load = config['frontend_load']
both_load = config['both_load']
fund_level = config['fund_level']


# Creating ordered list of Funds from MongoDB (Function to retrieve RFIDs from the Task2 database)
def getRFIDs_from_task2():
    client = MongoClient(mongo_connection_string, tlsCAFile=ca)
    db = client[db_name]
    collection = db[collection_task_2]

    # Fetch all rows and retrieve the 'rfids' field
    result = collection.find({}, {'_id': 0, 'RFID': 1})
    task1_list= set()
    # Iterate through the results and print the 'rfids' field from each document
    for document in result:
        task1_list.add(document['RFID'])
    return task1_list

# Function to get data from Task1 database
def get_from_Task1_db():
    client = MongoClient(mongo_connection_string, tlsCAFile=ca)
    db = client[db_name]
    collection = db[collection_from]
    return collection
    
# Function to fetch ordered list of funds for a given RFID
def fetch_ordered_fund_list(collection, rfid):
    # fetch documents 
    docs= collection.find({"RFID": rfid})[0]
    fund_list= docs['Funds']

    fund_dict = OrderedDict()
    # iterate through the dictionary
    prev_key=''
    for key, value in fund_list.items():

        fund_dict[key]=[value['Text Offset'], 999999999]
        if(prev_key!=''):
            fund_dict[prev_key][1]= value['Text Offset']
            fund_dict[prev_key] = tuple(fund_dict[prev_key])        
        prev_key = key
    fund_dict[prev_key] = tuple(fund_dict[prev_key]) 
    
    ordered_funds=[]
    for key, value in fund_dict.items():
        ordered_funds.append(key)
    
    return ordered_funds

# Function to extract data from a table
def extract_table(table):
    table_data=[]
    for row in table.find_all('tr'):
        # Extract the data from each cell in the row
        cells = row.find_all('td')
        row_data = []
        for cell in cells:
            row_data.append(cell.text.strip())
        table_data.append(row_data)
    df = pd.DataFrame(table_data)
    return df

# Function to extract tables containing relevant information from prospectus
def extract_all_relevant_tables(rfid):
    
    try:
        # URL of the webpage to extract tags from
        url = prospectus_base_url+str(rfid)

        # Send a GET request to the URL
        response = requests.get(url)

        # Create a BeautifulSoup object with the HTML content of the response
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract all <table> tags from the HTML content
        table_tags = soup.find_all('table')

        search_string='Maximum'
        table_list=[]

        for table_tag in table_tags:

            if(search_string in table_tag.text):
                table_list.append(table_tag)
        
        return table_list
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def get_data_from_table_list(table_list):
    list_of_tables=[]
    for table in table_list:
        table_data =  extract_table(table)
        list_of_tables.append(table_data)
    return list_of_tables

def contains_number(string):
    # Regular expression to match a valid number (integer or floating-point)
    number_pattern = re.compile(r'-?\d+\.?\d*')

    return bool(number_pattern.search(string))

def remove_esc_chars(word):
    # Remove escape characters and extra spaces
    string_without_escapes = word.replace('\r', '').replace('\n', '').replace('\\', '')
    string_without_extra_spaces = ' '.join(string_without_escapes.split())

    return string_without_extra_spaces

# Function to check if a string contains backend-related keywords
def check_string_for_backend(big_str, small_str):
    small_words = small_str.split()
    
    # Check if all words in the smaller string are present in the bigger string
    for word in small_words:
        if word not in big_str:
            return False
    return True

# Function to check if a string contains frontend-related keywords
def check_string_for_frontend(big_str, small_str):
    small_words = small_str.split()
    
    # Check if all words in the smaller string are present in the bigger string
    for word in small_words:
        if word not in big_str:
            return False
    # Check if the word "deferred" is not present in the bigger string
    if "deferred" in big_str:
        return False
    return True

# Function to extract frontend and backend load information at the class level
def class_level_front_back_load_extract(df):

    columns = df.columns.tolist()
    class_list=[]
    frontend='Maximum sales charge'.lower()

    backend='deferred sales charge'.lower()
    row_found=False
    front_row_found=False
    found_front=False
    found_back=False
    back_row_found=False
    front_mp=dict()
    back_mp=dict()

    for row_num, row in df.iterrows():

        if(found_front and found_back):
            break

        if(not row_found):
            for c in columns:
                
                if(row[c] and row[c]!='Class:'):
                    row_found=True
                    class_list.append(remove_esc_chars(row[c]))

        if(row_found):
            for c in columns:

                if(row[c] and check_string_for_frontend(row[c].lower(), frontend)):
                    front_row_found=True
                    item_ct=0
                    continue        

                if(front_row_found):
                    found_front=True
                    if(item_ct>=len(class_list)):
                        front_row_found= False
                        break
                    if('None'.lower() in row[c].lower() ):
                        front_mp[class_list[item_ct]]='not frontend'  
                        item_ct=item_ct+1
                    elif (("%" in row[c] and contains_number(row[c])) or contains_number(row[c]) and "None" not in row[c]):
                        front_mp[class_list[item_ct]]='frontend'
                        item_ct=item_ct+1

            front_row_found= False

        if(row_found):
            for c in columns:

                if(row[c] and check_string_for_backend(row[c].lower(), backend)):
                    back_row_found=True
                    item_ct=0
                    continue

                if(back_row_found):
                    found_back=True
                    if(item_ct>=len(class_list)):
                        break
                    if('None'.lower() in row[c].lower()):
                        back_mp[class_list[item_ct]]='not backend' 
                        item_ct=item_ct+1
                    elif ("%" in row[c] and contains_number(row[c]) and "None" not in row[c] ):
                        back_mp[class_list[item_ct]]='backend'
                        item_ct=item_ct+1

            back_row_found= False

            if(front_row_found):
                break
    return class_list, front_mp, back_mp

# Function to extract frontend and backend load information
def extract_front_back_load(class_list, front_mp, back_mp, frontend_load, backend_load, both_load, fund_level):
    field_list=dict()
    load_type=dict()
    
    for cl in class_list:
        
        if cl not in front_mp or cl not in back_mp:
            continue
        field_list[cl]=[]
        field_list[cl].extend(fund_level)
        if(front_mp[cl] == 'frontend' and back_mp[cl]=='backend'):
            field_list[cl].extend(both_load)
            field_list[cl].extend(frontend_load)
            field_list[cl].extend(backend_load)
            load_type[cl]= 'Both Frontend and Backend'

        elif(front_mp[cl] == 'frontend' and back_mp[cl]=='not backend'):
            field_list[cl].extend(frontend_load)
            field_list[cl].extend(both_load)
            load_type[cl]= 'Frontend'

        elif(front_mp[cl] == 'not frontend' and back_mp[cl]=='backend'):
            field_list[cl].extend(backend_load)
            field_list[cl].extend(both_load)
            load_type[cl]= 'Backend'

        else:
            load_type[cl]= 'None'
    return field_list, load_type       

# Function to push data to Task2 database
def push_to_Task2_db(all_docs_fetched_for_rfid):
    # connect to MongoDB server
    client = MongoClient(mongo_connection_string, tlsCAFile=ca)
    db = client[db_name]
    collection = db[collection_task_2]
    collection.insert_many(all_docs_fetched_for_rfid)
    print("Documents inserted successfully!")

# Function to run Task2 for a given RFID
def run_Task2(rfid, ordered_funds):
    table_list= extract_all_relevant_tables(rfid)
    list_of_tables= get_data_from_table_list(table_list)
    
    if(len(list_of_tables)==0):
        print("No Shareholder fee tables found")

    # Mapping each fund to shareholder fees table
    fund_table_dict=dict()

    for i in range(min(len(ordered_funds),len(list_of_tables))):
        fund_table_dict[ordered_funds[i]]=list_of_tables[i]

    # Creating a list of all Fund and Class for single RFID
    all_docs_fetched_for_rfid=[]

    for key, value in fund_table_dict.items():
        json_doc= {
        "RFID": rfid
        }
        json_doc.update({'Fund' : key})

        class_list, front_mp, back_mp= class_level_front_back_load_extract(value)

        field_list, load_type= extract_front_back_load(class_list, front_mp, back_mp, frontend_load, backend_load, both_load, fund_level)
                
        json_doc.update({'Load Type' : load_type})
        json_doc.update({'Field List' : field_list})
        all_docs_fetched_for_rfid.append(json_doc)
        
    return all_docs_fetched_for_rfid

# Main function to run Task2 for a given RFID if it's not already in the database
def run_task2_main(rfid):
    task2_list= getRFIDs_from_task2()  
    if rfid in task2_list:
        print("RFID " +rfid+" already present in Database")
    else: 
        collection= get_from_Task1_db()
        ordered_funds= fetch_ordered_fund_list(collection, rfid)
        all_docs_fetched_for_rfid= run_Task2(rfid, ordered_funds)
        if(len(all_docs_fetched_for_rfid)>0):
            push_to_Task2_db(all_docs_fetched_for_rfid)

# rfid = "2854858"
# run_task2_main(rfid)


