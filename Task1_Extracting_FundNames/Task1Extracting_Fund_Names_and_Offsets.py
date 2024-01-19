from bs4 import BeautifulSoup
import requests
import re
import pymongo
import os
import openai
import sys
import json
import warnings
warnings.filterwarnings('ignore')

# Loading MongoDB connection string
directory = os.path.abspath('./')
with open (directory + "\\mongodbConnectionParameters.txt", "r") as myfile:
    dataString = myfile.read().split('\n')
    os.environ['MONGODB_CONNECTION_STRING'] = dataString[0]
    os.environ['MONGODB_DATABASE'] = dataString[1]
    os.environ['MONGODB_COLLECTION'] = dataString[2]

# Load training prompt data from the JSON file
with open(directory + "\\Task1_Extracting_FundNames\\Task1GptTrainMessage.json", 'r',encoding='utf-8') as json_file:
    prompt_train = json.load(json_file)

gpt_model_task1 = "gpt-4"
gpt_temperature = 0.5
gpt_max_tokens = 750
gpt_top_p = 1
gpt_frequency_penalty = 0
gpt_presence_penalty = 0

try:
    mongo_connection_string = os.environ.get('MONGODB_CONNECTION_STRING')
    mongo_database = os.environ.get('MONGODB_DATABASE')
    mongo_collection = os.environ.get('MONGODB_COLLECTION')
except:
    print("The environment variables for MongoDB connection are not defined")


#Using the OpenAI API to generate text
api_key_path = directory + "\\Key.txt"
openai.api_key_path = api_key_path

def get_text_from_url(url, max_tags=50, max_length=6000):
    """
    Retrieve text content from a URL by extracting the text from the first N non-empty tags.

    Parameters:
    - url (str): The URL to fetch the content from.
    - max_tags (int): Maximum number of tags to consider.
    - max_length (int): Maximum length of the resulting text.

    Returns:
    - str: The extracted text content.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors

        soup = BeautifulSoup(response.text, 'html.parser')
        text_tags = soup.find_all(text=True)

        first_n_tags = []
        for tag in text_tags:
            if tag.parent.name not in ['script', 'style'] and len(tag.strip()) > 0:
                first_n_tags.append(tag.parent)
            if len(first_n_tags) >= max_tags:
                break

        current_text = ""
        for tag in first_n_tags:
            current_text += tag.text

        if len(current_text) > max_length:
            current_text = current_text[:max_length]

        return current_text

    except requests.exceptions.RequestException as e:
        # Handle exceptions (e.g., connection error, HTTP error)
        print(f"Error retrieving content from {url}: {e}")
        return ""

def get_fund_names(Input_RFID):
    """
    Retrieves the fund names associated with given RFIDs from mutual fund prospectuses.
    
    Utilizes the openai.ChatCompletion API by sending a training prompt that includes previously extracted fund names from the prospectus as examples. Additionally, appends the initial text data from the first few pages of the current RFID's prospectus as a test prompt. The objective is to retrieve fund names with the provided RFIDs.

    Parameters:
    - Input_RFID (list): List of RFIDs (strings) for which fund names need to be retrieved.

    Returns:
    - target_strings (dict): A dictionary mapping each RFID to its corresponding fund names.
    """
    # Generating the test part of the prompt for ChatGPT
    html_text_from_current_prospectus = []
    for rfid in Input_RFID:
        url = "https://prospectus-express.broadridge.com/getdocument.asp?rfid="+str(rfid)
        try:
            current = get_text_from_url(url)
            html_text_from_current_prospectus.append(current)
        except requests.exceptions.RequestException as e:
            # Handle exceptions (e.g., connection error, HTTP error)
            print(f"Error retrieving content from {url}: {e}")
            return ""

    # Removing all the unnecessary characters from the text
    remove = ['\n', '\t', '\r', '\xa0', '\u200b', '\u200e', '\u200f', '\u202a', '\u202c', '\u202d', '\u202e', '\u2060', '\ufeff']
    for i in range(len(html_text_from_current_prospectus)):
        html_text_from_current_prospectus[i] = re.sub(r'|'.join(map(re.escape, remove)), '', html_text_from_current_prospectus[i])
        html_text_from_current_prospectus[i] = html_text_from_current_prospectus[i] + "\nFund Name: "

    # Add the new interaction to the training prompt data
    new_interaction = {
        "role": "user",
        "content": html_text_from_current_prospectus[0]
    }
    prompt_train.append(new_interaction)
    output = []
    for i in range(len(html_text_from_current_prospectus)):
        response = openai.ChatCompletion.create(
            model=gpt_model_task1,
            messages=prompt_train,
            temperature=gpt_temperature,
            top_p=gpt_top_p,
            max_tokens=gpt_max_tokens,
            frequency_penalty=gpt_frequency_penalty,
            presence_penalty=gpt_presence_penalty
        )

        # Appending the generated fund name to the output list
        outputFund = response.choices[0]['message']['content']
        # If output starts with "Fund Name: ", then remove it
        if outputFund[:12] == "Fund Name: ":
            output.append(response.choices[0]['message']['content'][12:])
        else:
            output.append(response.choices[0]['message']['content'])

    # Removing all the unnecessary characters from the output
    for i in range(len(output)):
        output[i] = output[i].strip()

    # Splitting the output into a list of fund names
    output_fund_list = []
    for i in range(len(output)):
        output_fund_list.append(output[i])
    for i in range(len(output_fund_list)):
        output_fund_list[i] = output_fund_list[i].split(', ')

    # Creating a dictionary of the RFIDs and their corresponding fund names
    target_strings = {}
    for i in range(len(Input_RFID)):
        target_strings[Input_RFID[i]] = output_fund_list[i]

    return target_strings

def check_if_fees(visibleText, startIndex):
    """
        Checks if the given text contains section regarding Fees and Expenses.

        Parameters:
        - visibleText (str): The text to search for fees.
        - startIndex (int): Starting index for the search.

        Returns:
        - bool: True if the Fees and Expenses information is present, False otherwise.
    """
    feeString = 'fees'
    feePattern = re.compile(rf"{feeString}", re.IGNORECASE)
    match = feePattern.search(visibleText, startIndex)
    if not match:
        return True
    if match.start() - startIndex >= 50:
        return True

    return False

def check_if_investment(visibleText, startIndex):
    """
        Checks if the given text contains section regarding Investment Objective of a Fund.

        Parameters:
        - visibleText (str): The text to search for investment information.
        - index (int): Starting index for the search.

        Returns:
        - bool: True if investment objective section is present, False otherwise.
    """
    close_string = ['The Fund’s Investment Goal','Investment Objective','Investment Objectives','Investment Goal']
    i_pattern = False
    while(not i_pattern):
        for i_string in close_string:
            i_pattern = re.compile(rf"{i_string}", re.IGNORECASE)
            match = i_pattern.search(visibleText, startIndex)
            if match:
                if match.start() - startIndex <= 300:
                    return check_if_fees(visibleText,match.start())

def text_offsets_extraction(rfid, fundNamesList):
    """
        Retrieves HTML and text offsets for given RFIDs and search strings.

        Parameters:
        - rfid (str): RFID for which offsets are to be retrieved.
        - fundNamesList (list): List of search strings (Fund names from the prospectus)

        Returns:
        - offset_collection (dict): A dictionary containing offsets for each search string.
    """
    url = "https://prospectus-express.broadridge.com/getdocument.asp?rfid="+str(rfid)
    offset_collection = {}
    # Retrieve HTML content of URL
    try:
        response = requests.get(url)
        html = response.text

        # Use BeautifulSoup to extract visible text from HTML
        soup = BeautifulSoup(html, 'html.parser')
        visible_text = soup.get_text()
        visible_text = re.sub(r'\\.', '', visible_text)
        present = {}

        # Iterate through the funds list to get the offset of each fund
        for fund in fundNamesList:
            pattern = re.compile(rf"{fund}", re.IGNORECASE)
            start = 0

            while True:
                match = pattern.search(visible_text, start)
                if not match:
                    break

                else:
                    if check_if_investment(visible_text, match.start()):
                        present[fund] = [match.start()] # match.start() gives the offset value 
                        break
                start = match.end()
        offset_collection[rfid] = present
        return offset_collection
    except requests.exceptions.RequestException as e:
        # Handle exceptions (e.g., connection error, HTTP error)
        print(f"Error retrieving content from {url}: {e}")
        return ""

# Getting HTML and Text Offsets
def get_offsets(Input_RFID,target_strings):
    """
    Retrieves HTML and text offsets for a list of RFIDs.

    Parameters:
    - Input_RFID (list): List of RFIDs for which offsets are to be retrieved.
    - target_strings (dict): A dictionary mapping each RFID to its corresponding fund names.

    Returns:
    - tags (dict): HTML offsets for each fund name.
    - text_tags (dict): Text offsets for each fund name.
    """
    # Using these keywords to get the HTML offsets of the fund names
    closeByString = ['Fees and Expenses','INVESTMENT OBJECTIVE','Investment Objective','Investment Objectives','Investment Goal','nvestment Objective']
    tags = {}
    text_tags = {}
    for rfid in Input_RFID:
        current = {}
        url = "https://prospectus-express.broadridge.com/getdocument.asp?rfid="+str(rfid)
        try:
            html = requests.get(url).text

            # Assigning 0 offset for RFIDs with only one fund name
            if len(target_strings[rfid] ) == 1:
                tags[rfid] = {target_strings[rfid][0]:[0]}
                text_tags[rfid] = {target_strings[rfid][0]:[0]}
            else:
                # Getting the HTML offsets for RFIDs with more than one fund name
                for fund in target_strings[rfid]:
                    flag = False
                    offsets =  [m.start() for m in re.finditer(fund, html, re.IGNORECASE) ]
                    for offset in offsets:
                        for tag in closeByString:
                            if tag in html[offset:offset+1750]:
                                offsets = [offset]
                                flag = True
                    try:
                        if flag == False:
                            current[fund] = [offsets[2]]
                        else:
                            current[fund] = offsets
                    except:
                        # Setting offset to -1 when offset was not found
                        print("Error Finding offset for: " + str(fund))
                        current[fund] = [-1]
                tags[rfid] = current

                # Getting the text offsets for RFIDs with more than one fund name
                textOffsets = text_offsets_extraction(rfid, target_strings[rfid])

                # Handling the case where the text offsets are not found or, are incomplete
                if textOffsets[rfid] == {}:
                    for fund in target_strings[rfid]:
                        # Setting offset to -1 when offset was not found
                        print("Error Finding offset for: " + str(fund))
                        current[fund] = [-1]
                    text_tags[rfid] = current
                elif len(textOffsets[rfid]) != len(target_strings[rfid]):
                    for fund in target_strings[rfid]:
                        if fund not in textOffsets[rfid]:
                            # Setting offset to -1 when offset was not found
                            print("Error Finding offset for: " + str(fund))
                            textOffsets[rfid][fund] = [-1]
                    text_tags[rfid] = textOffsets[rfid]

                # Assigning to text_tags dictionary when the right text offsets are found
                else:
                    text_tags[rfid] = textOffsets[rfid]          
            return tags, text_tags
        except requests.exceptions.RequestException as e:
            # Handle exceptions (e.g., connection error, HTTP error)
            print(f"Error retrieving content from {url}: {e}")
            return ""

# Inserting the data into MongoDB
def insert_into_mongoDB(Input_RFID, tags, text_tags):
    """
    Inserts data into MongoDB collection.

    Parameters:
    - Input_RFID (list): List of RFIDs.
    - tags (dict): HTML offsets for each fund name.
    - text_tags (dict): Text offsets for each fund name.
    """
    documents = []
    for rfid in Input_RFID:
        try:
            dict = {"RFID": rfid}
            dict["Funds"] = {}
            for fund in tags[rfid]:
                #dict["Funds"][fund] = {"HTML Offset":tags[rfid][fund][0]}
                dict["Funds"][fund] = {"Text Offset":text_tags[rfid][fund][0]}
            documents.append(dict)
        except:
            print("Error with RFID: " + str(rfid))
    client = pymongo.MongoClient(mongo_connection_string)
    db = client[mongo_database]
    collection = db[mongo_collection]
    collection.insert_many(documents)
    return

def task1(RFID):
    """
    Main function for Task 1. Retrieves fund names, HTML, and text offsets and inserts data into MongoDB.

    Parameters:
    - RFID (str): RFID for which data is to be processed.
    """
    Input_RFID = str(RFID)

    # Checking if the RFID is already present in the database
    client = pymongo.MongoClient(mongo_connection_string)
    db = client[mongo_database]
    collection = db[mongo_collection]
    if collection.find_one({"RFID": Input_RFID}) is None:

        #Getting the fund names
        Input_RFID = [Input_RFID]
        target_strings = get_fund_names(Input_RFID)

        # Getting the HTML and Text offsets
        tags, text_tags = get_offsets(Input_RFID,target_strings)

        # Inserting the data into MongoDB
        insert_into_mongoDB(Input_RFID, tags, text_tags)
    else:
        print("RFID already present in the database")

# Executing script from command line
if __name__ == "__main__":
    # Check if the user provided an RFID argument
    if len(sys.argv) != 2:
        print("Usage: python runner.py <RFID_data>")
        sys.exit(1)

    # Retrieve RFID data from command-line arguments 
    rfid_data = sys.argv[1]

    # Run function1 with RFID data
    result1 = task1(rfid_data)
    print("--------------------------------- TASK 1 Completed Successfully ---------------------------------")
