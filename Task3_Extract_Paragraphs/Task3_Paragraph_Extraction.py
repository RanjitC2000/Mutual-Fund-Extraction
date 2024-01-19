# -*- coding: utf-8 -*-
#Importing the required libraries
import requests
import regex as re
from bs4 import BeautifulSoup
from pymongo import MongoClient
import os

# Loading MongoDB connection string
directory = os.path.abspath('./')
with open (directory + "/mongodbConnectionParameters.txt", "r") as myfile:
    dataString = myfile.read().split('\n')
    os.environ['MONGODB_CONNECTION_STRING'] = dataString[0]
    os.environ['MONGODB_DATABASE'] = dataString[1]
    os.environ['MONGODB_TASK1_COLLECTION'] = dataString[2]
    os.environ['MONGODB_TASK3_COLLECTION'] = dataString[3]


#Preparing a dictionary with all the field names as keys and it's type as value
search_strings = { 'investment mode':'fund', 'reinstatement privilege':'class', 'cdsc':'class', 'swp':'class','f_age':'class','investment objective':'class', 'nondiversified':'fund', 'nav_cdsc':'class'
                    , 'cdsc_swp': 'class', 'phoneswitch': 'class', 'objectgoaltype':'fund', 'cdscdivorce': 'class', 'cdsc_waiv_mandat': 'class',
                   'cdsc_error':'class', 'cdsc_hardship':'class', 'objectsubgoaltype':'fund', 'cdsc_deminimus': 'class', 'cdsc_termination':'class',
                   'tax_free_returns':'class', 'cdsc_non_mandat': 'class', 'cdsc_loan_distribution':'class', 'max_round_trips':'class',
                   'income_frequency': 'fund','f_age':'class',
                   'initpurchase_amount':'class', 'subpurchase_amount': 'class', 'distribution_12b_fee':'class', 'phone_tollfree':'class',
                   'administrator':'fund', 'auditor':'fund', 'custodian':'fund', 'market_timing_policy':'fund', 'purchases_regular_final_cutoff_time':'fund',
                   'loi_fulfillment_period':'class', 'loi_fulflmnt_perd_fr_mill':'class', 'div_calc_type':'fund', 'reinvst_eligb_indctor':'fund', 'tele_redemp_eligb':'fund', 'electronic_delivery':'fund',
                   'wire_bank':'fund', 'distribution_reinvestment_load':'class', 'federal_tax_liability_for_fund_distributions':'fund'}

#Preparing the regex patterns for the fieldnames using the search strings given
re_patterns = {'reinstatement privilege':re.compile(' reinvest| reinstate| repurchase',re.IGNORECASE)}
re_patterns['nav_frequency'] = re.compile(' Day| Month| Year| once| exercise| one-time', re.IGNORECASE)
re_patterns['nav_cdsc'] = re.compile(' reinstat| reinvest| repurchas| reimburs| redeem| refund| credit| CDSC| contingent', re.IGNORECASE)
re_patterns['dividends and distributions'] = re.compile('dividends |distributions |tax',re.IGNORECASE)
re_patterns['investment mode'] = re.compile('(principal(?:[\n\s]+)investment(?:[\n\s]+)strateg[y|ies])|Strategy|mode|approach|normal|fund of funds|master',re.IGNORECASE)
re_patterns['cdsc'] = re.compile(' Death| Disabl| Died| CDSC| Waive| Defer| Retire| Employ| 401| Simple', re.IGNORECASE)
re_patterns['investment objective'] = re.compile('(investment(?:[\n\s]+)(objective|goal))| Goal| seek| objective', re.IGNORECASE)
re_patterns['swp'] = re.compile('Automatic| systematic| regular| periodic| scheduled| withdraw| payout', re.IGNORECASE)
re_patterns['cdsc_swp'] = re.compile('Withdrawal| Death| Systematic| Automatic| Waive| Perodic', re.IGNORECASE)
re_patterns['nondiversified'] = re.compile('Divers',re.IGNORECASE)
re_patterns['phoneswitch'] = re.compile('Phon| call| calling| exchang| contact', re.IGNORECASE)
re_patterns['objectgoaltype'] = re.compile('Goal|seek|objective|Income', re.IGNORECASE)
re_patterns['objectsubgoaltype'] = re.compile('Goal|seek|objective|second|incident', re.IGNORECASE)
re_patterns['cdscdivorce'] = re.compile('Divorce|waive|cdsc', re.IGNORECASE)
re_patterns['cdsc_waiv_mandat'] = re.compile('Distribution|minimum|mandatory|cdsc|waive', re.IGNORECASE)
re_patterns['cdsc_error'] = re.compile('Error|Correct|Mistake|Waive', re.IGNORECASE)
re_patterns['cdsc_hardship'] = re.compile('Hardship|Waive', re.IGNORECASE)
re_patterns['cdsc_deminimus'] = re.compile('Minimum|Balance|Small Account|Involuntary|Waive', re.IGNORECASE)
re_patterns['cdsc_termination'] = re.compile('Separation|Termination|Waive|CDSC|contingent', re.IGNORECASE)
re_patterns['tax_free_returns'] = re.compile('Excess|Death|Waive|CDSC|contingent', re.IGNORECASE)
re_patterns['cdsc_non_mandat'] = re.compile('59|Death|Mandatory|Waive|CDSC|contingent', re.IGNORECASE)
re_patterns['cdsc_loan_distribution'] = re.compile('Loan|Death|Waive|CDSC|contingent', re.IGNORECASE)
re_patterns['max_round_trips'] = re.compile('round|trip|into and out|in and out|in or out|into or out', re.IGNORECASE)
re_patterns['income_frequency'] = re.compile('dividend|distribu|net invest|annual|month|quarter|daily|policy', re.IGNORECASE)
re_patterns['f_age'] = re.compile('Distribution|70,72|minimum|mandatory|cdsc|waive',re.IGNORECASE)
re_patterns['initpurchase_amount'] = re.compile('Minimum|initial|regular|purchase|least', re.IGNORECASE)
re_patterns['subpurchase_amount'] = re.compile('Minimum|subsequent|additional|purchase|least', re.IGNORECASE)
re_patterns['distribution_12b_fee'] = re.compile('12b|Distribution fee|12b-1 & fee', re.IGNORECASE)
re_patterns['phone_tollfree'] = re.compile('800|833|844|855|866|877|889', re.IGNORECASE)
re_patterns['administrator'] = re.compile('admin', re.IGNORECASE)
re_patterns['auditor'] = re.compile('Audit|Financial Highlights|independent Registered|LLP|LLC', re.IGNORECASE)
re_patterns['custodian'] = re.compile('custodian', re.IGNORECASE)
re_patterns['market_timing_policy'] = re.compile('Adopted|approved|policy|frequent', re.IGNORECASE)
re_patterns['purchases_regular_final_cutoff_time'] = re.compile('p.m.|pm|4:00|Close|NAV|net asset value', re.IGNORECASE)
re_patterns['loi_fulfillment_period'] = re.compile('intent|letter|Month|LOI', re.IGNORECASE)
re_patterns['loi_fulflmnt_perd_fr_mill'] = re.compile('intent|letter|Month|LOI',re.IGNORECASE)
re_patterns['div_calc_type'] = re.compile('Declare|dividend|distribution|annually|daily|once a year',re.IGNORECASE)
re_patterns['reinvst_eligb_indctor'] = re.compile('Reinvest|Additional|Dividend|Securities|Same',re.IGNORECASE)
re_patterns['tele_redemp_eligb'] = re.compile('Telephone|phone|call',re.IGNORECASE)
re_patterns['electronic_delivery'] = re.compile('Electronic|Deliver|Internet|online|Web|www|.com', re.IGNORECASE)
re_patterns['wire_bank'] = re.compile('Wire|ABA|DDA|FFC|Routing|Account', re.IGNORECASE)
re_patterns['distribution_reinvestment_load'] = re.compile('Reinvest|Dividend|Distributions|Sales charge|Initial NAV', re.IGNORECASE)
re_patterns['federal_tax_liability_for_fund_distributions'] = re.compile('Federal|Ordinary Income|Taxable|Tax', re.IGNORECASE)

#Creating a dictionary with key as Field Name and value as the search string created above
fieldnames = {'NAV_REINSTATEMENT_ALLOWED': 'reinstatement privilege', 'NAV_REINSTATEMENT_PERIOD': 'reinstatement privilege', 'NAV_REINSTATEMENT_ACNT_RULS':'reinstatement privilege',\
              'NAV_REINSTATEMENT_FREQUENCY':'reinstatement privilege', 'NAV_REPRCHS_CDSC_REIMBURSE':'reinstatement privilege',\
              "CDSC_WAIVER_401K": 'cdsc', "CDSC_WAIVER_403B": 'cdsc', "CDSC_WAIVER_457": 'cdsc', "CDS_WAIV_DEATH_INDCT": 'cdsc', "CDS_WAIV_DISAB_INDCT": 'cdsc', "CDS_WAIV_DIVORCE": 'cdsc',\
              'INVESTMENT_MODE': 'investment mode', 'SWP': 'swp', 'SWP_CYCLE_INDICATOR': 'swp', 'NONDIVERSIFIED': 'nondiversified', 'CDSC_WAIVER_SWP': 'cdsc_swp'\
              , 'PHONESWITCH': 'phoneswitch', 'OBJECT_GOAL_TYPE': 'objectgoaltype', 'OBJECT_SUB_GOAL_TYPE_1' : 'objectsubgoaltype', 'CDS_WAIV_DIVORCE':'cdscdivorce', 'CDS_WAIV_MANDAT_DIST_INDCT': 'cdsc_waiv_mandat'
              , 'CDSC_WAIVER_ERROR_CORRECTION':'cdsc_error', 'CDSC_WAIVER_HARDSHIP': 'cdsc_hardship', 'CDSC_WAIVER_DEMINIMUS_DISTRIBUTION': 'cdsc_deminimus',
              'CDSC_WAIVER_TERMINATION_DISTRIBUTION':'cdsc_termination', 'TAX_FREE_RETURNS_OF_EXCESS_CONTRIBUTIONS_TO_IRA':'tax_free_returns',
              'CDSC_WAIVER_NON_MANDATORY_DISTRIBUTION':'cdsc_non_mandat', 'CDSC_WAIVER_LOAN_DISTRIBUTION': 'cdsc_loan_distribution',
              'MAXIMUM_ROUNDTRIPS_PER_YEAR':'max_round_trips', 'INCOME_FREQUENCY':'income_frequency', 'F_AGE_FOR_DISTR_WAVR':'f_age',
              'INITPURCHASE_AMOUNT':'initpurchase_amount', 'SUBPURCHASE_AMOUNT':'subpurchase_amount','DISTRIBUTION_12B_FEE':'distribution_12b_fee',
              'PHONE_TOLLFREE':'phone_tollfree', 'ADMINISTRATOR':'administrator', 'AUDITOR':'auditor', 'CUSTODIAN': 'custodian',
              'MARKET_TIMING_POLICY': 'market_timing_policy', 'PURCHASES_REGULAR_FINAL_CUTOFF_TIME':'purchases_regular_final_cutoff_time',
              'LOI_FULFILLMENT_PERIOD':'loi_fulfillment_period', 'LOI_FULFLMNT_PERD_FOR_MILL_DOL':'loi_fulflmnt_perd_fr_mill',
              'DIV_CALC_TYPE':'div_calc_type', 'REINVST_ELIGB_INDCTOR':'reinvst_eligb_indctor', 'TELE_REDEMP_ELIGB':'tele_redemp_eligb', 'ELECTRONIC_DELIVERY':'electronic_delivery','WIRE_BANK':'wire_bank',
              'DISTRIBUTION_REINVESTMENT_LOAD':'distribution_reinvestment_load', 'FEDERAL_TAX_LIABILITY_FOR_FUND_DISTRIBUTIONS':'federal_tax_liability_for_fund_distributions'}

#Selecting only few fieldnames required for the final Demo. Comment the below line to extract the paragraphs for every field names
fieldnames = {'NONDIVERSIFIED': 'nondiversified', 'INCOME_FREQUENCY':'income_frequency', 'NAV_REINSTATEMENT_ALLOWED': 'reinstatement privilege',
               'OBJECT_GOAL_TYPE': 'objectgoaltype', 'INITPURCHASE_AMOUNT':'initpurchase_amount', 'SUBPURCHASE_AMOUNT':'subpurchase_amount',
               'AUDITOR':'auditor',}

# Define a function named documentParser that takes a URL as input
def documentParser(url):
  """
    Retrieves HTML content from the specified URL and parses it using BeautifulSoup.

    Parameters:
    - url (str): The URL of the document to be parsed. Must be a valid and accessible URL.

    Returns:
    - BeautifulSoup: An object representing the parsed HTML content.
  """
  try:
    # Make a GET request to the URL
    response = requests.get(url)
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    return soup
  except Exception as e:
    return "Encountered the following error while trying to fetch the data from the HTML:" + "\n" + e

def filterParagraphs(paras):
    """
    Filters out duplicate paragraphs from the provided list.

    Parameters:
    - paras (list): A list of paragraphs, each represented as a tuple with content and offset.

    Returns:
    - list: A filtered list of paragraphs without duplicates.

    Notes:
    - The function iterates over each paragraph to identify and exclude duplicates based on content.
    - The first loop compares each paragraph with all others, excluding self-comparisons.
    - If a duplicate is found, the index of the current paragraph is added to the exclusion list ('idxes').
    - The second loop constructs the final list by including paragraphs whose index is not in the exclusion list.
    - The function returns the filtered list of paragraphs without duplicates.
    """
    # Initialize a list to store indexes of paragraphs to be excluded
    idxes = []

    # Initialize a list to store the final set of paragraphs
    final_list = []

    # Iterate over each paragraph in the provided list
    for i in range(len(paras)):
      # Compare the current paragraph with all other paragraphs
      for j in range(len(paras)):
        # Skipping self-comparisons
        if i != j:
          # Check if the content of the current paragraph is present in another paragraph
          if paras[i][0] in paras[j][0]:
            # Check if the paragraphs have the same content but different indices
            if paras[i][0] == paras[j][0] and paras[i][1] != paras[j][1]:
                # If the content is the same but indices are different, skip to the next iteration
                continue

            # Add the index of the current paragraph to the exclusion list
            idxes.append(i)
            # Break out of the inner loop since a match has been found
            break

    # Iterate over each paragraph and include it in the final list if its index is not in the exclusion list
    for i in range(len(paras)):
      if i not in idxes:
          final_list.append(paras[i])

    # Return the final list of paragraphs without duplicates
    return final_list


def class_text_extraction(element, tags_info, tagslist, all_text=''):
    """
    Extracts text occurrences for a given element based on specified tags and patterns.

    Parameters:
    - element (str): The element for which text occurrences are to be extracted.
    - tags_info (list): List of tags containing information about sections relevant to the element.
    - tagslist (list): List of tag names corresponding to tags_info.
    - all_text (str): Optional parameter containing the entire text document.

    Returns:
    - list: A list of dictionaries, each containing 'text' and 'offset' keys, representing extracted text occurrences.

    Notes:
    - The function uses regular expressions specified by 'element' to identify relevant text within each section.
    - For each identified section, it extracts text along with offset and appends it to the 'outputs' list.
    - The resulting list provides instances of text occurrences for the given element.
    """

    # Initialize a list to store text occurrences along with their offsets
    tag_outputs = []

    # Iterate over each tag in the provided 'tags_info' list
    for tag in range(len(tags_info)):
        # Initialize the previous offset
        prev_offset = 0

        try:
            # Initialize a counter for occurrences within a section
            occurrence = 0

            # Iterate over each section in the current tag's information
            for section in tags_info[tag]:
                # Extract the regular expression pattern for the given element
                pattern = re_patterns[element]

                try:
                    # Attempt to extract the text from the section
                    text = section.get_text(separator=' ', strip=True)
                except Exception as e:
                    # Continue to the next iteration if text extraction fails
                    continue

                # Find all occurrences of the pattern within the text
                pmatch = re.findall(pattern, text)
                # Initialize a string to store the text content of the occurrence
                occurrence_paragraph = ''

                if pmatch:
                    # Get the full text of the section
                    text1 = section.get_text()

                    # Calculate the offset of the text within the entire document
                    offset = all_text.find(text1, prev_offset)
                    prev_offset = offset + len(text1)
                    occurrence_paragraph += text + '\n'
                    # Initialize the parent div as the current section
                    parent_div = section

                    # Iterate through parent divs to extract additional text
                    while True:
                        # Find the next sibling with the specified tag
                        parent_div = parent_div.find_next_sibling(tagslist[tag])

                        try:
                            # Attempt to get the text content of the parent div
                            parent_div_text = parent_div.get_text()
                        except:
                            # Break the loop if text extraction fails
                            break

                        # Skip if the text consists of digits or mentions a table of contents
                        if parent_div_text.isdigit() or re.search('table of contents', parent_div_text.strip()):
                            continue

                        # Try to find the index of the next alphabetical character in the text
                        try:
                            char_index = parent_div_text.find(next(filter(str.isalpha, parent_div_text)))
                        except Exception as index_error:
                            char_index = 0

                        # Skip if the parent div text is already part of the occurrence paragraph
                        if parent_div_text in occurrence_paragraph:
                            continue

                        # If the text contains alphabets and is either short or starts with uppercase letters,
                        # add it to the occurrence paragraph
                        if re.search('[a-zA-Z]', parent_div_text.strip()) and (len(parent_div_text.split(' ')) < 3 or parent_div_text[char_index:char_index + 3].isupper()):
                            occurrence_paragraph += parent_div_text + ' '
                            break

                        # Add the parent div text to the occurrence paragraph
                        occurrence_paragraph += parent_div_text + ' '
                        
                        # Break the loop if the length of the occurrence paragraph exceeds 700 words
                        if len(occurrence_paragraph.split(' ')) > 700:
                            break

                    # Limit the occurrence paragraph to the first 700 words
                    occurrence_paragraph = ' '.join(occurrence_paragraph.split()[:700])

                    # If the pattern is still found in the first 200 words of the occurrence paragraph,
                    # increment the occurrence counter and append the occurrence to the outputs list
                    if re.findall(pattern, ' '.join(occurrence_paragraph.split()[:200])):
                      occurrence += 1
                      tag_outputs.append((occurrence_paragraph, offset))
        except Exception as e:
            # Print an error message if there is no section with the given data
            print('No section with given data', e)

    # Initialize a list to store the final outputs
    outputs = []

    # Check if there are any tag outputs
    if len(tag_outputs) > 0:
        # Remove duplicate tag outputs
        set_outputs = list(set(tag_outputs))
        # Filter and process the outputs using the filterParagraphs function
        set_outputs = filterParagraphs(tag_outputs)
        # Iterate through the set outputs and create instances with 'text' and 'offset' keys
        for i in set_outputs:
            instance = {}
            instance['text'] = i[0]
            instance['offset'] = i[1]
            outputs.append(instance)

    # Return the final list of dictionaries containing '

    return outputs

#For fund level paragraphs, assigning each paragraph to respective fund
def field_offsets(element, rfid):
    """
    Assigns each paragraph in the provided element to the respective fund based on offset ranges retrieved from the database.

    Parameters:
    - element (list): A list of paragraphs, each represented as a dictionary with 'offset' information.
    - rfid (str): RFID identifier used to fetch the corresponding fund list with offset ranges from the database.

    Returns:
    - dict: A dictionary where keys are fund names, and values are lists of paragraphs assigned to each fund based on offsets.

    Notes:
    - The function retrieves offset information for funds associated with the given RFID from a MongoDB database.
    - Each paragraph in the 'element' is assigned to the fund whose offset range encompasses the paragraph's offset.
    - The resulting dictionary provides a mapping of funds to paragraphs.
    - In case of an error, the original 'element' is returned, and an error message is printed.
    """
    try:
        # Try to retrieve MongoDB connection information from environment variables
        try:
            mongo_connection_string = os.environ.get('MONGODB_CONNECTION_STRING')
            mongo_database = os.environ.get('MONGODB_DATABASE')
            mongo_task1_collection = os.environ.get('MONGODB_TASK1_COLLECTION')
        except:
            print("The environment variables for MongoDB connection are not defined")

        # Connect to the MongoDB server
        client = MongoClient(mongo_connection_string)
    
        # Select the 'broadridge' database
        db = client[mongo_database]

        # Access the 'Task1_Demo' collection within the database
        collection = db[mongo_task1_collection]

        # Retrieve document with fund information based on RFID
        docs = collection.find({"RFID": rfid})[0]
        fund_list = docs['Funds']
        fund_final = {}
        print(fund_list)
        
        # Sort fund list based on 'Text Offset'
        fund_list = sorted(fund_list.items(), key=lambda x:x[1]['Text Offset'])
        print(fund_list)
        
        # Assigning each paragraph to a fund based on offset ranges
        for i in element:
          current_offset = i['offset']
          
          # Check if the offset is -1, and assign to 'others' fund if true
          if current_offset == -1:
            try:
                  fund_final["others"].append(i)
            except:
                  fund_final["others"] = [i]
            continue

          # Iterate through the sorted fund list and assign the paragraph to the appropriate fund
          for idx in range(len(fund_list)):
            if idx != len(fund_list) - 1:
              if fund_list[idx][1]['Text Offset'] <= current_offset <= fund_list[idx+1][1]['Text Offset']:
                try:
                  fund_final[fund_list[idx][0]].append(i)
                except:
                  fund_final[fund_list[idx][0]] = [i]
                break
            else:
              try:
                fund_final[fund_list[idx][0]].append(i)
              except:
                fund_final[fund_list[idx][0]] = [i]
              break
    except Exception as e:
        # Print an error message if an exception occurs
        print("Error while fetching field_offsets", e)
        return element
    
    # Return the final dictionary mapping funds to paragraphs
    return fund_final


def validate_rfid_format(rfid):
    """
    Validates the format of an RFID.

    Parameters:
    - rfid (str): The RFID code to be validated.

    Returns:
    - bool: True if the RFID code has the correct format (7 alphanumeric characters),
            False otherwise.
    """
    # Assuming RFID should have 10 alphanumeric characters
    return bool(re.match("^[0-9]{7}$", rfid))

def Task3(RFID):
  """
  Process data for a given RFID using web scraping.

  Parameters:
  - RFID (str): The RFID for which data is to be processed.

  Returns:
  - dict: A dictionary containing processed data for the given RFID.

  Notes:
  - The function extracts data from a specific URL using BeautifulSoup and processes it based on predefined rules.
  - The final_outputs dictionary contains information about different fieldnames.
  """

  # Set up the URL
  final_outputs = {}
  rfid = RFID
  final_outputs['RFID'] = rfid
  if validate_rfid_format(RFID) is False:
    print('Invalid RFID Format')
    return final_outputs
  url = 'https://prospectus-express.broadridge.com/getdocument.asp?rfid=' + rfid

  # Get the Beautiful Soup instance
  soup = documentParser(url)
  if isinstance(soup, str):
    return "Error while parsing the HTML: " + soup
  all_text = soup.get_text()

  # List of HTML tags to extract information from
  tagslist = ['span', 'p', 'div', 'table']
  tags_info = []

  # Getting the text of all the HTML tags present in the tagslist
  for tag in tagslist:
      extracted_section = soup.find_all(tag, recursive=True)
      tags_info.append(extracted_section)

  # Getting paragraphs of every fieldname
  for element in fieldnames.keys():
    # Process text for each fieldname using class_text_extraction function
    final_outputs[element] = class_text_extraction(fieldnames[element], tags_info, tagslist, all_text)

    # If it is a fund level fieldname, get the information about the fund of the paragraph also
    if search_strings[fieldnames[element]] == 'fund':
      final_outputs[element] = field_offsets(final_outputs[element], rfid)

    print(element, len(final_outputs[element]))

  try:

    try:
        mongo_connection_string = os.environ.get('MONGODB_CONNECTION_STRING')
        mongo_database = os.environ.get('MONGODB_DATABASE')
        mongo_task3_collection = os.environ.get('MONGODB_TASK3_COLLECTION')
    except:
        print("The environment variables for MongoDB connection are not defined")

    # Connect to the MongoDB server
    client = MongoClient(mongo_connection_string)
    
    # Select the 'broadridge' database
    db = client[mongo_database]

    # Access the 'Task3_Extract' collection within the database
    collection = db[mongo_task3_collection]

    if collection.find_one({"RFID": RFID}) is None:
        try:
            # Insert the document into the collection
            result = collection.insert_one(final_outputs)
            inserted_id = result.inserted_id
            print(f"Inserted document with id {inserted_id}")
        except Exception as e:
           print('Error while inserting the record ', e)
    else:
        print("RFID already present in the database")

  # Handle any unexpected errors that may occur during MongoDB operations
  except Exception as e:
    print(f"Following error occurred while iserting the record into database: {e}")

if __name__ == '__main__':
    #Change the RFID to process the record for different RFID
    RFID = '2854858'
    Task3(RFID)