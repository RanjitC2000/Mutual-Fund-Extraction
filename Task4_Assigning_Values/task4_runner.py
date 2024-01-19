# Import necessary libraries and modules
from pymongo import MongoClient  # For MongoDB connectivity
import pandas as pd  # For data manipulation and analysis
from .HelperFunctions import paragraphFiltering, gptTextExtraction, bertValueAssignment  # Custom helper functions
import re  # For regular expressions
from unicodedata import normalize  # For text normalization
from sklearn.metrics.pairwise import cosine_similarity  # For cosine similarity calculation
from sentence_transformers import SentenceTransformer  # For encoding sentences into vectors
import os

# Load a pre-trained SentenceTransformer model for sentence encoding
model = SentenceTransformer('bert-base-nli-mean-tokens')

# Define time intervals mapping for specific time-related keywords
time_intervals = {
    "monthly": 12,
    "yearly": 1,
    "annually": 1,
    "once a year": 1,
    "quarterly": 4,
    "each month": 1,
    "annual": 1,
    "semiannually": 2
}

# Loading MongoDB connection string
directory = os.path.abspath('./')
with open (directory + "\\mongodbConnectionParameters.txt", "r") as myfile:
    dataString = myfile.read().split('\n')
    os.environ['MONGODB_CONNECTION_STRING'] = dataString[0]
    os.environ['MONGODB_DATABASE'] = dataString[1]
        # Column Names
os.environ['FIELD_NAME'] = "Field Name "


def checkFieldName(df, fieldName, className):
    """
    Checks if a given field exists within a DataFrame filtered by a specified class name.

    Parameters:
    - df (DataFrame): The DataFrame containing columns "Field Name", "Class", and "Class Name".
    - fieldName (str): The name of the field to check within the DataFrame.
    - className (str): The class against which the existence of the field is to be checked.

    Returns:
    - bool: True if the field exists in the specified class or in the "All Classes" category, False otherwise.
    """
    
    # Filter the DataFrame `df` to obtain rows where the "Field Name" matches the provided `fieldName`.
    FIELD_NAME = os.environ.get('MONGODB_CONNECTION_STRING')
    df = df[df[FIELD_NAME] == fieldName]

    # Filter the resulting DataFrame (`df`) to identify rows containing the "All Classes" category in the "Class" column.
    filtered_df_all_class = df[df["Class "].str.contains("All Classes")]
    
    # Further filter the resulting DataFrame to identify rows with the specified `className` in the "Class Name" column.
    filtered_df = df[df["Class Name"].str.contains(className)]

    # Check if either of the filtered DataFrames (`filtered_df` or `filtered_df_all_class`) contain any rows.
    return len(filtered_df) > 0 or len(filtered_df_all_class) > 0


def getAlphaNumeric(text):
    """
    Extracts alphanumeric characters from a given text string.

    Parameters:
    - text (str): The input text from which alphanumeric characters will be extracted.

    Returns:
    - str: A string containing only alphanumeric characters extracted from the input text.
    """    
    # Return the string containing only the extracted alphanumeric characters.
    return ''.join([ch for ch in text if ch.isalnum()])


def clean_text(text):
    """
    Cleans and normalizes the given text by removing specific characters and extra whitespaces.

    Parameters:
    - text (str): The input text to be cleaned and normalized.

    Returns:
    - str: The cleaned and normalized text.
    """
    
    # Normalizes the text using NFKD normalization form to handle diacritic characters.
    text = normalize("NFKD", text)  # Normalization
    
    # Removes specific characters like bullets, asterisks, hyphens, etc., and replaces them with a space.
    text = re.sub(r'[\u2022\u2023\u25E6\u2043*\-+•·]', " ", text)
    
    # Replaces multiple whitespaces with a single space.
    text = re.sub("\s+", " ", text)
    
    # Strips leading and trailing whitespaces from the text.
    text = text.strip()
    
    # Returns the cleaned and normalized text.
    return text


def get_closest_extracted_string(extractedTexts, fieldName, className):
    """
    Finds the closest extracted string matching the provided field name and class name.

    Parameters:
    - extractedTexts (list): List of strings containing extracted text snippets.
    - fieldName (str): The name of the field to match against in the dataset.
    - className (str): The class name against which matching is performed.

    Returns:
    - str: The closest extracted string matching the field name and class name.
    """

    final_texts = []  # Initialize an empty list to store final text candidates

    # Reads data from the specified Excel file and filters it by the given field name
    directory = os.path.abspath('./')
    actual_df = pd.read_excel(directory + "\\Task4_Assigning_Values\\Data\\CompiledReq.xlsx")
    FIELD_NAME = os.environ.get('MONGODB_CONNECTION_STRING')
    actual_df = actual_df[actual_df[FIELD_NAME] == fieldName]

    # Iterate through each extracted text
    for text in extractedTexts:
        # Encode the extracted text using a model (assumed to be defined elsewhere)
        dataStringEncoding = model.encode([text])
        df = actual_df  # Assign the filtered DataFrame to a new variable

        # Process the class name to find the corresponding column in the DataFrame
        className = getAlphaNumeric(className)  # Removes non-alphanumeric characters
        className = "Class " + className[len('Class'):]  # Prepares the class column name

        # Filters the DataFrame based on the class name
        df = pd.concat([df[df["Class"].str.contains(className)], df[df["Class"].str.contains("All Classes")]])

        # Check if there are any matching rows in the DataFrame
        if len(df) > 0:
            dataStrings = df["Data String"].tolist()  # Extract data strings from the DataFrame
            encodings = model.encode(dataStrings)  # Encode data strings using the same model

            # Calculate similarity score between the encoded extracted text and data strings
            print("CHECKINGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG")
            print(dataStringEncoding)
            print(encodings)
            print(cosine_similarity(dataStringEncoding, encodings).tolist())
            print("CHECKINGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG")
            score = cosine_similarity(dataStringEncoding, encodings).tolist()
            score = sum(score[0]) / len(score[0])  # Calculate average similarity score
            final_texts.append((text, score))  # Append extracted text and its score to final_texts list
        else:
            final_texts.append((text, -1))  # If no matching rows, append text and a default score of -1

    final_texts.sort(key=lambda x: x[1])  # Sort final_texts based on similarity score
    return final_texts[-1][0]  # Return the closest extracted string with the highest score

    
def extractRelevantText(paragraphs, fieldName, className):
    """
    Extracts relevant text snippets based on field and class names from given paragraphs.

    Parameters:
    - paragraphs (list): A list of paragraphs or text snippets.
    - fieldName (str): The name of the field to extract text for.
    - className (str): The class name for filtering relevant text.

    Returns:
    - set: A set containing relevant text snippets extracted based on the provided field and class names.
    """

    extractedTexts = set()  # Initialize an empty set to store extracted text snippets

    # Iterate through each paragraph in the provided list
    for para in paragraphs:
        para = clean_text(para)  # Clean and normalize the paragraph text
        response = gptTextExtraction.extract_text_from_para(para, fieldName, className)  # Extract text using a specific function/API
        text = response['choices'][0]['message']['content']  # Extract the content of the extracted text

        # Check if the extracted text is not "None" before adding it to the set
        if text != "None":
            extractedTexts.add(text)  # Add the extracted text to the set of extractedTexts
    
    # If no text snippets were extracted, add "None" to the set
    if len(extractedTexts) == 0:
        extractedTexts.add("None")
    
    # Return the set containing relevant text snippets
    return extractedTexts


def getRelevantParas(paragraphs, field):
    """
    Retrieves relevant paragraphs based on a specified field from a given list of paragraphs.

    Parameters:
    - paragraphs (list): A list of paragraphs or text snippets.
    - field (str): The field to filter relevant paragraphs.

    Returns:
    - list: A list of the top 20 relevant paragraphs sorted by relevance.
    """

    relevantParas = []  # Initialize an empty list to store tuples of paragraphs and their relevance
    
    # Iterate through each paragraph in the provided list
    for para in paragraphs:
        # Filter the paragraph based on the specified field and get its relevance score
        result = paragraphFiltering.filter_paragraphs(para, field)
        relevantParas.append((para, result))  # Append the paragraph and its relevance score as a tuple
    
    # Sort the list of tuples based on relevance score in descending order and select the top 10
    relevantParas = sorted(relevantParas, key=lambda x: x[1], reverse=True)[:10]
    
    sortedRelevantParas = []  # Initialize an empty list to store the sorted relevant paragraphs
    
    # Extract only the paragraphs from the list of tuples and append them to the sortedRelevantParas list
    for para in relevantParas:
        sortedRelevantParas.append(para[0])
    
    # Return the top 20 relevant paragraphs sorted by relevance
    return sortedRelevantParas


def extractTextFieldSpecific(relevantParas, field, className, key):
    """
    Extracts specific fields of interest from relevant paragraphs based on field name and class name.
    Currently included the fields NONDIVERSIFIED, INCOME_FREQUENCY, AUDITOR

    Parameters:
    - relevantParas (list): A list of relevant paragraphs.
    - field (str): The specific field to extract.
    - className (str): The class name related to the field.
    - key: A key used for identification or logging purposes.

    Returns:
    - tuple: Extracted field information or a default value.
    """

    
    # Check for specific field types and perform extraction based on field type
    if field == "NONDIVERSIFIED":
        # Extracts information related to the "NONDIVERSIFIED" field
        for para in relevantParas:
            para_new = re.sub(r'[^a-zA-Z]', '', para).lower()  # Cleans and normalizes the paragraph text
            # Checks if specific keywords related to non-diversified are present in the paragraph
            if ('non diversified' in para_new) or ('nondiversified' in para_new):
                return (1, 1, "non-diversified")  # Returns information related to non-diversified field
    
    elif field == "INCOME_FREQUENCY":
        # Extracts information related to the "INCOME_FREQUENCY" field
        for para in relevantParas:
            para_new = re.sub(r'[^a-zA-Z]', '', para).lower()  # Cleans and normalizes the paragraph text
            # Checks for specific time interval keywords in the paragraph
            for s in time_intervals.keys():
                if s in para_new:
                    return (time_intervals[s], time_intervals[s], s)  # Returns information related to time intervals
    
    elif field == "AUDITOR":
        # Extracts information related to the "AUDITOR" field using other defined functions
        extractedTexts = extractRelevantText(relevantParas, field, className)  # Extracts relevant text snippets
        extractedText = get_closest_extracted_string(extractedTexts, field, className)  # Finds closest extracted string
        return (extractedText, extractedText, extractedText)  # Returns extracted auditor information
    
    return ("NOT_VALID", 1, "")  # Returns a default value if field type is not handled

        
def assignValues(rfId):
    """
    Assigns values to specific fields based on extracted data and predefined templates.

    Parameters:
    - rfId (str): The unique identifier used to retrieve data.

    Returns:
    - None
    """
    print("RFID ", rfId)  # Print the RFID for identification
    
    # Connect to MongoDB
    try:
        mongo_connection_string = os.environ.get('MONGODB_CONNECTION_STRING')
        mongo_database = os.environ.get('MONGODB_DATABASE')
    except:
        print("ERROR: The environment variables for MongoDB connection are not defined.")
    client = MongoClient(mongo_connection_string)

    # Retrieve data from Task2_Demo collection
    collection = client.broadridge["Task2_Demo"]
    document = collection.find_one({"RFID": rfId})

    # Retrieve data from Task3_Extract collection
    paragraphs_collection = client.broadridge["Task3_Extract"]
    paragraphs_document = paragraphs_collection.find_one({"RFID": rfId})

    # Read data from an Excel file and filter based on the provided rfId
    directory = os.path.abspath('./')
    df = pd.read_excel(directory + "\\Task4_Assigning_Values\\Data\\Requirement_Template.xlsx")
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])
    df = df[df['RFID'] == int(rfId)]
    df_columns = df.columns
    FIELD_NAME = os.environ.get('MONGODB_CONNECTION_STRING')


    printValues = dict()  # Initialize a dictionary to store assigned values
    
    if document:
        for className, fieldNames in document["Field List"].items():
            className = gptTextExtraction.getAlphaNumeric(className)
            
            # Loop through field names in the document
            for field in list(set(fieldNames)):
                if field in ['AUDITOR']: # Currently working field

                ### Other Field Names
                    
                # "CDSC_WAIVER_IRA_SEP", "CDSC_WAIVER_SIMPLE_401K" "NAV_REINSTATEMENT_ALLOWED","CDS_WAIV_DEATH_INDCT",
                            #  "CDS_WAIV_DISAB_INDCT","CDSC_WAIVER_401K","CDSC_WAIVER_403B",
                            #  "CDSC_WAIVER_PROFIT_SHARING","CDSC_WAIVER_SIMPLE_IRA",
                            #  "NAV_REINSTATEMENT_ACNT_RLES","NAV_REINSTATEMENT_FREQUENCY" field == 'NAV_REINSTATEMENT_ALLOWED'
                    
                    # Check if field exists in paragraphs_document
                    if field in paragraphs_document:
                        paragraphs = []

                        ### For class level fields

                        for key, value in paragraphs_document[field].items():
                            paragraphs = [x['text'] for x in value]
                            relevantParas = getRelevantParas(paragraphs, field)
                            relevantParas = paragraphs if len(relevantParas) == 0 else relevantParas

                            if (field in ["NONDIVERSIFIED", "AUDITOR"]):
                                res = extractTextFieldSpecific(relevantParas, field, className, key)
                                assignedValue, actualValue, extractedText = res
                            else:
                                extractedTexts = extractRelevantText(relevantParas, field, className)
                                if(field == 'NAV_REINSTATEMENT_ALLOWED'):
                                    for item in extractedTexts:
                                        item = item.strip().rstrip('.')
                                    for item in extractedTexts:
                                        item = item.strip('.').rstrip()            
                                extractedText = get_closest_extracted_string(extractedTexts, field, className)

                                assignedValue = bertValueAssignment.getValue(extractedText, field, className)
                                filtered_df = df[df[FIELD_NAME] == field]
                                filtered_df = filtered_df[filtered_df["Class Name"].str.contains(className)]
                                actualValue = -1
                                if len(filtered_df) > 0:
                                    actualValue = filtered_df.iloc[0]['Values to be attributed ']
                                if (actualValue == -1):
                                    filtered_df = df[df[FIELD_NAME] == field]
                                    filtered_df = filtered_df[filtered_df["Class Name"].str.contains("All Classes")]
                                    if len(filtered_df) > 0:
                                        actualValue = filtered_df.iloc[0]['Values to be attributed ']

                            if assignedValue != 'NOT_VALID':
                                if isinstance(paragraphs_document[field], list):
                                    printValues[(field, className)] = (assignedValue, actualValue, extractedText)
                                else:
                                    fundName = key
                                    printValues[(field, key)] = (assignedValue, fundName, extractedText)

                        with open("./final-output.csv","a") as f:
                            for key in printValues:
                                f.write("{},{},{},{},{},{}\n".format(rfId, key[0], key[1], printValues[key][0],
                                                                     printValues[key][1], printValues[key][2]))

    for key in printValues:
        # Print assigned values and information for identification
        print("RFID: {}\nField: {}\nFund: {}\nData String: {}\nValue: {}\n".format(rfId, key[0],
                                                                                     printValues[key][1],
                                                                                     printValues[key][2],
                                                                                     printValues[key][0]))
        print(key)


if __name__ == "__main__":
    assignValues("2840645")