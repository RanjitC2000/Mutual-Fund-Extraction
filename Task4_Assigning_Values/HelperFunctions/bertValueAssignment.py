# Importing necessary libraries/modules
import pandas as pd  # Pandas library for data manipulation and analysis
from sklearn.metrics.pairwise import cosine_similarity  # Cosine similarity metric
from sentence_transformers import SentenceTransformer  # SentenceTransformer library for sentence embeddings
import os


# Function to extract alphanumeric characters from a string
def getAlphaNumeric(text):
    """
    Extracts alphanumeric characters from the provided text.

    Parameters:
    - text (str): The input text containing alphanumeric and non-alphanumeric characters.

    Returns:
    - temp (str): The extracted alphanumeric characters from the input text.
    """

    temp = ''
    for ch in text:
        if ch.isalnum():  # Checking if the character is alphanumeric
            temp += ch  # Appending alphanumeric characters to the 'temp' string
    return temp


# Function to retrieve ground truths based on field name and class name
def get_ground_truths(fieldName, className):
    """
    Retrieves ground truths based on the provided field name and class name.

    Parameters:
    - fieldName (str): The name of the field.
    - className (str): The name of the class.

    Returns:
    - df (DataFrame): The filtered dataframe containing ground truth information.
    """

    # Reading data from an Excel file containing template information
    directory = os.path.abspath('./')
    df = pd.read_excel(directory + "\\Task4_Assigning_Values\\Data\\Requirement_Template_2.xlsx")
    
    # Filtering the data based on the provided field name
    df = df[df["Field Name "] == fieldName]
    
    # Processing the class name to make it alphanumeric and suitable for comparison
    className = getAlphaNumeric(className)
    className = "Class " + className[len('Class'):]
    
    # Filtering the dataframe based on the processed class name and 'All Classes' strings
    df = pd.concat([df[df["Class"].str.contains(className)], df[df["Class"].str.contains("All Classes")]])

    return df  # Return the filtered dataframe


# Function to get the most probable data string value from given options
def getDataStringValue(dataString, uniqueValues, model, valueSentences):
    """
    Determines the most probable data string value from given options.

    Parameters:
    - dataString (str): The input data string to compare with unique values.
    - uniqueValues (list): List of unique values from ground truth data.
    - model: The sentence transformer model for encoding.
    - valueSentences (dict): Dictionary with unique values as keys and associated sentences as values.

    Returns:
    - maxValue: The most probable value determined from the given data string.
    """

    # Encode the given data string using the provided model
    dataStringEncoding = model.encode([dataString])

    valueScores = []
    for value in uniqueValues:
        # Encode sentences related to each unique value
        encodings = model.encode(valueSentences[value])
        score = cosine_similarity(dataStringEncoding, encodings).tolist()  # Calculate cosine similarity
        score = sum(score[0]) / len(score[0])  # Calculate the average similarity score
        valueScores.append((value, score))  # Store value and its similarity score

    maxValue, maxScore = 0, -1 * float("inf")
    for value, score in valueScores:
        if score > maxScore:
            maxValue, maxScore = value, score  # Update the maximum value and its score

    return maxValue  # Return the value with the highest similarity score


# Function to get the value based on predicted text, field name, and class name
def getValue(predicted_text, fieldName, className):
    """
    Determines the value based on predicted text, field name, and class name.

    Parameters:
    - predicted_text (str): The predicted text to match against ground truth values.
    - fieldName (str): The name of the field.
    - className (str): The name of the class.

    Returns:
    - value: The determined value based on similarity with ground truth values.
    """

    if fieldName == 'INITPURCHASE_AMOUNT':
            return int(predicted_text)
    if fieldName == 'SUBPURCHASE_AMOUNT':
        if predicted_text in ['Yes', 'No', 'None', '0', '1', '10', '25', '50', '100', '1,000', '5,000','subsequent','additional','least']:
            return predicted_text
        elif int(predicted_text) in ['Yes', 'No', 'None', '0', '1', '10', '25', '50', '100', '1,000', '5,000','subsequent','additional','least']:
            return int(predicted_text)
        else: return -1

    groundTruths = get_ground_truths(fieldName, className)

    uniqueValues = list(set(groundTruths['Values to be attributed '].unique()))
    
    valueSentences = dict()
    for val in uniqueValues:
        valueSentences[val] = groundTruths[groundTruths['Values to be attributed '] == val]["Data String"].tolist()

    model = SentenceTransformer('bert-base-nli-mean-tokens')

    value = getDataStringValue(predicted_text, uniqueValues, model, valueSentences)

    return value
