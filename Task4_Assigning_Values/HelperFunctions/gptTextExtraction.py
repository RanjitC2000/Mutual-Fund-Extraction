import pandas as pd  # Library for data manipulation and analysis
import openai  # OpenAI Python library for accessing the OpenAI GPT model
from sklearn.utils import shuffle  # Utility function for shuffling arrays
import re  # Library for regular expressions
import time  # Library for time-related functions
import json  # Library for handling JSON data
from unicodedata import normalize  # Library for Unicode normalization
import spacy  # Library for natural language processing tasks
from sentence_transformers import SentenceTransformer  # Library for encoding sentences into embeddings
import os

# Load the English language model from spaCy for text processing
nlp = spacy.load('en_core_web_sm', disable=['parser', 'ner'])

# Load the SentenceTransformer model for encoding sentences into embeddings
model = SentenceTransformer('bert-base-nli-mean-tokens')


def getAlphaNumeric(text):
    """
    Extracts alphanumeric characters from the provided text.

    Parameters:
    - text (str): The input text containing alphanumeric and non-alphanumeric characters.

    Returns:
    - temp (str): The extracted alphanumeric characters from the input text.
    """

    temp = ''  # Initialize an empty string to store alphanumeric characters
    for ch in text:  # Iterate through each character in the input text
        if ch.isalnum():  # Check if the character is alphanumeric
            temp += ch  # Append alphanumeric characters to the 'temp' string
    return temp  # Return the extracted alphanumeric characters



def clean_text(text):
    """
    Cleans the input text by performing normalization, removing extra spaces, and stripping leading/trailing spaces.

    Parameters:
    - text (str): The input text to be cleaned.

    Returns:
    - text (str): The cleaned and processed text.
    """

    text = normalize("NFKD", text)  # Normalizing the text using Unicode normalization form NFKD
    
    text = re.sub("\s+", " ", text)  # Substituting multiple spaces with a single space
    
    text = text.strip()  # Removing leading and trailing spaces
    
    return text  # Return the cleaned and processed text



def get_positive_examples(fieldName, className):
    """
    Retrieves positive examples based on the provided field name and class name.

    Parameters:
    - fieldName (str): The field name to filter positive examples.
    - className (str): The class name used for filtering.

    Returns:
    - field_class_filtered_ps (DataFrame): DataFrame containing positive examples filtered by field name and class name.
    """

    # Read the data from the specified Excel file into a DataFrame
    directory = os.path.abspath('./')
    df = pd.read_excel(directory + "\\Task4_Assigning_Values\\Data\\Requirement_Template_2.xlsx")

    # Filter the DataFrame based on the provided field name
    positive_samples = positive_samples[positive_samples['Field Name '] == fieldName]

    # Process the class name to extract alphanumeric characters
    className = getAlphaNumeric(className)
    className = "Class " + className[len('Class'):]

    # Filter positive samples based on the class name and concatenate the results
    field_class_filtered_ps = pd.concat([
        positive_samples[positive_samples["Class"].str.contains(className)],
        positive_samples[positive_samples["Class"].str.contains("All Classes")]
    ])

    # Shuffle the resulting DataFrame for randomness
    field_class_filtered_ps = shuffle(field_class_filtered_ps)

    return field_class_filtered_ps


def get_negative_examples(fieldName, className):
    """
    Retrieves negative examples based on the provided field name and class name.

    Parameters:
    - fieldName (str): The field name to filter negative examples.
    - className (str): The class name used for filtering.

    Returns:
    - field_class_filtered_ns (DataFrame): DataFrame containing negative examples filtered by field name and class name.
    """

    # Read the data from the specified CSV file into a DataFrame
    directory = os.path.abspath('./')
    negative_samples = pd.read_csv(directory + "\\Task4_Assigning_Values\\Data\\{}.csv".format(fieldName))

    # Filter the DataFrame based on the 'result' column to get negative samples
    negative_samples = negative_samples[negative_samples['result'] == 'negative']

    # Process the class name to extract alphanumeric characters
    className = getAlphaNumeric(className)
    className = "Class " + className[len('Class'):]

    # Filter negative samples based on the class name
    field_class_filtered_ns = negative_samples[negative_samples['class'] == className]

    # Shuffle the resulting DataFrame for randomness
    field_class_filtered_ns = shuffle(field_class_filtered_ns)

    return field_class_filtered_ns


def positive_text_clean(text):
    """
    Cleans positive text samples by removing special characters and a specific starting phrase.

    Parameters:
    - text (str): The input text to be cleaned.

    Returns:
    - text (str): The cleaned text after removing special characters and a specific phrase if it starts with it.
    """

    # Remove special characters except letters, numbers, spaces, and dots from the text
    text = re.sub(r'[^A-Za-z0-9 .]+', '', text)
    
    # Convert text to lowercase for case-insensitive comparison
    check = text.lower()
    
    # Check if the text starts with 'reinstatement privilege'
    if check.startswith('reinstatement privilege'):
        text = text[len('reinstatement privilege'):]  # Remove the specific phrase from the start of the text
    
    return text  # Return the cleaned text


def negative_text_clean(text):
    """
    Cleans negative text samples by removing special characters.

    Parameters:
    - text (str): The input text to be cleaned.

    Returns:
    - text (str): The cleaned text after removing special characters.
    """

    # Remove special characters except letters, numbers, spaces, and dots from the text
    text = re.sub(r'[^A-Za-z0-9 .]+', '', text)
    
    return text  # Return the cleaned text

def get_prompt_paras1(fieldName, className):
    """
    Retrieves prompt paragraphs for training based on the provided field name and class name.

    Parameters:
    - fieldName (str): The field name used for filtering prompt paragraphs.
    - className (str): The class name used for filtering prompt paragraphs.

    Returns:
    - final_df (DataFrame): DataFrame containing prompt paragraphs for training.
    """

    data = {"Field Name": [], "Value": [], "Text": [], "Class": [], "RFID": [], "Paragraph": []}
    final_df = pd.DataFrame(data)  # Initializing an empty DataFrame to store prompt paragraphs

    NUM_POSITIVE_SAMPLES = 6  # Number of positive samples required
    NUM_NEGATIVE_SAMPLES = 4  # Number of negative samples required

    # Retrieve positive examples and clean the 'How to derive the value' column
    field_class_filtered_ps = get_positive_examples(fieldName, className)
    field_class_filtered_ps['How to derive the value '] = [positive_text_clean(text) for text in field_class_filtered_ps['How to derive the value ']]

    # Retrieve negative examples and clean the 'text' column
    field_class_filtered_ns = get_negative_examples(fieldName, className)
    field_class_filtered_ns["text"] = [negative_text_clean(text) for text in field_class_filtered_ns["text"]]

    # Iterate over positive examples to collect prompt paragraphs
    for row in field_class_filtered_ps.iterrows():
        if NUM_POSITIVE_SAMPLES == 0:
            break

        # Create a sample series and append it to the final DataFrame
        sample = pd.Series([fieldName, row[1]['Values to be attributed '], row[1]['Data String'], className, row[1]['RFID'], row[1]['How to derive the value ']], index=final_df.columns)
        final_df = final_df.append(sample, ignore_index=True)
        NUM_POSITIVE_SAMPLES -= 1

    # Iterate over negative examples to collect prompt paragraphs (currently set to 0)
    for row in field_class_filtered_ns.iterrows():
        if NUM_NEGATIVE_SAMPLES == 0:
            break

        # Create a sample series and append it to the final DataFrame
        sample = pd.Series([fieldName, row[1]['value'], "None", className, row[1]['rfid'], row[1]['text']], index=final_df.columns)
        final_df = final_df.append(sample, ignore_index=True)
        NUM_NEGATIVE_SAMPLES -= 1
    
    return final_df  # Return the DataFrame containing prompt paragraphs for training



def generate_gpt_prompt(fieldName, className):
    """
    Generates GPT-3 prompts based on provided field and class names.

    Parameters:
    - fieldName (str): Name of the field.
    - className (str): Name of the class.

    Returns:
    - prompts (list): List of prompts containing system and user messages.
    """

    sheet = " Data & Excerpts"
    directory = os.path.abspath('./')
    df = pd.read_excel(directory + "\\Task4_Assigning_Values\\Data\\CompiledReq.xlsx", sheet_name=sheet)

    df = df[df['Field Name '] == fieldName]
    df = df[df["Class"].str.contains(className)]
    data_strings = df["Data String"].tolist()
    data_strings = list(set(data_strings))
    data_strings = data_strings[:20]

    df_keywords = pd.read_excel(directory + "\\Task4_Assigning_Values\\Data\\Keyword Dataset.xlsx")
    df_keywords = df_keywords[df_keywords['Field'] == fieldName]
    keywords = df_keywords["Keywords List"].tolist()

    
    prompts = []
    if("Class" in className or "class" in className):
        system_prompt = "Extract a value from the paragraph for "+ className +" that is similar to the given examples. Answer with only the value and no other information. If there is no such value in the paragraph, return \"None\". Here are the example sentences:"
    else:
        system_prompt = "Extract a value from the paragraph for Class "+ className +" that is similar to the given examples. Answer with only the value and no other information. If there is no such value in the paragraph, return \"None\". Here are the example sentences:"
    for data_string in data_strings:
        system_prompt+="\n- \""+ str(data_string).strip() +"\""
    system_prompt = system_prompt+ "the search criteria for the value is the below keywords " 
    for keyword in keywords:
        system_prompt+="\n- \""+ str(keyword).strip() +"\""

    # Modify system prompt based on specific field names
    if fieldName == "INCOME_FREQUENCY":
        system_prompt = """Your task is to extract the frequency that a fund intends to make income payments (dividends or interest) from given paragraphs. Provide the extracted frequency, or respond 'None' if no such frequency is found."""
        # Include specific examples and expected responses for INCOME_FREQUENCY
    elif fieldName == "AUDITOR":
        system_prompt = """Your task is to identify the name of the company acting as the independent accountant for the fund and auditing the fund's financial statements. Provide the company name, or respond 'None' if no such name is found."""
        # Include specific examples and expected responses for AUDITOR

    # Add the system prompt to the prompts list
    prompts.append({"role": "system", "content": system_prompt})

    # Generate additional prompts based on the field and class names if not INCOME_FREQUENCY or AUDITOR
    if fieldName != "INCOME_FREQUENCY" and fieldName != "AUDITOR":
        final_df = get_prompt_paras1(fieldName, className)
        for row in final_df.iterrows():
            prompts.append({"role": "user", "content": row[1]['Paragraph'].strip()})
            prompts.append({"role": "assistant", "content": "\"" + row[1]['Text'].strip() + "\""})
    
    return prompts  # Return the generated prompts

def extract_text_from_para(paragraph, fieldName, className):
    """
    Generates GPT-3 completion based on the provided paragraph, field, and class names.

    Parameters:
    - paragraph (str): The paragraph text to process.
    - fieldName (str): Name of the field.
    - className (str): Name of the class.

    Returns:
    - completion (object): GPT-3 generated completion.
    """

    # Generate training prompts based on the provided field and class names
    training_prompt = generate_gpt_prompt(fieldName, className)
    
    # Clean the paragraph text by removing special characters and non-alphabetic characters
    para_new = clean_text(re.sub(r'[^a-zA-Z.,\s]', '', paragraph))

    # Append the cleaned paragraph to the training prompts and write to a file
    training_prompt.append({"role": "user", "content": para_new})
    with open("./gpt.txt", "a") as f:
        json.dump(para_new + "\n\n\n\n\n\n", f)

    # Set up OpenAI API key for authentication
    key = ""
    try:
        directory = os.path.abspath('./')
        key = open(directory + "\\Key.txt").readlines()[0]
    except:
        print("ERROR: Reading openai Key.")
        sys.exit()
    
    openai.api_key = key

    # Generate GPT-3 completion based on the training prompts and paragraph
    completion = openai.ChatCompletion.create(
        model="gpt-4",  # GPT-4 model for completion
        messages=training_prompt,  # Training prompts and user messages
    )

    time.sleep(1.5)  # Delay to avoid immediate OpenAI request

    return completion  # Return the generated GPT-4 completion
