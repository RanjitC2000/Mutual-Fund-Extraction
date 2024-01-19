# Import necessary libraries
import pandas as pd  # Library for data manipulation and analysis
from sklearn.metrics.pairwise import cosine_similarity  # Function to compute cosine similarity
from sentence_transformers import SentenceTransformer  # Library for encoding sentences into embeddings
from unicodedata import normalize  # Library for Unicode normalization
import re  # Library for regular expressions
import spacy  # Library for natural language processing tasks
import pickle  # Library for object serialization
import os

# Load the English language model from spaCy for text processing
nlp = spacy.load('en_core_web_sm', disable=['parser', 'ner'])

# Load the SentenceTransformer model for encoding sentences into embeddings
model = SentenceTransformer('bert-base-nli-mean-tokens')


def clean_text(text):
    """
    Cleans the input text by normalizing, removing special characters, stopwords, and extra spaces.

    Parameters:
    - text (str): The input text to be cleaned.

    Returns:
    - text (str): The cleaned and processed text.
    """

    # Normalize the text using Unicode normalization form NFKD
    text = normalize("NFKD", text)
    
    # Remove special characters (keep only alphanumeric characters and spaces)
    text = re.sub(r"[^\w\s]", "", text)
    
    # Tokenize the text, lemmatize each token, and remove stop words
    text = " ".join([token.lemma_ for token in nlp(text) if not token.is_stop])
    
    # Remove extra spaces by replacing multiple spaces with a single space
    text = re.sub("\s+", " ", text)
    
    # Strip leading and trailing spaces
    text = text.strip()
    
    return text  # Return the cleaned and processed text


def generate_embeddings(text):
    """
    Generates embeddings for a given text.

    Parameters:
    - text (str): The input text to generate embeddings for.

    Returns:
    - dataStringEncoding[0]: The generated embeddings for the text.
    """

    text = clean_text(text)  # Clean the text (possibly a pre-processing step)
    dataStringEncoding = model.encode([text])  # Encode the cleaned text using a model
    return dataStringEncoding[0]  # Return the generated embeddings for the text



def filter_paragraphs(para, field):
    """
    Filters a paragraph based on a specified field using a pre-trained model.

    Parameters:
    - para (str): The input paragraph to be filtered.
    - field (str): The field used for filtering.

    Returns:
    - result[0][1]: The filtered result based on the specified field.
    """

    # Open the pre-trained model corresponding to the specified field
    directory = os.path.abspath('./')
    with open(directory + '\\Task4_Assigning_Values\\FilteringModels\\'+field+'.pkl', 'rb') as f:
        model = pickle.load(f)  # Load the pre-trained model
        f.close()  # Close the file stream
    
    embeddings = generate_embeddings(para)  # Generate embeddings for the paragraph
    
    # Predict the probability of filtering for the provided embeddings
    result = model.predict_proba([embeddings])
    
    return result[0][1]  # Return the filtering result based on the specified field
