# Extracting Fund Names and their Offsets from Mutual Fund Prospectus Documents

This python script extracts fund names and their HTML and text offsets from Mutual Fund Prospectus documents. It uses the OpenAI API to extract the fund names, and web scraping using BeautifulSoup to extract the HTML and text offsets. The extracted data is then inserted into a MongoDB database.

## Installation

1. Install the required Python packages:

   ```bash
   pip beautifulsoup4 requests pymongo openai
   ```

2. Obtain OpenAI API and MongoDB credentials:
   - Get OpenAI API key and save it in `Key.txt`.
   - Create a MongoDB database and save the connection string, database name, collection name in `mongodbConnectionParameters.txt`.
      - Line 1 should contain the connection string.
      - Line 2 should contain the database name.
      - Line 3 should contain the collection name.

3. Training Prompt
   - The training prompt for the OpenAI ChatGPT model is stored in a JSON file named `Task1GptTrainMessage.json`. This file contains the interactions used to train the model and is crucial for generating accurate responses related to mutual fund prospectuses.
   - If you need to modify the training prompt for any reason, you can do so by editing the `Task1GptTrainMessage.json` file. Ensure that any changes made align with the desired behavior you expect from the model.

## Usage

To extract fund names and their offsets from a Mutual Fund Prospectus document, run the following command:

```bash
python Task1Extracting_Fund_Names_and_Offsets.py your_rfid_here
```

Replace `your_rfid_here` with the RFID of the prospectus document.

## Methods

### `get_text_from_url(url, max_tags=50, max_length=6000)`

Retrieve text content from a URL by extracting text from the first N non-empty tags.

- **Parameters:**
  - `url` (str): The URL to fetch the content from.
  - `max_tags` (int): Maximum number of tags to consider.
  - `max_length` (int): Maximum length of the resulting text.

- **Returns:**
  - `str`: The extracted text content.

### `get_fund_names(Input_RFID)`

Retrieves the fund names associated with given RFIDs from mutual fund prospectuses.

- **Parameters:**
  - `Input_RFID` (list): List of RFIDs (strings) for which fund names need to be retrieved.

- **Returns:**
  - `target_strings` (dict): A dictionary mapping each RFID to its corresponding fund names.

### `getOffsets(Input_RFID,target_strings)`

Retrieves HTML and text offsets for given RFIDs and fund names.

- **Parameters:**
  - `Input_RFID` (list): List of RFIDs.
  - `target_strings` (dict): A dictionary mapping each RFID to its corresponding fund names.

- **Returns:**
    - `tags` (dict): HTML offsets for each fund name.
    - `text_tags` (dict): Text offsets for each fund name.

### `insertIntoMongoDB(Input_RFID, tags, text_tags)`

Inserts data into MongoDB collection.

- **Parameters:**
  - `Input_RFID` (list): List of RFIDs.
  - `tags` (dict): HTML offsets for each fund name.
  - `text_tags` (dict): Text offsets for each fund name.

### `task1(RFID)`

Main function for Task 1. Retrieves fund names, HTML, and text offsets and inserts data into MongoDB.

- **Parameters:**
  - `RFID` (str): RFID for which data is to be processed.

- **Usage Example:**

  ```python
  from main import task1

  RFID = "your_rfid_here"
  task1(RFID)
  ```

## Important Notes

- Ensure you have a stable internet connection.
- OpenAI API key is required; make sure to obtain and save it in `Key.txt`.
- MongoDB connection parameters are required; make sure to obtain and save it in `mongodbConnectionParameters.txt`.