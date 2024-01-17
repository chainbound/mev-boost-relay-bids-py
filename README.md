Relay Bids Metadata Retrieval Script
====================================

This Python script allows you to retrieve relay bids metadata from various relays using the `httpx` library. The script fetches data for a range of block numbers and saves it in a Parquet file. It is designed to work with a virtual environment to manage dependencies.

Installation
------------

To set up and run this script, follow these steps:

### 1\. Clone the Repository

Clone this repository to your local machine:

bashCopy code

```
git clone https://github.com/your-username/relay-bids-retrieval.git
```

### 2\. Create a Virtual Environment

Navigate to the project directory and create a virtual environment:

```
cd relay-bids-retrieval python -m venv venv
```

Activate the virtual environment:

On macOS and Linux:
```
source venv/bin/activate
```

On Windows:
```
venv\Scripts\activate
```

### 3\. Install Required Packages
Install the required packages listed in `requirements.txt`:

```
pip install -r requirements.txt
```

### 4\. Configure the Script

Edit the `RELAYS` dictionary and other parameters within the `main.py` script to configure it according to your requirements. 
You can also specify the number of concurrent requests by setting the `CONCURRENT_REQ` variable.

### 5\. Run the Script

Execute the `main.py` script with the following command, replacing `from_block`, `to_block`, and `filename` with your desired values:

```
python src/main.py from_block to_block filename
```

For example:
```
python main.py 100 200
```

This command will retrieve relay bids metadata for block numbers from 100 to 200 and save it to unique Parquet file per block_number in `outputs` folder.

Output
------
The script will log information about the progress and errors. It will save the retrieved data in Parquet format, splitting it into multiple files if the number of records exceeds a specified limit.