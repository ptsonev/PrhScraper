# This affects only the PDF and HTML downloader.
# Scraping the IDs and parsing the files is single threaded.
THREADS = 15

# The folder where the HTML and PDF files will be saved.
DATA_DIR = './DATA/'
OUTPUT_CSV_FILE = 'output.csv'
ID_FILE = 'ids.jsonl'

# Change the username and the password with your own.
# Let me know when you register a BrightData account and I will record a short video showing you how to set up a zone and whitelist your IP - it is not hard.
BRIGHT_DATA_USERNAME = 'brd-customer-hl_d1d3483b-zone-prh_proxy'
BRIGHT_DATA_PASSWORD = 'b2mmef4qp9oi'

# DON'T CHANGE THIS
# The script will work with most proxy providers, but BrightData is probably the best in this case
ROTATING_PROXY = f'http://{BRIGHT_DATA_USERNAME}-session-random{{session}}:{BRIGHT_DATA_PASSWORD}@brd.superproxy.io:22225'
