import pulsar
from pulsar.schema import Record, String, Float, Long, JsonSchema
import json
import uuid
import os
import csv
import argparse

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: 'dotenv' module not found. Using default environment variables.")

# Use environment variables for Pulsar connection details
PULSAR_SERVICE_URL = os.getenv("PULSAR_SERVICE_URL")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
TOPIC = os.getenv("PULSAR_TOPIC_URL")
# Define the JSON schema as a class
class StockDataSchema(Record):
    symbol = String()
    date = String()
    openPrice = Float()
    closePrice = Float()
    highPrice = Float()
    lowPrice = Float()
    volume = Long()
    uuid = String()

# Create a Pulsar client
client = pulsar.Client(PULSAR_SERVICE_URL, authentication=pulsar.AuthenticationToken(AUTH_TOKEN))

# Create a producer with the specified JSON schema
producer = client.create_producer(
    TOPIC,
    schema=JsonSchema(StockDataSchema)  # Use the new JSON schema class here
)

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Process CSV data and produce JSON messages.')
parser.add_argument('csv_file', help='Path to the CSV file')
args = parser.parse_args()

# Read data from CSV file
csv_file_path = args.csv_file
with open(csv_file_path, "r") as csv_file:
    csv_reader = csv.reader(csv_file)
    # Skip the header row if present
    if csv_reader.line_num == 1:
        next(csv_reader)
    for row in csv_reader:
        symbol, date, open_price, close_price, high_price, low_price, volume = row
        # Generate a new UUID for each object
        uuid_str = str(uuid.uuid4())
        # Create a message that adheres to the schema
        message = StockDataSchema(
            symbol=symbol,
            date=date,
            openPrice=float(open_price),
            closePrice=float(close_price),
            highPrice=float(high_price),
            lowPrice=float(low_price),
            volume=int(volume),
            uuid=uuid_str
        )
        # Produce the JSON message directly
        producer.send(message)

# Close the producer and client
producer.close()
client.close()

print("JSON messages produced successfully from CSV data.")