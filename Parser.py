import pandas as pd
from pymongo import MongoClient
from pymongo import InsertOne

def parse_csv(filename):
    df = pd.read_csv(filename)
    # Replace commas in 'Value' column
    df['Value'] = df['Value'].str.replace(',', '')
    df['Value'] = pd.to_numeric(df['Value'].str.replace(',', ''), errors='coerce')
    # Drop rows with NaN values in 'Value' column
    df = df.dropna(subset=['Value'])
    return df.to_dict('records')

def create_bulk_write_instructions(data):
    bulk_instructions = [InsertOne(item) for item in data]
    return bulk_instructions

def perform_bulk_write(collection, bulk_instructions):
    result = collection.bulk_write(bulk_instructions)
    return result

# Connect to MongoDB
client = MongoClient('mongodb+srv://gesantizo:FPuncake1*@cluster0.xopgwbl.mongodb.net/?retryWrites=true&w=majority')
db = client['Lab03']
collection = db['Lab03']

# Parse CSV file using pandas
filename = 'data.csv'
csv_data = parse_csv(filename)

# Create bulk write instructions
bulk_instructions = create_bulk_write_instructions(csv_data)

# Perform bulk write operation
result = perform_bulk_write(collection, bulk_instructions)

print(f"Inserted {result.inserted_count} documents.")
