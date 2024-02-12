import pandas as pd
from pymongo import MongoClient
from pymongo import InsertOne
import numpy as np
import json
def parse_csv(filename):
    df = pd.read_csv(filename)
    # Replace commas in 'Value' column and convert it to numeric
    df['Value'] = pd.to_numeric(df['Value'].str.replace(',', ''), errors='coerce')
    # Drop rows with NaN values in 'Value' column
    df = df.dropna(subset=['Value'])
    return df

def create_documents(df):
    documents = []
    # Group by Year and Industry_name
    grouped = df.groupby(['Year', 'Industry_name_NZSIOC'])
    for group_name, group_df in grouped:
        document = {
            'Year': int(group_name[0]),  # Convert numpy.int64 to Python int
            'Industry_name': group_name[1],
            'Variables': []
        }
        # Iterate over each row in the group
        for index, row in group_df.iterrows():
            variable = {
                'Variable_name': row['Variable_name'],
                'Variable_category': row['Variable_category'],
                'Units': row['Units'],
                'Value': int(row['Value'])  # Convert numpy.int64 to Python int
            }
            document['Variables'].append(variable)
        documents.append(document)
    return documents



# Function to handle conversion of int64 to Python-native types
def convert_to_builtin_type(obj):
    if isinstance(obj, np.int64):
        return int(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


# Parse CSV file using pandas
filename = 'data.csv'
csv_data = parse_csv(filename)

# Create documents for MongoDB insertion
documents = create_documents(csv_data)

# Connect to MongoDB
client = MongoClient('mongodb+srv://gesantizo:FPuncake1*@cluster0.xopgwbl.mongodb.net')
db = client['Lab03']
collection = db['Lab03']

# # Export documents variable as JSON
# with open('documents.json', 'w') as json_file:
#     json.dump(documents, json_file, indent=4, default=convert_to_builtin_type)

# print("Exported documents to documents.json")
result = collection.insert_many(documents)

print(f"Inserted {len(result.inserted_ids)} documents.")
