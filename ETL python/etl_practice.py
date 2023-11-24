import glob
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df

def extract_from_json(file_to_process):
    df = pd.read_json(file_to_process, lines = True)
    return df

def extract_from_xml(file_to_process):
    df = pd.DataFrame(columns = ['car_model', 'year_of_manufacture',
     'price',
    'fuel'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root:
        name = car.find("car_model").text
        year_of_manufacture = float(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        df = pd.concat([df, pd.DataFrame([{"car_model": name,
        "year_of_manufacture" : year_of_manufacture,
        "price" : price,
        "fuel" : fuel}])], ignore_index = True)
    return df

def extract():

    extracted_data = pd.DataFrame(columns=[
        'car_model',
        'year_of_manufacture',
        'price',
        'fuel'
    ]) # empty df to hold all the data

    # process all csv files
    for csvfile in glob.glob('*.csv'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index = True)
    
    print("CSV File added: \n", extracted_data)

    # process all the json
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index = True )

    print("JSON File added: \n", extracted_data)

    # process all the xml
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index= True)

    print("XML file added: \n", extracted_data)
    return extracted_data

def transform(data):
    """
    Convert the in. to meters 
    rounding to 2 decimal 
    """

    data['price'] = round(data.price , 2)

    '''Convert lbs. to kg and rounding to 2 decimals '''
    #data['weight'] = round(data.weight * 0.45359237, 2)

    return data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_progress(message):
    # Year-Month-Day - hour-minute-second
    timestamp_format = "%Y-%h-%d-%H:%M:%S" 
    now = datetime.now() #get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')


# Initializing the process
log_progress("ETL Job Started")

# log the start of extraction 
log_progress("Extract phase started")
extracted_data = extract()

# compleiton of the Extraction process
log_progress("Extraction phase completed")

# log the completion of the Extraction process 
log_progress("Transform phase started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print('-' * 25)
print(transformed_data)

# log completion of transform process
log_progress("Transfrom phase completed")

# log start of loading process
log_progress("Load phase started")
load_data(target_file, transformed_data)

# log the completionn of loading process
log_progress("Load phase ended")

# log completion
log_progress("ETL job completed")