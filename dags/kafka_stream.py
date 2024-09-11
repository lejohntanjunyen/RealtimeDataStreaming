from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import json
import time
import logging
import requests

default_args = {
    "owner": "airscholar",
    "start_date": datetime(2024, 9, 10, 10, 00)
}

def get_data():
    """
    Fetches random user data from an external API.

    This function makes a GET request to the "https://randomuser.me/api/" API, 
    retrieves the response in JSON format, and extracts the first user result 
    from the response.

    Returns:
        dict: A dictionary containing details about a random user, such as 
              their name, gender, location, email, and other information.
    """
    # GET response to call randomuser API
    res = requests.get("https://randomuser.me/api/")
    # retrieves in json format
    res = res.json()
    # get the first user infomration generated
    res = res["results"][0]
    
    return res
    
def format_data(res):
    """
    Formats user data into a structured dictionary.

    This function takes a dictionary of user information from randomuser API, 
    and restructures it into a more organized format. It extracts specific fields such as 
    the user's name, gender, date of birth, address, contact information, and account details, 
    and organizes them into a dictionary.

    Args:
        res (dict): A dictionary containing raw user data from an API.

    Returns:
        dict: A dictionary containing formatted user information, including:
            - firstName (str): User's first name.
            - lastName (str): User's last name.
            - gender (str): User's gender.
            - dob (str): User's date of birth.
            - age (int): User's age.
            - address (str): User's formatted address.
            - coordinates (str): User's geographic coordinates (longitude, latitude).
            - email (str): User's email address.
            - contact (str): User's primary phone number.
            - contact2 (str): User's secondary phone number (cell).
            - accountOpenedDate (str): Date when the user's account was opened.
            - accountOpenedAge (int): Age of the account in years.
            - userId (str): User's unique identifier (UUID).
            - userName (str): User's username.
            - password (str): User's password.
            - profilePicture (str): URL of the user's profile picture.
    """
    # Initializes empty dictionary
    data = {}
    # Begin transfromation to store each information in its respective key
    data["firstName"] = res["name"]["first"]
    data["lastName"] = res["name"]["last"]
    data["gender"] = res["gender"]
    data["dob"] = res["dob"]["date"]
    data["age"] = res["dob"]["age"]
    
    data["address"] = "{}, {}, {}, {}, {}, {}".format(str(res["location"]["street"]["number"]),
                                   res["location"]["street"]["name"],
                                   res["location"]["city"], res["location"]["postcode"],
                                   res["location"]["state"], res["location"]["country"])
    data["coordinates"] = "{}, {}".format(res["location"]["coordinates"]["longitude"],
                                    res["location"]["coordinates"]["latitude"])
    
    data["email"] = res["email"]
    data["contact"] = res["phone"]
    data["contact2"] = res["cell"]
    
    data["accountOpenedDate"] = res["registered"]["date"]
    data["accountOpenedAge"] = res["registered"]["age"]
    
    data["userId"] = res["login"]["uuid"]
    data["userName"] = res["login"]["username"]
    data["password"] = res["login"]["password"]
    data["profilePicture"] = res["picture"]["medium"]
    
    return data

def stream_data():
    """
    Fetches, formats, and streams user data to a Kafka topic.

    This function retrieves user data from the randomuser API, formats it using the `format_data` function, 
    and streams the formatted data to a Kafka topic named "customerInfo". The function sends 
    data continuously for 60 seconds. In case of any errors during data streaming, the function 
    logs the error and continues the streaming process.

    The Kafka producer is set up with the following configuration:
        - bootstrap_servers: ['localhost:9092']
        - max_block_ms: 5000

    Behavior:
        - The function runs for 1 minute.
        - If data streaming encounters an error, it logs the error and retries.
        - Data is streamed to the "customerInfo" Kafka topic in JSON format.

    Args:
        None

    Returns:
        None
    """
    # configure Kafka connection 
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    # initialize current time
    current_time = time.time()
    # run function for a minute
    while True:
        if time.time() > current_time + 60: # 1 minute
            break
        else:
            try:
                # fetch, format user data from randomuser API
                response = get_data()
                response = format_data(response)
                # send transformed user data to Kafka
                producer.send(topic="customerInfo", value=json.dumps(response).encode('utf-8'))  
            except Exception as e:
                logging.ERROR(f"[ERROR] An error occured: {e}")
                continue
    
# Airflow DAG Computation
with DAG("user_automation",
         default_args=default_args, 
         schedule_interval="@daily", # scheduled to run daily
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id = "stream_data_from_api", # airflow task id specification
        python_callable = stream_data # stream_data function to be executed 
    )