from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airscholar",
    "start_date": datetime(2024, 8, 28, 10, 00)
}

def get_data():
    import requests
    
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res["results"][0]
    # print(json.dumps(res, indent=3))
    return res
    
get_data()

def format_data(res):
    data = {}
    data["firstName"] = res["name"]["first"]
    data["lastName"] = res["name"]["last"]
    data["gender"] = res["gender"]
    data["dob"] = res["dob"]["date"]
    data["age"] = res["dob"]["age"]
    data["address"] = res["location"]["street"]["number"] + " " + \
                            res["location"]["street"]["name"] + "," + \
                            res["location"]["city"] + "," + \
                            res["location"]["state"] + \
                            res["location"]["postcode"] + "," + \
                            res["location"]["country"]
    data["coordinates"] = res["location"]["coordinates"]["longitude"] + "," + \
                            res["location"]["coordinates"]["latitude"]
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
    response = get_data()
    response = format_data(response)
    
    print(json.dumps(response, indent=3))
    
stream_data()
    
# with DAG("user_automation",
#          default_args=default_args,
#          schedule_interval="@daily",
#          catchup=False) as dag:
    
#     streaming_task = PythonOperator(
#         task_id = "stream_data_from_api",
#         python_callable = stream_data
#     )