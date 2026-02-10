import requests
import json

def verify_api():
    url = "http://localhost:8000/predict"
    data = {
        "sales_amount": 100.0,
        "quantity": 5,
        "product_key": 1,
        "year": 2023,
        "month": 10,
        "day": 25,
        "quarter": 4,
        "weekday_name": "Wednesday",
        "is_weekend": False
    }
    
    try:
        response = requests.post(url, json=data)
        if response.status_code == 200:
            print("API Verification Successful!")
            print("Response:", response.json())
        else:
            print(f"API Failed: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    verify_api()
