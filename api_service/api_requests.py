import requests
import json


if __name__ == '__main__':
    response = requests.get('http://127.0.0.1:5000/status')
    print(response.text)

    response = requests.get("http://127.0.0.1:5000/users/with_most_followers", params={"limit": "10"})
    
    # Check if the response status code indicates success
    if response.status_code == 200:
        try:
            # Attempt to decode the JSON response
            json_response = response.json()
            print(json_response)
        except ValueError:
            # Handle the case where JSON decoding fails
            print("Response content is not valid JSON:", response.text)
    else:
        # Handle unsuccessful responses
        print("Request failed with status code:", response.status_code)

    response = requests.get("http://127.0.0.1:5000/users/user_followers", params={"user_id": "40981798"})

    # Check if the response status code indicates success
    if response.status_code == 200:
        try:
            # Attempt to decode the JSON response
            json_response = response.json()
            print(json_response)
        except ValueError:
            # Handle the case where JSON decoding fails
            print("Response content is not valid JSON:", response.text)
    else:
        # Handle unsuccessful responses
        print("Request failed with status code:", response.status_code)




    response = requests.get("http://127.0.0.1:5000/users/user_follows", params={"user_id": "40981798"})

    # Check if the response status code indicates success
    if response.status_code == 200:
        try:
            # Attempt to decode the JSON response
            json_response = response.json()
            print(json_response)
        except ValueError:
            # Handle the case where JSON decoding fails
            print("Response content is not valid JSON:", response.text)
    else:
        # Handle unsuccessful responses
        print("Request failed with status code:", response.status_code)
                            