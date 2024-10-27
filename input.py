import requests
import time
def main():
    ports = [8010,8011,8015,8016,8017]
    while True:
        user_input = int(input("Enter -1 to terminate the system or a valid value for the set of transactions or enter 0 to get the status of the system: "))
        
        if user_input == -1:
            print("Terminating...")
            break
        if user_input == 0:
            for port in ports:
                response = requests.post(f"http://127.0.0.1:{port}/status", json={"user_input": user_input})
            continue
        start_time = time.time()
        for port in ports:
            response = requests.post(f"http://127.0.0.1:{port}/process", json={"user_input": user_input})
        end_time = time.time()
        '''if response.status_code == 200:
            print(response.json())
        else:
            print(f"Failed to process request: {response.status_code}")'''

if __name__ == "__main__":
    main()