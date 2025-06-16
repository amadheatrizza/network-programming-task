import requests
import os
from html.parser import HTMLParser

SERVER_ADDRESS = "http://127.0.0.1:9977"

class FileListingParser(HTMLParser):
    def handle_data(self, data):
        if data.strip():
            print(f"> {data.strip()}")

def list_files():
    try:
        print(":: Retrieving file listing...")
        resp = requests.get(f"{SERVER_ADDRESS}/list")
        
        if resp.headers.get('Content-Type') == 'text/html':
            p = FileListingParser()
            p.feed(resp.text)
        else:
            print("Server replied with:")
            print(resp.text)
            
        print("++ File list retrieved")
        resp.raise_for_status()
    except requests.exceptions.RequestException as err:
        print(f"!! Failed to get files: {err}")

def upload_file():
    local_file = input("Enter local file path to upload: ")

    if not os.path.exists(local_file):
        print(f"!! Local file missing: {local_file}")
        return

    try:
        with open(local_file, 'rb') as f:
            data = f.read()

        headers = {'X-File-Name': local_file}
        
        print(f":: Sending {local_file}...")
        resp = requests.post(
            f"{SERVER_ADDRESS}/upload",
            data=data,
            headers=headers
        )

        print(f"Server response ({resp.status_code}):")
        print(resp.text)
        resp.raise_for_status()
        print("++ File transfer complete")

    except requests.exceptions.RequestException as err:
        print(f"!! Upload error: {err}")

def delete_file():
    filename = input("Enter filename to delete from server: ")
    
    try:
        print(f":: Removing {filename}...")
        resp = requests.delete(f"{SERVER_ADDRESS}/{filename}")

        print(f"Server response ({resp.status_code}):")
        print(resp.text)
        resp.raise_for_status()
        print("++ File removed")

    except requests.exceptions.RequestException as err:
        print(f"!! Delete failed: {err}")

def main():
    print("\nHTTP Client Menu:")
    print("1. List files on server")
    print("2. Upload file to server")
    print("3. Delete file from server")
    print("4. Exit")

    try:
        while True:
            choice = input("\nEnter your choice (1-4): ")
            
            if choice == '1':
                list_files()
            elif choice == '2':
                upload_file()
            elif choice == '3':
                delete_file()
            elif choice == '4':
                print('Good bye ;D')
                break
            else:
                print("!! Invalid choice - please enter 1-4")
    except KeyboardInterrupt:
        print('\nGood bye ;D')

if __name__ == "__main__":
    main()
