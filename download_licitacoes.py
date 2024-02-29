import requests
import os
import json
import sys
import time
import threading


class ConfirmedDownloads():
    # self.data dictionary format:
    # KEY = file_id
    # Attributes:
    #   cd_comprador
    #   cd_licitacao
    #   file_url
    #   file_path
    #   file_size

    # retrieve from disk if there is already a confirmed_downloads file
    def __init__(self):
        json_file_path = "confirmed_downloads.json"
        if os.path.exists(json_file_path):
            with open(json_file_path) as json_file:
                self.data = json.load(json_file)
        else:
            self.data = {}

        self.lock = threading.RLock()

    # save to disk
    def save(self):
        with self.lock:
            json_file_path = "confirmed_downloads.json"
            with open(json_file_path, 'w') as json_file:
                json.dump(self.data, json_file, indent=4)

    # check if file has already been downloaded
    def check(self, file_id):
        with self.lock:
            return file_id in self.data

    # add file to list of donwloaded files
    def add(self, file_id, cd_comprador, cd_licitacao, file_url, file_path, file_size):
        with self.lock:
            self.data[file_id] = {}
            self.data[file_id]["cd_comprador"] = cd_comprador
            self.data[file_id]["cd_licitacao"] = cd_licitacao
            self.data[file_id]["file_url"] = file_url
            self.data[file_id]["file_path"] = file_path
            self.data[file_id]["file_size"] = file_size

            self.save()


confirmed_downloads = ConfirmedDownloads()

def extract_file_id(file_url):
    return file_url[len('http://arquivos.portaldecompraspublicas.com.br/v1/download/'):]

filesystem_lock = threading.Lock()

def download_file(file_url, file_id, cd_comprador, cd_licitacao):
    print("Downloading file",file_id)

    with filesystem_lock:
        # Makes sure destination folder exists
        if not os.path.isdir("files"):
            os.makedirs("files")
        if not os.path.isdir("files/"+cd_licitacao):
            os.makedirs("files/"+cd_licitacao)

        # If file exists and we want to download it again, delete older version
        file_path = "files/"+cd_licitacao+"/"+file_id
        if os.path.exists(file_path):
            os.remove(file_path)

    # Send an HTTP GET request to the URL
    response = requests.get(file_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print("File downloaded successfully!")
    else:
        print("Failed to download file:", response.status_code)
        return False

    file_size = os.path.getsize(file_path)

    confirmed_downloads.add(file_id, cd_comprador, cd_licitacao, file_url, file_path, file_size)
    return True



json_file_path = sys.argv[1]
print("Loading file",json_file_path)

with open(json_file_path) as json_file:
    data = json.load(json_file)

print("Table Name:",data["table"])
table_size = len(data["rows"])
print("Table Size:",table_size)




def format_duration(seconds):
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    return "{} hours {} minutes {} seconds".format(int(hours), int(minutes), int(seconds))

time_start = time.time()
number_of_files_downloaded = 0
number_of_new_files_downloaded = 0
ammount_of_storage_used = 0

statistics_lock = threading.Lock()

def process_row(index, row):
    print("Processing index",index)
    global number_of_files_downloaded
    global number_of_new_files_downloaded
    global ammount_of_storage_used

    cd_comprador = str(row["cd_comprador"])
    cd_licitacao = str(row["cd_licitacao"])
    file_url = row["CONCAT('http://arquivos.portaldecompraspublicas.com.br/v1/download/', al.ID_ARQUIVO)"]
    if file_url == None:
        print("ERROR: FILE_URL IS NONE FOR cd_licitacao",cd_licitacao,", SKIPPING!")
        return False

    file_id = extract_file_id(row["CONCAT('http://arquivos.portaldecompraspublicas.com.br/v1/download/', al.ID_ARQUIVO)"])

    if confirmed_downloads.check(file_id):
        print(file_id,"has already been downloaded, skipping...")
        number_of_files_downloaded+=1
        ammount_of_storage_used+=confirmed_downloads.data[file_id]["file_size"]
        return True

    download_file(file_url, file_id, cd_comprador, cd_licitacao)


    with statistics_lock:
        number_of_files_downloaded+=1
        number_of_new_files_downloaded+=1
        ammount_of_storage_used+=confirmed_downloads.data[file_id]["file_size"]

        # calculates and displays statistics about remaining time and storage
        if number_of_new_files_downloaded > 0:
            elapsed_time = time.time() - time_start
            average_time_per_download = elapsed_time / number_of_new_files_downloaded
            remaining_number_of_files_to_download = table_size - number_of_files_downloaded
            estimated_remaining_time = remaining_number_of_files_to_download * average_time_per_download
            print("Estimated remaining time =", format_duration(estimated_remaining_time))
            print("Ammount of storage used =", int(ammount_of_storage_used / 10**6), "MBs")
            print("Estimated remaining storage needed =", int(remaining_number_of_files_to_download * (ammount_of_storage_used/number_of_files_downloaded) / 10**6), "MBs")

            print("Progress: {:.2f}%, {} out of {}".format((index+1)/table_size*100, index+1, table_size))

        print()

    return True


threads = []
for index, row in enumerate(data["rows"]):
    max_number_of_concurrent_threads = 15
    while len(threads) >= max_number_of_concurrent_threads:
        for t in threads[:]: # Iterate over a copy of the list, because we will be changing it inside the loop
            if not t.is_alive():
                t.join()
                threads.remove(t)

    t = threading.Thread(target=process_row,args=(index, row))
    threads.append(t)
    t.start()

