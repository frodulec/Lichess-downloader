import requests
from queue import Queue
import datetime
import threading
import os



def download_file(url):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)
    return local_filename


def download_worker(downloading_queue):
    while True:
        new_url = downloading_queue.get()
        print(datetime.datetime.now(), 'downloading', new_url)
        if os.path.isfile('./lichess_data/' + new_url.split('/')[-1]):
            print(datetime.datetime.now(), 'Skipping,', new_url.split('/')[-1], 'exists.')
        else:
            try:
                download_file(new_url)
                # pass
            except Exception as e:
                print(datetime.datetime.now(), e)


if __name__ == '__main__':
    downloads_queue = Queue()

    years = ['2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    urls = []
    for year in years:
        for month in months:
            urls.append('https://database.lichess.org/standard/lichess_db_standard_rated_' + year + '-' + month + '.pgn.zst')
    for url in urls:
        downloads_queue.put(url)
    threading.Thread(target=download_worker, args=[downloads_queue]).start()
    # threading.Thread(target=download_worker, args=[downloads_queue]).start()
    # threading.Thread(target=download_worker, args=[downloads_queue]).start()
    # threading.Thread(target=download_worker, args=[downloads_queue]).start()
    # threading.Thread(target=download_worker, args=[downloads_queue]).start()
