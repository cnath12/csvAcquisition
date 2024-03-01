from time import sleep
import util as util
import os
import load_csv as loadCsv
import traceback

csv_db_connection_limit=5
csv_db_connection_count=0


def schedule():
    # list the dir
    folder_path=util.FOLDER_BASE_LOCATION
    if os.path.exists(folder_path):
        client_folder=[os.path.join(folder_path, folder,util.SPLIT) for folder in os.listdir(folder_path) if os.path.exists(os.path.join(folder_path, folder,util.SPLIT)) and os.path.isdir(os.path.join(folder_path, folder,util.SPLIT))]
        for folder in client_folder:
            loadCsv.process_available_csv_files(folder)


def main():
    try:
        print("scheduler started")
        while True:
            schedule()
            sleep(1)
    except Exception as e:
        stack_trace_str = traceback.format_exc()
        print(stack_trace_str)
        sleep(10)
        main()



if __name__ == "__main__":
    main()
