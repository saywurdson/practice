import subprocess
import os
import time
import dotenv

dotenv.load_dotenv(".env")

def main():
    # Ask the user for the files to extract. Keep asking until the user enters a valid input
    files_to_extract = input("Which files? (Enter any number(s) between 1 and 20, or 'all'): ")
    files_to_extract = files_to_extract.strip().split()

    while not all(map(str.isdigit, files_to_extract)) and files_to_extract != ["all"] and files_to_extract != ["exit"]:
        files_to_extract = input("Invalid argument. Please enter either any number(s) between 1 and 20, separated by spaces, or the word 'all', or type 'exit' to escape: ")
        files_to_extract = files_to_extract.strip().split()

    if files_to_extract == ["exit"]:
        return

    if files_to_extract == ["all"] or (all(map(str.isdigit, files_to_extract)) and all(map(lambda x: 1 <= int(x) <= 20, files_to_extract))):
        # Ask the user for the load method. Keep asking until the user enters a valid input
        load_method = input("Load method? (Enter 'overwrite' or 'append'): ")

        while load_method != "overwrite" and load_method != "append" and load_method != "exit":
            load_method = input("Invalid argument. Please enter either 'overwrite' or 'append', or type 'exit' to escape: ")

        if load_method == "exit":
            return
        
    # Check if the input is either "all" or any number(s) between 1 and 20. Wait to run the next file until the first file finishes.
    if files_to_extract == ["all"]:
        subprocess.run(["python", "/workspaces/dec-etl-project/etl/extract.py", "/workspaces/dec-etl-project/data/BASE_SYNPUF_INPUT_DIRECTORY", "all"], check=True)
        # wait for extract.py to finish before continuing
        while not os.path.exists(os.environ['BASE_SYNPUF_INPUT_DIRECTORY']):
            time.sleep(1)
        subprocess.run(["python", "/workspaces/dec-etl-project/etl/transform.py", "all"], check=True)
    elif all(map(str.isdigit, files_to_extract)) and all(map(lambda x: 1 <= int(x) <= 20, files_to_extract)):
        for file_number in files_to_extract:
            # start extract.py first, then run transform.py after extract.py finishes
            subprocess.run(["python", "/workspaces/dec-etl-project/etl/extract.py", "/workspaces/dec-etl-project/data/BASE_SYNPUF_INPUT_DIRECTORY", file_number], check=True)
            while not os.path.exists(os.environ['BASE_SYNPUF_INPUT_DIRECTORY']):
                time.sleep(1)
            subprocess.run(["python", "/workspaces/dec-etl-project/etl/transform.py", file_number], check=True)

    # Check if the input is either "overwrite" or "append"
    if load_method == "overwrite" or load_method == "append":
        subprocess.run(["python", "/workspaces/dec-etl-project/etl/load.py", load_method])
    else:
        return

if __name__ == "__main__":
    main()