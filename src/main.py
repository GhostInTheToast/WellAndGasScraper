import time
import random
from scraper import scrape_well_data, read_api_numbers
from database import insert_well_data
from src.database import initialize_db


# This code is for doing the scraping using that scraper code etc.
# then after this is done, you can run app.py instead and test api

def main():
    api_numbers = read_api_numbers('../data/apis_pythondev_test.csv') # extracting the api numbers

    # counter is for debugging purposes and progress tracking.
    scraped_counter_total = 0
    for api_number in api_numbers:
        print(f"Scraping data for API number: {api_number}")
        well_data = scrape_well_data(api_number)

        print(well_data)

        if well_data is None:
            print(f"Skipping API {api_number} due to repeated failures.")
            continue  # Skip to next api if scraping failed

        scraped_counter_total += 1
        print(f"Total successful scrapes so far: {scraped_counter_total}")

        # Add randomized sleep to evade throttling
        sleep_time = random.uniform(3, 7)
        print("\n" + f"Sleeping for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)


        # This section is doing a bunch of database stuff and adding this stuff to the db
        initialize_db() # Make the db if it doesnt exist yet

        if well_data:
            print(f"Inserting data for API {api_number} into database")
            insert_well_data(well_data) # Insert the record into the db
        else:
            print(f"Failed to scrape data for API {api_number}")

if __name__ == "__main__":
    main()
