import time
import random
from scraper import scrape_well_data, read_api_numbers
from database import insert_well_data
from src.database import initialize_db


def main():
    api_numbers = read_api_numbers('../data/apis_pythondev_test.csv')

    scraped_counter_total = 0
    for api_number in api_numbers[1:]:  # Skipping the first API (if needed)
        print(f"Scraping data for API number: {api_number}")

        well_data = scrape_well_data(api_number)
        print(well_data['operator'], 'is the operator lmao')

        if well_data is None:
            print(f"Skipping API {api_number} due to repeated failures.")
            continue  # Skip if scraping failed

        scraped_counter_total += 1
        print(f"Total successful scrapes: {scraped_counter_total}")

        # Add more randomized sleep to evade throttling
        sleep_time = random.uniform(3, 7)
        print(f"Sleeping for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

        print(well_data)

        initialize_db()

        if well_data:
            print(f"Inserting data for API {api_number} into database")
            insert_well_data(well_data)
        else:
            print(f"Failed to scrape data for API {api_number}")

if __name__ == "__main__":
    main()
