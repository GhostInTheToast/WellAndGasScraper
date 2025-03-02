import time
import random
import logging
from scraper import scrape_well_data, read_api_numbers
from database import insert_well_data, read_database, clear_database
from src.database import initialize_db

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Main entry point for scraping and inserting well data into the database
def main2():
    """
    Main function for scraping well data for a list of API numbers and inserting it into the database.

    Steps:
    1. Reads the list of API numbers from a CSV file.
    2. Scrapes well data for each API number using the scraper.
    3. Inserts the scraped data into the database.
    4. Tracks the progress and handles failures.
    5. Avoids throttling by sleeping for a randomized duration between requests.
    """
    # Reading the API numbers from the CSV file
    api_numbers = read_api_numbers('../data/apis_pythondev_test.csv')  # Extracting the API numbers

    # Counter for tracking successful scrapes
    scraped_counter_total = 0
    for api_number in api_numbers:
        # Scraping the well data for the current API number
        well_data = scrape_well_data(api_number)

        if well_data is None:
            logging.warning(f"Skipping API {api_number} due to repeated failures.")
            continue  # Skipping the current API number if scraping failed

        # Incrementing the successful scrape counter
        scraped_counter_total += 1
        logging.info(f"Total successful scrapes so far: {scraped_counter_total}")

        # Initializing the database if it doesn't exist yet
        initialize_db()  # Making the database if it doesn't exist

        # Inserting the scraped data into the database
        if well_data:
            logging.info(f"Inserting data for API {api_number} into the database.")
            insert_well_data(well_data)  # Inserting the record into the database
        else:
            logging.error(f"Failed to scrape data for API {api_number}")

        # Adding randomized sleep to evade throttling
        sleep_time = random.uniform(3, 7)
        logging.info(f"Sleeping for {sleep_time:.2f} seconds...\n\n")
        time.sleep(sleep_time)

    read_database()

def main():
    # clear_database()
    read_database()

# Running the main function when the script is executed
if __name__ == "__main__":
    main()
