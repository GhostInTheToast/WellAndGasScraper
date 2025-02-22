# Well Data Scraper and API

This project provides a solution for scraping well data based on API numbers, inserting the scraped data into a database, and serving the data through a Flask API.

## Project Overview

The project is broken down into three main components:

1. **Scraping Well Data**: The scraper fetches detailed well information from a public website for a list of API numbers.
2. **Database Insertion**: The scraped data is inserted into a SQLite database for easy retrieval and further processing.
3. **Flask API**: A Flask application serves as an API to query well data based on API numbers and geospatial polygons.

## Features

- Scraping well data, including operator, status, well type, and geospatial coordinates.
- Inserting the scraped data into a database.
- Avoiding throttling by introducing randomized sleep times between requests.
- Exposing well data via a Flask API.
- Retry mechanism to handle request failures during scraping.


You can use SQLite DB Browser (its free) to check the data in the db file. Itll look like this:

![test]("images/SQLiteDbBrowser.png")
