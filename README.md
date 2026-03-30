#  DoubanMovie-Analyzer

> A comprehensive data pipeline for Douban Top 250 Movies, featuring web scraping, data storage (MongoDB & MySQL), and visualization.

##  Project Overview
This project is designed to crawl movie rankings, titles, directors, ratings, and review counts from the Douban Top 250 list. It demonstrates a complete data workflow:
1.  **Data Extraction**: Scraping data using `requests` and `lxml`.
2.  **Data Storage**: Storing data in both **MongoDB** (NoSQL) and **MySQL** (Relational) databases.
3.  **Data Visualization**: Generating pie charts to analyze rating distributions.

##  Tech Stack
- **Language**: Python 3.x
- **Libraries**:
    - `requests`: HTTP requests
    - `lxml`: HTML parsing (XPath)
    - `pandas`: Data manipulation
    - `pymongo`: MongoDB interaction
    - `mysql-connector-python`: MySQL interaction
    - `matplotlib`: Data visualization

##  File Structure
- `mongodb.py`: The main script containing the crawler, database classes, and visualization logic.
- `README.md`: Project documentation.

##  How to Run

### 1. Prerequisites
- Python 3 installed.
- **MongoDB** service running locally (default port 27017).
- **MySQL** server running locally with a database named `douban_movies`.

### 2. Install Dependencies
```bash
pip install pandas requests lxml pymongo matplotlib mysql-connector-python
