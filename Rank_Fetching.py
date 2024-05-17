import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

def google_search(keyword, num_results=50):
    """Searches Google for the given keyword and returns a list of result URLs."""
    query = str(keyword).replace(' ', '+')  # Convert keyword to string
    url = f"https://www.google.com/search?q={query}&num={num_results}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    result_urls = []
    for g in soup.find_all('div', class_='g'):
        anchor = g.find('a')
        if anchor:
            link = anchor['href']
            result_urls.append(link)
    
    return result_urls

def check_link_ranking(search_results, link):
    """Checks the ranking of the provided link in the search results."""
    try:
        rank = search_results.index(link) + 1
    except ValueError:
        rank = -1
    return rank

def main(df, keyword_column, link_column, rank_column):
    num_rows = int(input("Enter the number of rows to consider from the file: "))
    num_rows = min(num_rows, len(df))

    for index, row in df.head(num_rows).iterrows():
        keyword = row[keyword_column]
        link_to_check = row[link_column]
        
        if pd.isna(keyword) or pd.isna(link_to_check):
            print(f"Skipping row {index} due to missing keyword or link.")
            df.at[index, rank_column] = -1
            continue
        
        # Convert to string
        keyword = str(keyword)
        link_to_check = str(link_to_check)
        
        # Perform the Google search with a delay between requests
        print(f"Searching for '{keyword}' on Google...")
        search_results = google_search(keyword)
        
        # Check the ranking of the provided link
        rank = check_link_ranking(search_results, link_to_check)
        
        if rank != -1:
            print(f"{keyword} - {link_to_check} - #{rank}")
            # Update the rank in the DataFrame
            df.at[index, rank_column] = rank
        else:
            print(f"{keyword} - {link_to_check} - Not found in the top {len(search_results)}")
            # If not found, mark the rank as -1 in the DataFrame
            df.at[index, rank_column] = -1

        # Add a delay of 2 seconds between requests to avoid triggering rate limits
        time.sleep(2)

    # Write the updated DataFrame back to the file
    file_extension = file_path.split('.')[-1]
    if file_extension == 'xlsx':
        df.to_excel('updated_' + file_path, index=False)
    elif file_extension == 'csv':
        df.to_csv('updated_' + file_path, index=False)

if __name__ == "__main__":
    file_path = input("Enter the path of the file (CSV or XLSX): ")
    file_extension = file_path.split('.')[-1]
    
    if file_extension == 'xlsx':
        df = pd.read_excel(file_path)
    elif file_extension == 'csv':
        df = pd.read_csv(file_path)
    else:
        print("Unsupported file format. Please provide a CSV or XLSX file.")
        exit()

    print("Columns available in the file:")
    for i, column in enumerate(df.columns):
        print(f"{i}: {column}")

    keyword_column = input("Enter the column name to use for keywords: ")
    link_column = input("Enter the column name to use for URLs: ")
    rank_column = input("Enter the column name to update the ranking: ")

    if rank_column not in df.columns:
        df[rank_column] = ''

    main(df, keyword_column, link_column, rank_column)

