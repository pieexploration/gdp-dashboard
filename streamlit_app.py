import csv
import time
import random
import streamlit as st
import pandas as pd
import concurrent.futures
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# List of user-agents to mimic different browsers
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
]

# Function to randomize delay between requests
def random_delay(base_delay):
    return random.uniform(base_delay - 10, base_delay + 10)

# Function to fetch URL with retries
def fetch_url(url, retries=3, base_delay=100):
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    
    for attempt in range(retries):
        try:
            time.sleep(random_delay(base_delay))
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                st.warning(f"Error fetching {url}: {e}. Retrying ({attempt + 1}/{retries})...")
                time.sleep(random_delay(base_delay))
            else:
                return None

# Extract seller information
def extract_seller_info(url, base_delay=100):
    response = fetch_url(url, base_delay=base_delay)
    if not response:
        return {"URL": url, "Error": "Failed to fetch page"}
    
    soup = BeautifulSoup(response.content, "html.parser")
    data = {"URL": url, "Nom commercial": "not found", "Numéro TVA": "not found", "E-mail": "not found"}
    
    seller_info = soup.find("div", id="page-section-detail-seller-info")
    if seller_info:
        name = seller_info.find("span", text=lambda x: x and "Nom commercial:" in x)
        if name:
            data["Nom commercial"] = name.find_next("span").text.strip()
    
    return data

# Save data to CSV
def save_to_csv(data_list, filename="amazon_sellers.csv"):
    headers = ["URL", "Nom commercial", "Numéro TVA", "E-mail"]
    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data_list)

# Streamlit UI
def main():
    st.title("Amazon Seller Scraper")
    urls_input = st.text_area("Paste seller page URLs (one per line):")
    urls = [url.strip() for url in urls_input.split('\n') if url.strip()]
    delay = st.slider("Random delay between requests (seconds)", 60, 300, 100)
    
    if st.button("Start Scraping") and urls:
        st.write(f"Scraping {len(urls)} URLs...")
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(extract_seller_info, url, delay): url for url in urls}
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
        
        save_to_csv(results)
        st.success("Scraping complete! Download the results below.")
        df = pd.DataFrame(results)
        st.dataframe(df)
        
        with open("amazon_sellers.csv", "rb") as f:
            st.download_button("Download CSV", f, file_name="amazon_sellers.csv", mime="text/csv")

if __name__ == "__main__":
    main()
