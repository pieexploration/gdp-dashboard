import csv
import time
import random
import streamlit as st
import pandas as pd
import concurrent.futures
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# User-agent list to mimic different browsers
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; SM-A505FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36"
]

# Random delay function
def random_delay(base_delay):
    return random.uniform(base_delay - 10, base_delay + 10)

# Format text
def format_text(text):
    return " ".join(word.capitalize() for word in text.split())

# Fetch URL with retries
def fetch_url(url, proxy=None, retries=3, base_delay=100):
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    proxies = {"http": proxy, "https": proxy} if proxy else None
    
    for attempt in range(retries):
        try:
            time.sleep(random_delay(base_delay))
            response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if response.status_code == 503:
                st.warning(f"503 Error for {url}. Retrying ({attempt + 1}/{retries})...")
                time.sleep(random_delay(base_delay))
            else:
                raise e
    raise requests.exceptions.HTTPError(f"Failed to fetch {url} after {retries} retries.")

# Extract seller info
def extract_seller_info(url, proxy=None, base_delay=100):
    try:
        response = fetch_url(url, proxy, base_delay=base_delay)
    except requests.RequestException as e:
        return {"URL": url, "Error": str(e)}
    
    soup = BeautifulSoup(response.content, "html.parser")
    data = {key: "missing data" for key in [
        "URL", "Nom commercial", "Type d'activité", "Numéro de registre de commerce", "Numéro TVA",
        "Numéro de téléphone", "E-mail", "Possible Email"] + [f"Adresse commerciale {i+1}" for i in range(8)]}
    
    data["URL"] = url
    seller_info = soup.find("div", id="page-section-detail-seller-info") or soup.find("div", class_="a-box")
    if seller_info:
        for key, label in {
            "Nom commercial": "Nom commercial:", "Type d'activité": "Type d'activité:",
            "Numéro de registre de commerce": "Numéro de registre de commerce:", "Numéro TVA": "Numéro TVA:",
            "Numéro de téléphone": "Numéro de téléphone:", "E-mail": "E-mail :"
        }.items():
            element = seller_info.find("span", text=lambda x: x and label in x)
            if element:
                data[key] = format_text(element.find_next("span").text.strip())
        
        if data["E-mail"] == "missing data":
            for span in seller_info.find_all("span"):
                if "@" in span.text:
                    data["Possible Email"] = span.text.strip().lower()
                    break
        
        address = seller_info.find("span", text=lambda x: x and "Adresse commerciale:" in x)
        if address:
            address_lines = address.find_next_siblings("div", class_="indent-left")
            for i, line in enumerate(address_lines[:8]):
                data[f"Adresse commerciale {i+1}"] = format_text(line.text.strip())
    
    return data

# Save results to CSV
def save_to_csv(data_list, filename="amazon_sellers.csv"):
    headers = list(data_list[0].keys()) if data_list else []
    with open(filename, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        if file.tell() == 0:
            writer.writeheader()
        writer.writerows(data_list)

# Streamlit UI
def main():
    st.title("Amazon Seller Scraper")
    urls_input = st.text_area("Paste seller page URLs (one per line):")
    urls = [url.strip() for url in urls_input.split("\n") if url.strip()]
    proxy = st.text_input("Enter Proxy (Optional):")
    
    delay = st.slider("Random delay between requests (seconds)", 60, 300, 100)
    max_workers = st.slider("Max concurrent requests", 1, 5, 1)
    
    if st.button("Start Scraping") and urls:
        st.write(f"Checking 3 random URLs first to verify functionality...")
        test_urls = random.sample(urls, min(3, len(urls)))
        for test_url in test_urls:
            result = extract_seller_info(test_url, proxy, delay)
            if "Error" in result:
                st.error(f"Error detected for {test_url}. Stopping process.")
                return
        
        st.write(f"All test URLs passed. Proceeding with full scraping...")
        filename = f"amazon_sellers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        all_data, errors = [], []
        progress_bar = st.progress(0)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(extract_seller_info, url, proxy, delay): url for url in urls}
            for index, future in enumerate(concurrent.futures.as_completed(futures)):
                result = future.result()
                if "Error" in result:
                    errors.append(result)
                else:
                    all_data.append(result)
                progress_bar.progress((index + 1) / len(urls))
                time.sleep(random_delay(delay))
        
        save_to_csv(all_data, filename)
        st.success("Scraping complete! Download the results below.")
        with open(filename, "rb") as f:
            st.download_button("Download CSV", f, file_name=filename, mime="text/csv")

if __name__ == "__main__":
    main()

