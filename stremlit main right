import csv
import time
import random
import streamlit as st
import pandas as pd
import concurrent.futures
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Set page config
st.set_page_config(
    page_title="Amazon Seller Scraper",
    page_icon="🛍️",
    layout="wide"
)

# User-agent list to mimic different browsers
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; SM-A505FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:35.0) Gecko/20100101 Firefox/35.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0"
]

def random_delay(base_delay):
    """Add random delay between requests to avoid detection"""
    return random.uniform(base_delay - 20, base_delay + 20)

def format_text(text):
    """Format text with proper capitalization"""
    return " ".join(word.capitalize() for word in text.split())

def fetch_url(url, proxies=None, retries=3, base_delay=100):
    """Fetch URL with retries, exponential backoff, and proxy rotation"""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    proxy = random.choice(proxies) if proxies else None
    proxies_dict = {"http": proxy, "https": proxy} if proxy else None

    for attempt in range(retries):
        try:
            time.sleep(random_delay(base_delay))  # Initial delay
            response = requests.get(url, headers=headers, proxies=proxies_dict, timeout=10)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if hasattr(e.response, 'status_code') and e.response.status_code == 503:
                wait_time = base_delay * (2 ** attempt)  # Exponential backoff
                st.warning(f"503 Error for {url}. Retrying in {wait_time} seconds... {proxy}")
                time.sleep(wait_time)
            else:
                raise e
    raise requests.exceptions.HTTPError(f"Failed to fetch {url} after {retries} retries.")

def fetch_with_selenium(url):
    """Fetch URL using Selenium for JavaScript-heavy pages"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.get(url)
    
    # Wait for page load
    time.sleep(random.uniform(2, 5))
    content = driver.page_source
    driver.quit()
    return content

def extract_seller_info(url, proxies=None, base_delay=100, use_selenium=False):
    """Extract seller information from Amazon seller page"""
    try:
        if use_selenium:
            content = fetch_with_selenium(url)
            soup = BeautifulSoup(content, "html.parser")
        else:
            response = fetch_url(url, proxies, base_delay=base_delay)
            soup = BeautifulSoup(response.content, "html.parser")
    except Exception as e:
        return {"URL": url, "Error": str(e)}
    
    data = {key: "missing data" for key in [
        "URL", "Nom commercial", "Type d'activité", "Numéro de registre de commerce", "Numéro TVA",
        "Numéro de téléphone", "E-mail", "Possible Email"] + [f"Adresse commerciale {i+1}" for i in range(8)]}
    
    data["URL"] = url
    seller_info = soup.find("div", id="page-section-detail-seller-info") or soup.find("div", class_="a-box")
    
    if seller_info:
        # Extract seller information
        for key, label in {
            "Nom commercial": "Nom commercial:", "Type d'activité": "Type d'activité:",
            "Numéro de registre de commerce": "Numéro de registre de commerce:", "Numéro TVA": "Numéro TVA:",
            "Numéro de téléphone": "Numéro de téléphone:", "E-mail": "E-mail :"
        }.items():
            element = seller_info.find("span", text=lambda x: x and label in x)
            if element:
                data[key] = format_text(element.find_next("span").text.strip())
        
        # Look for possible email in text if not found
        if data["E-mail"] == "missing data":
            for span in seller_info.find_all("span"):
                if "@" in span.text:
                    data["Possible Email"] = span.text.strip().lower()
                    break
        
        # Extract address information
        address = seller_info.find("span", text=lambda x: x and "Adresse commerciale:" in x)
        if address:
            address_lines = address.find_next_siblings("div", class_="indent-left")
            for i, line in enumerate(address_lines[:8]):
                data[f"Adresse commerciale {i+1}"] = format_text(line.text.strip())
    
    return data

def save_to_csv(data_list, filename="amazon_sellers.csv"):
    """Save scraped data to CSV file"""
    headers = list(data_list[0].keys()) if data_list else []
    with open(filename, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        if file.tell() == 0:
            writer.writeheader()
        writer.writerows(data_list)

def main():
    st.title("🛍️ Amazon Seller Scraper")
    
    st.markdown("""
    This tool helps you extract seller information from Amazon seller pages.
    Please enter the URLs of the seller pages you want to scrape below.
    """)
    
    # Input section
    with st.expander("📝 Input Settings", expanded=True):
        urls_input = st.text_area(
            "Paste seller page URLs (one per line):",
            height=150,
            help="Enter Amazon seller page URLs, one per line"
        )
        urls = [url.strip() for url in urls_input.split("\n") if url.strip()]
        
        proxy_input = st.text_area(
            "Enter Proxies (one per line, optional):",
            height=100,
            help="Enter proxy addresses in format: http://ip:port or http://user:pass@ip:port"
        )
        proxies = [proxy.strip() for proxy in proxy_input.split("\n") if proxy.strip()]
    
    # Configuration section
    with st.expander("⚙️ Scraping Settings", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            delay = st.slider(
                "Random delay between requests (seconds)",
                45, 300, 200,
                help="Adding random delays helps avoid detection"
            )
        with col2:
            max_workers = st.slider(
                "Max concurrent requests",
                1, 5, 1,
                help="Higher values may increase speed but also increase detection risk"
            )
        
        use_selenium = st.checkbox(
            "Use Selenium for advanced scraping",
            help="Enable this if regular requests fail to capture data"
        )
    
    if st.button("🚀 Start Scraping") and urls:
        with st.spinner("Testing sample URLs..."):
            st.info(f"Checking {min(3, len(urls))} random URLs first to verify functionality...")
            test_urls = random.sample(urls, min(3, len(urls)))
            for test_url in test_urls:
                result = extract_seller_info(test_url, proxies, delay, use_selenium)
                if "Error" in result:
                    st.error(f"Error detected for {test_url}. Stopping process.")
                    return
        
        st.success("All test URLs passed. Proceeding with full scraping...")
        filename = f"amazon_sellers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        all_data, errors = [], []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(extract_seller_info, url, proxies, delay, use_selenium): url for url in urls}
            for index, future in enumerate(concurrent.futures.as_completed(futures)):
                result = future.result()
                if "Error" in result:
                    errors.append(result)
                else:
                    all_data.append(result)
                progress = (index + 1) / len(urls)
                progress_bar.progress(progress)
                status_text.text(f"Processing URL {index + 1} of {len(urls)}")
                time.sleep(random_delay(delay))
        
        save_to_csv(all_data, filename)
        
        st.success("✅ Scraping complete!")
        if errors:
            st.warning(f"⚠️ {len(errors)} URLs failed to scrape. Check the data for details.")
        
        # Download section
        st.download_button(
            "📥 Download CSV",
            open(filename, "rb"),
            file_name=filename,
            mime="text/csv",
            help="Click to download the scraped data"
        )
        
        # Display results preview
        if all_data:
            st.subheader("📊 Results Preview")
            df = pd.DataFrame(all_data)
            st.dataframe(df.head())

if __name__ == "__main__":
    main()
