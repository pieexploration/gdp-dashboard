import csv
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# List of user-agent strings to mimic different browsers/devices
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; SM-A505FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36"
]

# Function to randomize delays between requests
def random_delay(base_delay=100):
    return random.uniform(base_delay - 10, base_delay + 10)  # Randomize delay slightly

# Function to fetch URL with retries
def fetch_url(url, retries=3, base_delay=100):
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    for attempt in range(retries):
        try:
            time.sleep(random_delay(base_delay))  # Random delay before request
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e} (Attempt {attempt + 1}/{retries})")
    return None

# Extract seller information
def extract_seller_info(url, base_delay=100):
    response = fetch_url(url, base_delay=base_delay)
    if not response:
        return {"URL": url, "Error": "Failed to fetch page"}
    
    soup = BeautifulSoup(response.content, "html.parser")
    data = {"URL": url}
    
    seller_info = soup.find("div", id="page-section-detail-seller-info")
    if not seller_info:
        seller_info = soup.find("div", class_="a-box a-spacing-none a-color-base-background box-section")
    
    if not seller_info:
        data["Error"] = "Seller info section not found"
        return data
    
    # Extracting relevant fields
    fields = [
        ("Nom commercial", "Nom commercial:"),
        ("Type d'activité", "Type d'activité:"),
        ("Numéro de registre de commerce", "Numéro de registre de commerce:"),
        ("Numéro TVA", "Numéro TVA:"),
        ("Numéro de téléphone", "Numéro de téléphone:"),
        ("E-mail", "E-mail:")
    ]
    
    for key, label in fields:
        element = seller_info.find("span", text=lambda x: x and label in x)
        data[key] = element.find_next("span").text.strip() if element else "missing data"
    
    return data

# Save data to CSV
def save_to_csv(data_list, filename="amazon_sellers.csv"):
    headers = ["URL", "Nom commercial", "Type d'activité", "Numéro de registre de commerce", "Numéro TVA", "Numéro de téléphone", "E-mail", "Error"]
    
    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data_list)

# Test with given URLs
urls = [
    "https://www.amazon.fr/sp?ie=UTF8&seller=A1VQSI08986K6B&asin=B0CRGPMTNX&ref_=dp_merchant_link&isAmazonFulfilled=1",
    "https://www.amazon.fr/sp?ie=UTF8&seller=ARD9JB1N4N25Z&asin=B0876CX6L6&ref_=dp_merchant_link&isAmazonFulfilled=1",
    "https://www.amazon.fr/sp?ie=UTF8&seller=A3E3G8LQHV2HAC&asin=B09X22FKDB&ref_=dp_merchant_link&isAmazonFulfilled=1",
    "https://www.amazon.fr/sp?ie=UTF8&seller=APCJLEOJ1IVNW&asin=B0D8L6VT41&ref_=dp_merchant_link&isAmazonFulfilled=1"
]

data_list = [extract_seller_info(url) for url in urls]
save_to_csv(data_list)
print("Scraping complete. Data saved to amazon_sellers.csv.")
