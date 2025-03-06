import csv
import time
import random
import streamlit as st
import pandas as pd
import concurrent.futures
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# List of user-agent strings to mimic different browsers/devices
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; SM-A505FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36"
]

# Function to randomize delays between requests
def random_delay(base_delay):
    return random.uniform(base_delay - 10, base_delay + 10)  # Randomize delay slightly

# Function to format text (capitalize first letter of each word)
def format_text(text):
    return " ".join(word.capitalize() for word in text.split())

# Function to handle 503 errors and mimic human behavior
def fetch_url(url, proxy=None, retries=3, base_delay=100):
    headers = {
        "User-Agent": random.choice(USER_AGENTS)  # Randomize user-agent
    }
    proxies = {"http": proxy, "https": proxy} if proxy else None

    for attempt in range(retries):
        try:
            # Add a random delay before each request
            time.sleep(random_delay(base_delay))  # Random delay around base_delay
            response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if response.status_code == 503:
                st.warning(f"503 Error for {url}. Retrying ({attempt + 1}/{retries})...")
                time.sleep(random_delay(base_delay))  # Wait before retrying
            else:
                raise e
    raise requests.exceptions.HTTPError(f"Failed to fetch {url} after {retries} retries.")

# Extract seller information
def extract_seller_info(url, proxy=None, base_delay=100):
    try:
        response = fetch_url(url, proxy, base_delay=base_delay)
    except requests.RequestException as e:
        return {"URL": url, "Error": str(e)}

    soup = BeautifulSoup(response.content, "html.parser")

    # Initialize data dictionary with default values
    data = {
        "URL": url,
        "Nom commercial": "missing data",
        "Type d'activité": "missing data",
        "Numéro de registre de commerce": "missing data",
        "Numéro TVA": "missing data",
        "Numéro de téléphone": "missing data",
        "E-mail": "missing data",
        "Possible Email": "missing data",
        "Adresse commerciale 1": "missing data",
        "Adresse commerciale 2": "missing data",
        "Adresse commerciale 3": "missing data",
        "Adresse commerciale 4": "missing data",
        "Adresse commerciale 5": "missing data",
        "Adresse commerciale 6": "missing data",
        "Adresse commerciale 7": "missing data",
        "Adresse commerciale 8": "missing data"
    }

    # Try to find the seller info container in both possible structures
    seller_info = soup.find("div", id="page-section-detail-seller-info") or soup.find("div", class_="a-box a-spacing-none a-color-base-background box-section")

    if seller_info:
        # Extract Nom commercial
        nom_commercial = seller_info.find("span", text=lambda x: x and "Nom commercial:" in x)
        if nom_commercial:
            data["Nom commercial"] = format_text(nom_commercial.find_next("span").text.strip())

        # Extract Type d'activité
        type_activite = seller_info.find("span", text=lambda x: x and "Type d'activité:" in x)
        if type_activite:
            data["Type d'activité"] = format_text(type_activite.find_next("span").text.strip())

        # Extract Numéro de registre de commerce
        registre_commerce = seller_info.find("span", text=lambda x: x and "Numéro de registre de commerce:" in x)
        if registre_commerce:
            data["Numéro de registre de commerce"] = format_text(registre_commerce.find_next("span").text.strip())

        # Extract Numéro TVA
        num_tva = seller_info.find("span", text=lambda x: x and "Numéro TVA:" in x)
        if num_tva:
            data["Numéro TVA"] = format_text(num_tva.find_next("span").text.strip())

        # Extract Numéro de téléphone
        num_telephone = seller_info.find("span", text=lambda x: x and "Numéro de téléphone:" in x)
        if num_telephone:
            data["Numéro de téléphone"] = format_text(num_telephone.find_next("span").text.strip())

        # Extract E-mail
        email = seller_info.find("span", text=lambda x: x and "E-mail&nbsp;:" in x)
        if email:
            data["E-mail"] = email.find_next("span").text.strip().lower()

        # If email is still missing, search for any text containing "@"
        if data["E-mail"] == "missing data":
            for span in seller_info.find_all("span"):
                if "@" in span.text:
                    data["Possible Email"] = span.text.strip().lower()
                    break

        # Extract Adresse commerciale
        adresse = seller_info.find("span", text=lambda x: x and "Adresse commerciale:" in x)
        if adresse:
            address_lines = adresse.find_next_siblings("div", class_="indent-left")
            for i, line in enumerate(address_lines[:8]):
                data[f"Adresse commerciale {i+1}"] = format_text(line.text.strip())

    return data

# Save data to CSV
def save_to_csv(data_list, filename="amazon_sellers.csv"):
    headers = [
        "URL", "Nom commercial", "Type d'activité", "Numéro de registre de commerce", 
        "Numéro TVA", "Numéro de téléphone", "E-mail", "Possible Email", 
        "Adresse commerciale 1", "Adresse commerciale 2", "Adresse commerciale 3", 
        "Adresse commerciale 4", "Adresse commerciale 5", "Adresse commerciale 6",
        "Adresse commerciale 7", "Adresse commerciale 8"
    ]

    with open(filename, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        if file.tell() == 0:  # Write header only if file is empty
            writer.writeheader()
        for data in data_list:
            writer.writerow(data)

# Streamlit UI
def main():
    st.title("Amazon Seller Scraper")
    urls_input = st.text_area("Paste seller page URLs (one per line):")
    urls = urls_input.split('\n') if urls_input else []
    proxy = st.text_input("Enter Proxy (Optional):")

    # Recommended delay information
    st.write("### Recommended Delay Between Requests:")
    st.write("- **Minimum Delay:** 60-120 seconds between requests.")
    st.write("- **Moderate Delay:** 120-180 seconds for more cautious scraping.")
    st.write("- **Maximum Delay:** 180-300 seconds to stay under the radar and avoid detection.")

    # Settings
    max_workers = st.slider("Max concurrent requests", 1, 5, 1, help="Adjust the number of concurrent requests. Maximum is 5.")
    delay = st.slider("Random delay between requests (seconds)", 60, 300, 100, help="Add a random delay to avoid being blocked. Minimum delay is 60 seconds.")

    if st.button("Start Scraping") and urls:
        total_urls = len(urls)
        st.write(f"Detected {total_urls} URLs. Starting scraping...")

        all_data = []
        errors = []
        missing_info = []
        progress_bar = st.progress(0)
        status_text = st.empty()

        start_time = time.time()

        # Generate a unique filename for the CSV file
        filename = f"amazon_sellers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        # Display the download link and timer
        st.write(f"### Download Link:")
        st.write(f"Once scraping is complete, you can download the results here: [Download CSV](/{filename})")
        st.write(f"Estimated time remaining: Calculating...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(extract_seller_info, url.strip(), proxy, delay): url for url in urls if url.strip()}
            for index, future in enumerate(concurrent.futures.as_completed(futures)):
                result = future.result()
                if "Error" in result:
                    errors.append(result)
                else:
                    all_data.append(result)
                    # Check for missing email (both E-mail and Possible Email)
                    if result["E-mail"] == "missing data" and result["Possible Email"] == "missing data":
                        missing_info.append({"URL": result["URL"], "Missing Fields": "E-mail, Possible Email"})
                progress = (index + 1) / total_urls
                progress_bar.progress(progress)
                elapsed_time = time.time() - start_time
                estimated_total_time = elapsed_time / progress if progress > 0 else 0
                estimated_time_remaining = estimated_total_time - elapsed_time
                status_text.text(
                    f"Progress: {progress * 100:.2f}% | "
                    f"Elapsed time: {timedelta(seconds=int(elapsed_time))} | "
                    f"Estimated time remaining: {timedelta(seconds=int(estimated_time_remaining))}"
                )
                time.sleep(random_delay(delay))  # Random delay around user-specified delay

        # Save results to CSV
        save_to_csv(all_data, filename)

        # Display preview of the first 10 results
        st.write("### Preview of First 10 Results")
        preview_data = pd.DataFrame(all_data[:10])
        st.dataframe(preview_data)

        # Display error log (up to 10 errors)
        st.write("### Error Log (Up to 10 Errors)")
        if errors:
            error_log = pd.DataFrame(errors[:10])
            st.dataframe(error_log)
        else:
            st.write("No errors encountered.")

        # Display missing information log (up to 10 instances)
        st.write("### Missing Information Log (Up to 10 Instances)")
        if missing_info:
            missing_info_log = pd.DataFrame(missing_info[:10])
            st.dataframe(missing_info_log)
        else:
            st.write("No missing information found.")

        # Provide download link
        with open(filename, "rb") as f:
            st.download_button("Download CSV", f, file_name=filename, mime="text/csv")

        st.success("Scraping complete! You can now download the results.")

if __name__ == "__main__":
    main()
