import csv
import time
import random
import streamlit as st
import pandas as pd
import concurrent.futures
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# Extract seller information
def extract_seller_info(url, proxy=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    proxies = {"http": proxy, "https": proxy} if proxy else None

    try:
        response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
        response.raise_for_status()
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
        "Adresse commerciale 7": "missing data",  # New column
        "Adresse commerciale 8": "missing data"   # New column
    }

    seller_info = soup.find("div", id="page-section-detail-seller-info")
    if seller_info:
        # List of possible email labels
        email_labels = ["E-mail", "email", "EMAIL", "e-mail", "mail"]

        # Extract email using all possible labels
        for label in email_labels:
            email_label = seller_info.find("span", text=lambda x: x and label in x)
            if email_label:
                email_span = email_label.find_next("span")
                if email_span:
                    data["E-mail"] = email_span.text.strip()
                    break  # Stop searching once email is found

        # If email is still missing, search for any text containing "@"
        if data["E-mail"] == "missing data":
            for span in seller_info.find_all("span"):
                if "@" in span.text:
                    data["Possible Email"] = span.text.strip()
                    break

        # Extract other fields
        for row in seller_info.find_all("div", class_="a-row"):
            if "Nom commercial:" in row.text:
                data["Nom commercial"] = row.find("span", class_=False).text.strip()
            elif "Type d'activité:" in row.text:
                data["Type d'activité"] = row.find("span", class_=False).text.strip()
            elif "Numéro de registre de commerce:" in row.text:
                data["Numéro de registre de commerce"] = row.find("span", class_=False).text.strip()
            elif "Numéro TVA:" in row.text:
                data["Numéro TVA"] = row.find("span", class_=False).text.strip()
            elif "Numéro de téléphone:" in row.text:
                data["Numéro de téléphone"] = row.find("span", class_=False).text.strip()
            elif "Adresse commerciale:" in row.text:
                address_lines = row.find_next_siblings("div", class_="indent-left")
                for i, line in enumerate(address_lines[:8]):  # Updated to 8 columns
                    data[f"Adresse commerciale {i+1}"] = line.text.strip()

    return data

# Save data to CSV
def save_to_csv(data_list, filename="amazon_sellers.csv"):
    headers = [
        "URL", "Nom commercial", "Type d'activité", "Numéro de registre de commerce", 
        "Numéro TVA", "Numéro de téléphone", "E-mail", "Possible Email", 
        "Adresse commerciale 1", "Adresse commerciale 2", "Adresse commerciale 3", 
        "Adresse commerciale 4", "Adresse commerciale 5", "Adresse commerciale 6",
        "Adresse commerciale 7", "Adresse commerciale 8"  # New columns
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

    # Settings
    max_workers = st.slider("Max concurrent requests", 1, 10, 5, help="Adjust the number of concurrent requests.")
    delay = st.slider("Random delay between requests (seconds)", 1, 10, 3, help="Add a random delay to avoid being blocked.")

    if st.button("Start Scraping") and urls:
        total_urls = len(urls)
        st.write(f"Detected {total_urls} URLs. Starting scraping...")

        all_data = []
        errors = []
        missing_info = []
        progress_bar = st.progress(0)
        status_text = st.empty()

        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(extract_seller_info, url.strip(), proxy): url for url in urls if url.strip()}
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
                time.sleep(random.uniform(1, delay))  # Random delay

        # Save results to CSV
        filename = f"amazon_sellers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
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
    
