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
    return random.uniform(base_delay - 10, base_delay + 10)

def format_text(text):
    """Format text with proper capitalization"""
    return " ".join(word.capitalize() for word in text.split())

def fetch_url(url, proxies=None, retries=3, base_delay=100):
    """Fetch URL with retries, exponential backoff, and proxy rotation"""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    proxy = random.choice(proxies) if proxies else None
    proxies_dict = {"http": proxy, "https": proxy} if proxy else None
    proxy_msg = f"Using proxy: {proxy}" if proxy else "No proxy used"

    max_delay = 120  # Maximum delay cap of 120 seconds
    initial_delay = base_delay

    for attempt in range(retries):
        try:
            if attempt > 0:  # Only sleep if this is a retry
                # Use 1.5 as base for exponential backoff with a maximum cap
                wait_time = min(initial_delay * (1.5 ** attempt), max_delay)
                st.warning(
                    f"Retry attempt {attempt + 1}/{retries} for URL\n"
                    f"🔄 {url}\n"
                    f"🌐 {proxy_msg}\n"
                    f"⏳ Waiting {int(wait_time)} seconds before retry..."
                )
                time.sleep(wait_time)

            response = requests.get(url, headers=headers, proxies=proxies_dict, timeout=30)
            response.raise_for_status()
            return response

        except requests.exceptions.HTTPError as e:
            if hasattr(e.response, 'status_code'):
                if e.response.status_code == 503:
                    if attempt == retries - 1:  # Last attempt
                        st.error(
                            f"❌ 503 Service Unavailable Error: Amazon is rate limiting requests.\n"
                            f"URL: {url}\n"
                            f"{proxy_msg}\n"
                            "Try using different proxies or increasing the delay between requests."
                        )
                        raise
                    continue  # Try again with exponential backoff
                elif e.response.status_code == 404:
                    st.error(f"404 Not Found Error: The seller page does not exist.\nURL: {url}")
                    raise
                else:
                    st.error(f"HTTP Error {e.response.status_code}: {str(e)}\nURL: {url}")
                    raise
        except requests.exceptions.ConnectionError:
            st.error(f"Connection Error: Could not connect to {url}. Check your internet connection or proxy settings.")
            raise
        except requests.exceptions.Timeout:
            st.error(f"Timeout Error: The request to {url} timed out. Try increasing the timeout period.")
            raise
        except Exception as e:
            st.error(f"Unexpected error while fetching {url}: {str(e)}")
            raise

    raise requests.exceptions.RequestException(f"Failed to fetch {url} after {retries} retries.")

def fetch_with_selenium(url):
    """Fetch URL using Selenium with enhanced stealth measures"""
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument('--disable-browser-side-navigation')
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")

        # Additional stealth settings
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # Enhanced preferences
        chrome_options.add_experimental_option('prefs', {
            'profile.default_content_settings.images': 2,  # Disable images for faster loading
            'profile.managed_default_content_settings.javascript': 1,
            'profile.managed_default_content_settings.cookies': 1
        })

        driver = webdriver.Chrome(options=chrome_options)

        try:
            # Execute stealth scripts
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": random.choice(USER_AGENTS),
                "platform": "Win32",
                "acceptLanguage": "en-US,en;q=0.9"
            })

            # Mask webdriver presence
            driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            # Add plugins to look more like a real browser
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                "source": """
                    const newProto = navigator.__proto__;
                    delete newProto.webdriver;
                    navigator.__proto__ = newProto;
                """
            })

            # Initial longer delay before accessing URL
            time.sleep(random.uniform(8, 12))

            # Load the page
            driver.get(url)

            # Simulate human-like behavior
            # Random initial wait
            time.sleep(random.uniform(5, 8))

            # Scroll behavior with random pauses
            total_height = driver.execute_script("return document.body.scrollHeight")
            viewport_height = driver.execute_script("return window.innerHeight")
            scroll_points = range(0, total_height, viewport_height)

            for scroll in scroll_points:
                # Random scroll speed
                driver.execute_script(f"""
                    window.scrollTo({{
                        top: {scroll},
                        behavior: 'smooth'
                    }});
                """)
                time.sleep(random.uniform(1.5, 3))

                # Random mouse movements
                driver.execute_script("""
                    document.dispatchEvent(new MouseEvent('mousemove', {
                        'view': window,
                        'bubbles': true,
                        'cancelable': true,
                        'clientX': Math.floor(Math.random() * window.innerWidth),
                        'clientY': Math.floor(Math.random() * window.innerHeight)
                    }));
                """)

            # Final random wait
            time.sleep(random.uniform(4, 7))

            # Get page content
            content = driver.page_source

            # Check for anti-bot page
            if any(term in content.lower() for term in ["robot", "captcha", "automated access"]):
                raise Exception("Anti-bot detection triggered. Consider increasing delays and using proxies.")

            return content

        finally:
            driver.quit()

    except Exception as e:
        st.error(f"Selenium error: {str(e)}")
        raise

def extract_seller_info(url, proxies=None, base_delay=100, use_selenium=True):
    """Extract seller information from Amazon seller page"""
    try:
        # Always use Selenium now as it's more reliable
        content = fetch_with_selenium(url)
        soup = BeautifulSoup(content, "html.parser")

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

    except Exception as e:
        return {"URL": url, "Error": str(e)}

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
                45, 300, 100,
                help="Adding random delays helps avoid detection"
            )
        with col2:
            max_workers = st.slider(
                "Max concurrent requests",
                1, 5, 1,
                help="Higher values may increase speed but also increase detection risk"
            )

    if st.button("🚀 Start Scraping") and urls:
        with st.spinner("Testing sample URL..."):
            st.info(f"Checking 1 random URL first to verify functionality...")
            test_urls = random.sample(urls, min(1, len(urls)))
            for test_url in test_urls:
                try:
                    # Always use Selenium for better reliability
                    content = fetch_with_selenium(test_url)
                    soup = BeautifulSoup(content, "html.parser")
                    seller_info = soup.find("div", id="page-section-detail-seller-info") or soup.find("div", class_="a-box")

                    if not seller_info:
                        st.error(f"Could not find seller information on the page. URL might be invalid or blocked: {test_url}")
                        return
                except Exception as e:
                    st.error(f"Error detected for {test_url}: {str(e)}")
                    return

        st.success("Test URL passed. Proceeding with full scraping...")
        filename = f"amazon_sellers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        all_data, errors = [], []
        progress_bar = st.progress(0)
        status_text = st.empty()

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for url in urls:
                futures.append(executor.submit(extract_seller_info, url, proxies, delay, use_selenium=True))

            for index, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    result = future.result()
                    if "Error" in result:
                        errors.append(result)
                    else:
                        all_data.append(result)
                except Exception as e:
                    errors.append({"URL": urls[index], "Error": str(e)})

                progress = (index + 1) / len(urls)
                progress_bar.progress(progress)
                status_text.text(f"Processing URL {index + 1} of {len(urls)}")
                time.sleep(random_delay(delay))

        if all_data:
            save_to_csv(all_data, filename)
            st.success("✅ Scraping complete!")

            if errors:
                st.warning(f"⚠️ {len(errors)} URLs failed to scrape. Check the data for details.")

            st.download_button(
                "📥 Download CSV",
                open(filename, "rb"),
                file_name=filename,
                mime="text/csv",
                help="Click to download the scraped data"
            )

            # Display results preview
            st.subheader("📊 Results Preview")
            df = pd.DataFrame(all_data)
            st.dataframe(df.head())
        else:
            st.error("❌ No data was successfully scraped. Please try again with different settings or URLs.")

if __name__ == "__main__":
    main()
    
