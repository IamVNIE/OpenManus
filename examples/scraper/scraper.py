import requests
from bs4 import BeautifulSoup
import re
import os
import pandas as pd
import urllib.parse
import logging
import io
import pdfminer.high_level
import docx
import time
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_text_from_pdf(pdf_url):
    """Extracts text content from a PDF file using its URL."""
    try:
        response = requests.get(pdf_url)
        response.raise_for_status()
        pdf_file = io.BytesIO(response.content)
        text = pdfminer.high_level.extract_text(pdf_file)
        return text
    except Exception as e:
        logging.error(f"Error extracting text from PDF {pdf_url}: {e}")
        return None

def extract_text_from_docx(docx_url):
    """Extracts text content from a DOCX file using its URL."""
    try:
        response = requests.get(docx_url)
        response.raise_for_status()
        docx_file = io.BytesIO(response.content)
        document = docx.Document(docx_file)
        text = '\n'.join([paragraph.text for paragraph in document.paragraphs])
        return text
    except Exception as e:
        logging.error(f"Error extracting text from DOCX {docx_url}: {e}")
        return None

def extract_document_links(html_content, base_url):
    """Extracts document links and metadata from HTML content."""
    soup = BeautifulSoup(html_content, 'html.parser')
    document_links = []
    document_regex = re.compile(r'\.pdf$|\.doc[x]?$', re.IGNORECASE)

    for a_tag in soup.find_all('a', href=document_regex):
        href = a_tag.get('href')
        if href and not href.startswith('http'):
            href = urllib.parse.urljoin(base_url, href)

        document_name = a_tag.text.strip()

        parent = a_tag.find_parent()
        first, second, third = None, None, None

        if parent:
            try:
                siblings = list(parent.strings)
                siblings = [s.strip() for s in siblings if s.strip()]

                if len(siblings) >= 3:
                    first = siblings[0]
                    second = siblings[1]
                    third = siblings[2]
                elif len(siblings) >= 2:
                    first = siblings[0]
                    second = siblings[1]
                elif len(siblings) >= 1:
                    first = siblings[0]

            except Exception as e:
                logging.error(f"Error extracting sibling text: {e}")
                pass

        document_text = None
        if href.lower().endswith('.pdf'):
            document_text = extract_text_from_pdf(href)
        elif href.lower().endswith(('.doc', '.docx')):
            document_text = extract_text_from_docx(href)

        author = None
        if document_text:
            author_match = re.search(r"Author:\s*(.*)", document_text, re.IGNORECASE)
            if author_match:
                author = author_match.group(1).strip()

        document_links.append({
            'url': href,
            'document_name': document_name,
            'first': first,
            'second': second,
            'third': third,
            'author': author,
            'document_text': document_text
        })

    return document_links

def download_document(document_url, output_dir="downloads", rate_limit_delay=(1, 2)):
    """Downloads a document from a URL to the local filesystem."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    try:
        filename = _get_filename_from_url(document_url)
        if not filename:
            logging.error(f"Could not determine filename for {document_url}")
            return None
        filepath = os.path.join(output_dir, filename)

        if os.path.exists(filepath):
            logging.warning(f"File already exists: {filepath}. Skipping download.")
            return filepath

        time.sleep(time.uniform(rate_limit_delay[0], rate_limit_delay[1]))

        response = requests.get(document_url, stream=True)
        response.raise_for_status()

        with open(filepath, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        logging.info(f"Successfully downloaded {document_url} to {filepath}")
        return filepath

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download {document_url}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while downloading {document_url}: {e}")
        return None

def _get_filename_from_url(url):
    """Helper function to extract the filename from a URL."""
    try:
        parsed_url = urllib.parse.urlparse(url)
        path = parsed_url.path
        return os.path.basename(path)
    except Exception as e:
        logging.error(f"Error parsing URL {url}: {e}")
        return None

def batch_download_documents(document_links, output_dir="downloads", rate_limit_delay=(1, 2)):
    """Downloads a batch of documents, tracks progress, and handles errors."""
    total_documents = len(document_links)
    success_count = 0
    failure_count = 0
    success_log = []
    failure_log = []

    logging.info(f"Starting batch download of {total_documents} documents.")

    for i, doc_url in enumerate(document_links, 1):
        logging.info(f"Downloading document {i}/{total_documents}: {doc_url}")
        filepath = download_document(doc_url, output_dir, rate_limit_delay)

        if filepath:
            success_count += 1
            success_log.append({"url": doc_url, "filepath": filepath})
            logging.info(f"Successfully downloaded document {i}/{total_documents}: {doc_url} to {filepath}")
        else:
            failure_count += 1
            failure_log.append({"url": doc_url, "error": "Download failed"})
            logging.error(f"Failed to download document {i}/{total_documents}: {doc_url}")

    logging.info(f"Batch download complete.")
    logging.info(f"Successfully downloaded: {success_count} documents.")
    logging.info(f"Failed to download: {failure_count} documents.")

    success_log_path = os.path.join(output_dir, "success_log.csv")
    failure_log_path = os.path.join(output_dir, "failure_log.csv")

    pd.DataFrame(success_log).to_csv(success_log_path, index=False)
    pd.DataFrame(failure_log).to_csv(failure_log_path, index=False)

    logging.info(f"Success log saved to: {success_log_path}")
    logging.info(f"Failure log saved to: {failure_log_path}")

def format_document_data(document_data):
    """Formats the extracted document data into a list of lists."""
    formatted_data = []

    for doc in document_data:
        name = doc.get('document_name', '')
        first = doc.get('first', '')
        second = doc.get('second', '')
        third = doc.get('third', '')

        name = str(name)
        first = str(first)
        second = str(second)
        third = str(third)

        formatted_data.append([name, first, second, third])

    return formatted_data

def get_all_document_links(base_url, start_page=1, max_pages=None, limit=None):
    """
    Extracts document links from all pages of a website, handling pagination.

    Args:
        base_url (str): The base URL of the website.
        start_page (int): The page number to start scraping from.
        max_pages (int, optional): The maximum number of pages to scrape. If None, scrape all pages.
        limit (int, optional):  A limit to the number of documents to extract, for testing.
    Returns:
        list: A list of dictionaries, where each dictionary represents a document link
              and its associated metadata.
    """
    all_document_data = []
    page_num = start_page
    document_count = 0

    while True:
        # Construct the URL for the current page
        url = f"{base_url}page/{page_num}/" if page_num > 1 else base_url  # Adjust URL structure as needed
        logging.info(f"Fetching page: {url}")

        try:
            response = requests.get(url)
            response.raise_for_status()
            html_content = response.text
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching page {url}: {e}")
            break  # Stop if a page cannot be fetched

        # Extract document links from the current page
        document_data = extract_document_links(html_content, url)  # Use the current page's URL as base URL
        all_document_data.extend(document_data)
        
        document_count += len(document_data)

        # Check if the limit has been reached
        if limit and document_count >= limit:
            logging.info(f"Reached document limit ({limit}). Stopping.")
            all_document_data = all_document_data[:limit]  # Truncate the list to the limit
            break
            
        # Check for pagination and stopping conditions
        soup = BeautifulSoup(html_content, 'html.parser')
        next_page_link = soup.find("a", class_="nextpostslink")  # Adapt the selector to your website

        if not next_page_link:
            logging.info("No more 'next' page link found. Stopping.")
            break  # No next page link found; stop scraping

        if max_pages and page_num >= max_pages:
            logging.info(f"Reached max_pages limit ({max_pages}). Stopping.")
            break  # Reached the maximum number of pages to scrape

        page_num += 1
        time.sleep(1)  # Respect rate limits

    return all_document_data

def main_controller(base_url, output_dir="CBS_Case_Competition_Scraper", download_dir="downloaded_documents", start_page=1, max_pages=None, limit=None):
    """
    Orchestrates the entire scraping workflow.

    Args:
        base_url (str): The base URL of the website to scrape.
        output_dir (str): The main output directory.
        download_dir (str): The subdirectory for downloaded documents.
        start_page (int): The page number to start scraping from.
        max_pages (int, optional): The maximum number of pages to scrape. If None, scrape all pages.
        limit (int, optional): A limit to the number of documents to extract, for testing.
    """

    # 1. Create necessary directories
    output_path = os.path.join(output_dir, "output")
    download_path = os.path.join(output_dir, download_dir)
    Path(output_path).mkdir(parents=True, exist_ok=True)
    Path(download_path).mkdir(parents=True, exist_ok=True)

    # 2. Fetch all document links, handling pagination
    all_document_data = get_all_document_links(base_url, start_page, max_pages, limit)

    # 3. Extract URLs for downloading
    document_urls = [doc['url'] for doc in all_document_data if doc['url']]

    # 4. Format the document data
    formatted_data = format_document_data(all_document_data)

    # 5. Save formatted data to CSV
    csv_file = os.path.join(output_path, "formatted_document_data.csv")
    df = pd.DataFrame(formatted_data, columns=['Name', 'first', 'second', 'third'])
    df.to_csv(csv_file, index=False)
    logging.info(f"Formatted document data saved to {csv_file}")

    # 6. Download documents
    batch_download_documents(document_urls, download_path)

if __name__ == '__main__':
    CBS_LIBRARY_URL = "https://www.casecompetition.com/library/"
    
    # Example usage of the main controller function with a limit of 5 documents
    main_controller(CBS_LIBRARY_URL, limit=5)
