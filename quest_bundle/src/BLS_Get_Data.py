# Databricks notebook source
import requests
from bs4 import BeautifulSoup
from pathlib import Path

base_url = "https://download.bls.gov/pub/time.series/pr/"

headers = {
    "User-Agent": "MyBLSDownloader/1.0 (your.email@example.com)"
}

# Directory to save the files
output_dir = Path("/Volumes/datasets/default/bls/pr/")
output_dir.mkdir(exist_ok=True)

# Create a API request session
session = requests.Session()
session.headers.update(headers)

# Get the directory listing
response = session.get(base_url, timeout=30)
response.raise_for_status()

# Parse the HTML
soup = BeautifulSoup(response.text, "html.parser")

# Download each file
for link in soup.find_all("a"):
    href = link.get("href")
    filename = Path(href).name

    # Skip parent directory and subdirectories
    if not href or href == "../" or href.endswith("/"):
        continue

    url = base_url + filename
    destination = output_dir / filename

    print(f"Downloading {href}")

    r = session.get(url, timeout=60)
    r.raise_for_status()

    with open(destination, "wb") as f:
        f.write(r.content)

print("Done!")
