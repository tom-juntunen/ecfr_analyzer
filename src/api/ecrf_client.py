#!/usr/bin/env python
"""
ecfr_client.py

A client to download eCFR API data asynchronously. For each endpoint, it:
  - Generates a list of URLs (using date range and title parameters as needed)
  - Fetches the data using httpx with concurrency limited by an asyncio semaphore
  - For JSON endpoints, flattens the response (or the nested structure via a custom function)
  - For XML endpoints, downloads the XML (using proper stream handling) and converts it to JSON
  - For the "section" processing, extracts every section (i.e. nodes with @TYPE "SECTION")
    along with their full ancestry (title, chapter, subchap, part, subpart, appendix)
  - Splits the "P" field so that each P element becomes its own record with additional columns.
  - Writes the results as line-delimited JSON (JSONL) files in the "data" folder.
  
Usage:
    python ecfr_client.py
"""

import asyncio
import httpx
import json
import os
import calendar
from datetime import datetime, timedelta

import xmltodict  # pip install xmltodict

CONCURRENCY_LIMIT = 6  # maximum concurrent requests

# Date Range Helper
def generate_date_range(start_date, end_date):
    """
    Generates a list of date strings for every day between the start_date and end_date (inclusive).
    
    Args:
        start_date (str): Start date in "YYYY-MM-DD" format.
        end_date (str): End date in "YYYY-MM-DD" format.
        
    Returns:
        List[str]: A list of date strings for each day in the range.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    delta_days = (end - start).days + 1
    
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta_days)]


# HTTP and XML Helpers
async def fetch_url(url, params, semaphore, client, max_retries=3):
    """
    Fetches a URL using httpx with the given query parameters (for JSON endpoints),
    using the semaphore to limit concurrency. Implements retry logic for server errors.
    
    Args:
        url (str): URL to fetch.
        params (dict): Query parameters for the GET request.
        semaphore (asyncio.Semaphore): Semaphore to limit concurrency.
        client (httpx.AsyncClient): The shared HTTP client.
        max_retries (int): Maximum number of retries for server errors (500+).
        
    Returns:
        Parsed JSON data if the response is successful, otherwise None.
    """
    retries = 0
    while retries < max_retries:
        async with semaphore:
            try:
                response = await client.get(url, params=params, timeout=30.0)
            except Exception as e:
                print(f"Request exception for {url}: {e}")
                return None

        if response.status_code == 200:
            try:
                return response.json()
            except Exception as e:
                print(f"JSON parse error for {url}: {e}")
                return None
        elif response.status_code >= 500:
            retries += 1
            print(f"Retry {retries} for {url} due to server error {response.status_code}")
            await asyncio.sleep(2 ** retries)  # Exponential backoff
        else:
            print(f"Error {response.status_code} fetching {url}")
            return None
    
    print(f"Max retries ({max_retries}) reached for {url}")
    return None

async def process_request(url, params, data_key, semaphore, client):
    """
    Uses fetch_url() to retrieve JSON data from the URL and extracts records.
    If data_key is provided, returns data[data_key] (or [] if not found);
    otherwise, returns the JSON data (wrapped as a list if needed).
    
    Args:
        url (str): URL to fetch.
        params (dict): Query parameters.
        data_key (str or None): Key to extract from the JSON data.
        semaphore (asyncio.Semaphore): Semaphore for limiting concurrency.
        client (httpx.AsyncClient): Shared HTTP client.
    
    Returns:
        A list of records.
    """
    data = await fetch_url(url, params, semaphore, client)
    if data is None:
        return []
    if data_key:
        records = data.get(data_key, [])
    else:
        records = data
    if not isinstance(records, list):
        records = [records]
    return records

def join_p_records(record: dict):
    """
    Joins the "P" field of a section record into a single concatenated text.
    
    For a given section record dictionary:
      - If the "P" field is a list, concatenates all elements into a single string with a space as a separator.
      - If the "P" field is a single string, it is left unchanged.
      - Returns a new dictionary that is identical to the input record except that the "P" field
        contains the concatenated text.
    
    Args:
        record (dict): A dictionary representing a section record.
        
    Returns:
        dict: The section record with a single concatenated "P" field.
    """
    new_record = record.copy()
    if "P" in new_record:
        p_field = new_record["P"]
        if isinstance(p_field, list):
            new_record["P"] = " ".join(str(p) for p in p_field)
        else:
            new_record["P"] = str(p_field)
    return new_record

def process_item(item):
    """
    Recursively processes an item:
      - If it's a string, encode and decode it.
      - If it's a dictionary, process each value.
      - If it's a list, process each element.
      - Otherwise, return the item unchanged.
    
    Args:
        item: The item to process.
    
    Returns:
        The processed item.
    """
    if isinstance(item, str):
        return item.encode().decode('utf-8')
    elif isinstance(item, dict):
        return {key: process_item(value) for key, value in item.items()}
    elif isinstance(item, list):
        return [process_item(element) for element in item]
    else:
        return item


# Section Record Extraction

def extract_section_records(data, ancestry=None):
    """
    Recursively traverses the JSON (converted from the full XML) and extracts every node
    where "@TYPE" equals "SECTION". It also collects ancestry from nodes with types:
    TITLE, SUBTITLE, CHAPTER, SUBCHAP, PART, SUBPART, APPENDIX.
    
    Args:
        data: The JSON (dict or list) from the parsed XML.
        ancestry: A dictionary holding the current ancestry context.
        
    Returns:
        A list of dictionaries, each representing a section record with its own data
        plus the ancestry data.
    """
    if ancestry is None:
        ancestry = {}
    records = []
    if isinstance(data, dict):
        new_ancestry = ancestry.copy()
        type_val = data.get("@TYPE")
        if type_val in ["TITLE", "SUBTITLE", "CHAPTER", "SUBCHAP", "PART", "SUBPART", "APPENDIX"]:
            new_ancestry[type_val.lower()] = data.get("HEAD", data.get("@N", ""))
        if type_val == "SECTION":
            record = {}
            record.update(new_ancestry)
            for key, value in data.items():
                if key not in ["children"] and 'DIV' not in key:
                    record[key] = process_item(value)
            records.append(record)
        for value in data.values():
            if isinstance(value, (dict, list)):
                records.extend(extract_section_records(value, new_ancestry))
    elif isinstance(data, list):
        for item in data:
            records.extend(extract_section_records(item, ancestry))
    return records

async def process_xml_request(url, params, semaphore, client, max_retries=3, delay=1.0):
    """
    Special processing for XML endpoints (the full_xml endpoint).
    Sets the Accept header to application/xml, downloads the XML as a stream,
    decodes it as UTF-8, and parses it using xmltodict.
    Returns the JSON (a single dictionary) representation of the XML.
    
    Args:
        url (str): URL to fetch.
        params (dict): Query parameters.
        semaphore (asyncio.Semaphore): Semaphore for concurrency limiting.
        client (httpx.AsyncClient): Shared HTTP client.
        max_retries (int): Maximum number of retry attempts.
        delay (float): Base delay between retries in seconds.
        
    Returns:
        A dictionary representing the parsed XML converted to JSON, or None if error.
    """
    headers = {"Accept": "application/xml"}
    for attempt in range(1, max_retries + 1):
        async with semaphore:
            try:
                response = await client.get(url, params=params, headers=headers, timeout=60.0)
                if response.status_code == 200:
                    try:
                        xml_content = response.content.decode("utf-8")
                        data = xmltodict.parse(xml_content)
                        return json.loads(json.dumps(data))
                    except Exception as e:
                        print(f"Error parsing XML from {url} on attempt {attempt}: {e}")
                else:
                    print(f"Error {response.status_code} fetching XML from {url} on attempt {attempt}")
            except Exception as e:
                print(f"Request exception for XML {url} on attempt {attempt}: {e}")
        if attempt < max_retries:
            await asyncio.sleep(delay * (2 ** (attempt - 1)))
    return None

async def process_section_request(url, params, semaphore, client):
    """
    Downloads the full XML document from the API, converts it to JSON,
    extracts section records using extract_section_records(), and then
    splits any "P" field into individual records.
    
    Args:
        url (str): URL to fetch.
        params (dict): Query parameters.
        semaphore (asyncio.Semaphore): Semaphore for concurrency limiting.
        client (httpx.AsyncClient): Shared HTTP client.
    
    Returns:
        A list of dictionaries suitable for inserting into a "section" table.
    """
    print(f"Making request for to {url}")
    json_data = await process_xml_request(url, params, semaphore, client)
    if json_data is None:
        return []
    section_records = extract_section_records(json_data)
    final_records = []
    for rec in section_records:
        if "P" in rec:
            joined_records = join_p_records(rec)
            final_records.append(joined_records)
        else:
            final_records.append(rec)
    return final_records

# Endpoint Processing and Main
async def process_endpoint(endpoint, start_date=None, end_date=None, semaphore=None, client=None):
    """
    Processes one endpoint as defined by the endpoint dictionary.
    Iterates over date and/or title values, calls the proper processing function
    (JSON, XML, or section) for each request, and writes results to a JSONL file
    in the "data" folder.
    
    To reduce burst requests and potential 429 rate-limit errors, this function
    groups requests by title and awaits a delay between processing each title batch.
    
    Args:
        endpoint (dict): Endpoint definition.
        start_date (str): Start date in "YYYY-MM-DD" format (if applicable).
        end_date (str): End date in "YYYY-MM-DD" format (if applicable).
        semaphore (asyncio.Semaphore): Semaphore for concurrency limiting.
        client (httpx.AsyncClient): Shared HTTP client.
    """
    output_data = {}  # keys: output filename; values: list of records
    data_dir = os.path.abspath("data")
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    dates = generate_date_range(start_date, end_date) if endpoint.get("use_date") else [None]
    titles = endpoint.get("titles", []) if endpoint.get("use_title") else [None]

    # Process requests in batches grouped by title.
    for title in titles:
        tasks = []
        output_file_list = []  # maintain mapping for each task to its output file name
        for date in dates:
            # Build URL.
            if "url_template" in endpoint:
                url = endpoint["url_template"].format(date=date if date else "", title=title if title else "")
            else:
                url = endpoint["url"]
            params = endpoint.get("params", {}).copy()
            if date:
                params["date"] = date
            if title:
                params["title"] = title
            # Determine output filename for this combination.
            if endpoint.get("output"):
                output_filename = endpoint["output"].format(date=date if date else "", title=title if title else "")
                output_filename = os.path.join(data_dir, output_filename)
            else:
                output_filename = os.path.join(data_dir, f"{endpoint['name']}.jsonl")
            output_file_list.append(output_filename)
            
            # Create the appropriate task.
            if endpoint["name"] == "section":
                tasks.append(asyncio.create_task(process_section_request(url, params, semaphore, client)))
            else:
                tasks.append(asyncio.create_task(process_request(url, params, endpoint.get("data_key"), semaphore, client)))

            break  # use only the first date for now; TODO: setup historical pipeline for change history and use this there
        
        # Await the batch of tasks for the current title.
        responses = await asyncio.gather(*tasks)
        for i, records in enumerate(responses):
            if records:
                output_data.setdefault(output_file_list[i], []).extend(records)
        
        # Sleep between title batches to reduce burst request rates.
        await asyncio.sleep(1)
    
    # Write the gathered data to files.
    for filename, records in output_data.items():
        try:
            with open(filename, "w", encoding="utf-8") as f:
                for record in records:
                    f.write(json.dumps(record) + "\n")
            print(f"Endpoint '{endpoint['name']}' -> Written {len(records)} records to {filename}")
        except Exception as e:
            print(f"Error writing to file {filename}: {e}")


async def main():
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    API_BASE = "https://www.ecfr.gov/api"

    endpoints = [
        {
            "name": "agency",
            "url": f"{API_BASE}/admin/v1/agencies.json",
            "use_date": False,
            "use_title": False,
            "data_key": "agencies",
            "output": "agency.jsonl"
        },
        {
            "name": "title",
            "url": f"{API_BASE}/versioner/v1/titles.json",
            "use_date": False,
            "use_title": False,
            "data_key": "titles",
            "output": "title.jsonl"
        },
        {
            "name": "section",
            "url_template": f"{API_BASE}/versioner/v1/full/{{date}}/title-{{title}}.xml",
            "use_date": True,
            "use_title": True,
            # "titles": ["7"],  # this one seems to error at times, and it determines our timeout
            "titles": [str(i+1) for i in range(50) if (i+1) not in [7,35]],  # includes 48 of 50 titles
            "data_key": None,
            "output": "section_{date}_title-{title}.jsonl"
        },
    ]

    # Set desired date range.
    start_date = "2025-01-06"
    end_date = "2025-01-13"

    async with httpx.AsyncClient() as client:
        for endpoint in endpoints:
            print(f"Processing endpoint: {endpoint['name']}")
            await process_endpoint(endpoint, start_date, end_date, semaphore, client)
            print(f"Finished endpoint: {endpoint['name']}\n")

if __name__ == "__main__":
    asyncio.run(main())
