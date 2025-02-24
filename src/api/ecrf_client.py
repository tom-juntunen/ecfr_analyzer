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
import re
import calendar
from hashlib import md5
from datetime import datetime, date
from db_loader import merge_jsonl_file

import xmltodict

CONCURRENCY_LIMIT = 6  # maximum concurrent requests

def generate_date_range(start_date, end_date, month_increment=1):
    """
    Generates a list of date strings between start_date and end_date (inclusive)
    corresponding to the last day of each month, with customizable month increments.
    
    Args:
        start_date (str): Start date in "YYYY-MM-DD" format
        end_date (str): End date in "YYYY-MM-DD" format
        month_increment (int): Number of months to increment between dates (default: 1)
        
    Returns:
        List[str]: A list of date strings representing the last day of each month
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    dates = []
    
    # Initialize with the start month and year
    year = start.year
    month = start.month
    
    while True:
        # Determine the last day of the current month
        last_day = calendar.monthrange(year, month)[1]
        last_date = date(year, month, last_day)
        
        # Only include the last_date if it falls within the range
        if last_date >= start and last_date <= end:
            dates.append(last_date.strftime("%Y-%m-%d"))
        
        # Move to the next period
        months_total = year * 12 + month - 1 + month_increment
        year = months_total // 12
        month = (months_total % 12) + 1
        
        # Break if the first day of the next month is after the end date
        if date(year, month, 1) > end:
            break

    return dates

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

async def process_xml_request(url, params, semaphore, client, max_retries=3, delay=0.25):
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
    surrogate_id_fields = ["title", "chapter", "part", "subpart"]
    for rec in section_records:
        id_parts = []
        subpart_parts = []

        # Regex for matching identifier values for creating primary key
        pattern = re.compile(
            r"(?<=TITLE\s)(\S+?(?=(\\u| |-|\u2014)))|(?<=CHAPTER\s)(\S+?(?=(\\u| |-|\u2014)))|(?<=PART\s)(\S+?(?=(\\u| |-|\u2014)))|(?<=SUBPART\s)(\S+?(?=(\\u| |-|\u2014)))",
            re.IGNORECASE
        )

        # Build parts from fields
        for sid in surrogate_id_fields:
            if sid in rec:
                id_part = pattern.search(str(rec[sid]).upper())
                id_part = id_part.group().strip() if id_part else ""
                id_parts.append(id_part)
                subpart_parts.append(id_part)  # Same for subpart_id
        
        # Join parts with a consistent delimiter
        id_str = "_".join(id_parts) + f"_{rec.get('@N', '')}"  # Include @N for id
        subpart_str = "_".join(subpart_parts)  # No @N for subpart_id
        
            # Hash the strings
        rec["id"] = md5(id_str.encode('utf-8')).hexdigest()
        rec["subpart_id"] = md5(subpart_str.encode('utf-8')).hexdigest()

        if "P" in rec:
            joined_records = join_p_records(rec)
            final_records.append(joined_records)
        else:
            final_records.append(rec)
    return final_records

async def process_endpoint(endpoint, start_date=None, end_date=None, semaphore=None, client=None, delay=1):
    """
    Processes one endpoint as defined by the endpoint dictionary.
    Iterates over date and/or title values, calls the proper processing function
    (JSON, XML, or section) for each request, and writes results immediately
    to a JSONL file in the "data" folder for each date (and title) combination.
    
    This version writes output immediately after each request to prevent
    accumulating large amounts of data in memory.
    
    Args:
        endpoint (dict): Endpoint definition.
        start_date (str): Start date in "YYYY-MM-DD" format (if applicable).
        end_date (str): End date in "YYYY-MM-DD" format (if applicable).
        semaphore (asyncio.Semaphore): Semaphore for concurrency limiting.
        client (httpx.AsyncClient): Shared HTTP client.
        delay (int or float): Delay in seconds between each request.
    """
    data_dir = os.path.abspath("data")
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    dates = generate_date_range(start_date, end_date) if endpoint.get("use_date") else [None]
    titles = endpoint.get("titles", []) if endpoint.get("use_title") else [None]

    # Process requests sequentially.
    for title in titles:
        for date in dates:
            # Build URL.
            if "url_template" in endpoint:
                url = endpoint["url_template"].format(
                    date=date if date else "",
                    title=title if title else ""
                )
            else:
                url = endpoint["url"]

            params = endpoint.get("params", {}).copy()
            if date:
                params["date"] = date
            if title:
                params["title"] = title

            # Determine output filename for this combination.
            if endpoint.get("output"):
                output_filename = endpoint["output"].format(
                    date=date if date else "",
                    title=title if title else ""
                )
                output_filename = os.path.join(data_dir, output_filename)
            else:
                # Default filename using endpoint name and date (or "default" if date is None).
                date_part = date if date else "default"
                title_part = title if title else "default"
                output_filename = os.path.join(data_dir, f"{endpoint['name']}_{title_part}_{date_part}.jsonl")
            
            # Process the appropriate request function and await its result.
            if endpoint["name"] == "section":
                records = await process_section_request(url, params, semaphore, client)
            else:
                records = await process_request(url, params, endpoint.get("data_key"), semaphore, client)
                if endpoint["name"] == "agency":
                    children = [child for rec in records for child in rec.get("children", [])]
                    records.extend(children)
            
            # Write to file immediately if records exist.
            if records:
                try:
                    with open(output_filename, "w", encoding="utf-8") as f:
                        for record in records:
                            f.write(json.dumps(record) + "\n")
                    print(f"Endpoint '{endpoint['name']}' -> Written {len(records)} records to {output_filename}")

                # Merge the data from the file into the database.
                    merge_jsonl_file(output_filename, date)
                    print(f"Successfully merged {output_filename} into the database.")
                except Exception as e:
                    print(f"Error writing to file {output_filename}: {e}")
                finally:
                    # Remove the file after loading
                    os.remove(output_filename)
                    # pass

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
            # "titles": [str(i+1) for i in range(50) if (i+1) in [40]], 
            "titles": [str(i+1) for i in range(50) if (i+1) not in [7, 35]],  # includes 48 of 50 titles
            "data_key": None,
            "output": "section_{date}_title-{title}.jsonl"
        },
        # {
        #     "name": "correction",
        #     "url_template": f"{API_BASE}/admin/v1/corrections/title/{{title}}.json",
        #     "use_date": False,
        #     "use_title": True,
        #     "titles": [str(i+1) for i in range(50) if (i+1) not in [35]],
        #     "data_key": "ecfr_corrections",
        #     "output": "correction_title-{title}.jsonl"
        # },
    ]

    # Set desired date range.
    start_date = "2024-01-01"
    # start_date = "2024-01-31"
    end_date = "2025-02-19"

    async with httpx.AsyncClient() as client:
        for endpoint in endpoints:
            print(f"Processing endpoint: {endpoint['name']}")
            await process_endpoint(endpoint, start_date, end_date, semaphore, client)
            print(f"Finished endpoint: {endpoint['name']}\n")

if __name__ == "__main__":
    asyncio.run(main())
