# word_frequency_head.py

import re
import json
import duckdb
import pandas as pd
from collections import Counter
from typing import List, Tuple, Union

exclusion_list = [
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "b",
    "can", "can't", "cannot", "could", "couldn't", "cfr", "c",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
    "each",
    "few", "for", "from", "further",
    "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's",
    "hers", "herself", "him", "himself", "his", "how", "how's",
    "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself",
    "let's",
    "me", "may", "more", "most", "must", "mustn't",
    "my", "myself",
    "no", "nor", "not",
    "of", "off", "on", "once", "only", "or", "other", "ought",
    "our", "ours", "ourselves",
    "out", "over", "own",
    "part",
    "reserved",
    "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "s",
    "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's",
    "these", "they", "they'd", "they'll", "they're", "they've",
    "this", "those", "through", "to", "too",
    "under", "until", "up", "u",
    "very",
    "was", "wasn't", "we", "we'd", "we'll", "we're", "we've",
    "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while",
    "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't",
    "you", "you'd", "you'll", "you're", "you've",
    "your", "yours", "yourself", "yourselves"
]

def tokenize(text: str) -> List[str]:
    """
    Tokenizes the text into words by lowercasing it and extracting sequences
    of alphanumeric characters, excluding words specified in the exclusion list.
    
    Args:
        text: The input text string.
    
    Returns:
        A list of words not in the exclusion list.
    """
    # Convert exclusion list to a regex pattern for negative lookahead assertion
    blocked_terms = '|'.join(re.escape(word) for word in exclusion_list)
    # The regex \b(?!blocked_terms)\w+\b matches word characters,
    # but excludes the words in the exclusion list (and pure digits).
    return re.findall(r'\b(?!' + blocked_terms + r')(?!\d+)\w+\b', text.lower())

def top_words_from_agency_section_ref(
    con: duckdb.DuckDBPyConnection, num: int = 5):
    """
    Queries the DuckDB database to join the agency, agency_section_ref, and section tables,
    concatenates the HEAD and P columns from the section table, tokenizes the resulting text,
    counts word frequencies per agency, and returns the total word count and top N words for each agency.

    Args:
        con: A DuckDB connection.
        num: The number of top words to return per agency (default is 5).

    Returns:
        A dictionary where each key is the agency name and the value is a tuple:
            (total_word_count, [(word1, count1), (word2, count2), ...]).
    """
    query = """
    SELECT 
        a.name,
        s.cfr_ref_title AS section_title,
        COALESCE(s.HEAD || ' ' || s.p, '') AS section_head_and_p_text
    FROM agency a
    JOIN agency_section_ref r ON a.slug = r.slug 
    JOIN section s 
        ON CAST(s.cfr_ref_title AS INTEGER) = r.cfr_ref_title
        AND s.cfr_ref_chapter IS NOT DISTINCT FROM r.cfr_ref_chapter
    WHERE a.name like '%Defense'
    """
    # Retrieve the data into a DataFrame.
    df = con.execute(query).fetchdf()
    
    # Prepare a dictionary to hold our analysis per agency.
    agency_word_analysis = {}
    
    # Group the rows by agency name.
    for (agency, section_title), group in df.groupby(["name", "section_title"]):
        agency_df = df[df['name'] == agency]
        # Concatenate all the section texts for this agency.
        combined_text = ' '.join(group['section_head_and_p_text'].tolist())
        # Tokenize the text.
        words = tokenize(combined_text)
        # Count the words.
        counter = Counter(words)
        total_word_count = sum(counter.values())
        # Get the doc count
        total_doc_count = int(agency_df['section_title'].nunique())
        # Store a tuple of total word count and top N words.
        agency_word_analysis[agency] = (section_title, total_word_count, counter.most_common(num), total_doc_count)
    
    return agency_word_analysis


if __name__ == "__main__":
    # Connect to the DuckDB database.
    con = duckdb.connect(database='ecfr_analyzer_local.db')
    # Analyze the top 10 words per agency.
    result = top_words_from_agency_section_ref(con, num=10)
    
    # Sort the result by total word count in descending order.
    sorted_result = sorted(result.items(), key=lambda x: x[1][1], reverse=True)
    
    # Print the results with 2-space indentation for sub-values.
    for agency, (section_title, total_count, top_words, doc_count) in sorted_result:
        print(f"Agency: {agency}")
        print(f"  Section Title: {section_title}")
        print(f"  Total word count: {total_count}")
        print(f"  Top words: {top_words}\n")
        print(f"  Doc count: {doc_count}\n")