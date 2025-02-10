import re
import duckdb
import pandas as pd
from collections import Counter
from typing import List, Dict, Optional

# Exclusion List & Tokenizer
exclusion_list = [
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "b",
    "can", "can't", "cannot", "could", "couldn't", "cfr", "c",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "d",
    "each", "e",
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
    Tokenizes text into words (lowercased) using a regex that excludes words 
    in the exclusion list and pure numbers.
    """
    blocked_terms = '|'.join(re.escape(word) for word in exclusion_list)
    pattern = r'\b(?!' + blocked_terms + r')(?!\d+)\w+\b'
    return re.findall(pattern, text.lower())

# Consolidated Agency Data Function
def get_keyword_stats_by_agency(
    df
) -> Dict[str, Dict]:
    agency_stats = {}
    for _, row in df.iterrows():
        agency = row["name"]
        sec_title = row["section_title"]
        text = row["section_text"]
        doc_count = row["doc_count"]
        total_word_count = len(text.split(' '))
        words = tokenize(text)
        if agency not in agency_stats:
            agency_stats[agency] = {
                "section_title": sec_title,
                "total_word_count": total_word_count,
                "counter": Counter(words),
                "doc_count": doc_count
            }
        else:
            agency_stats[agency]["total_word_count"] += len(words)
            agency_stats[agency]["counter"].update(words)
    return agency_stats
