# hugging face utils
from huggingface_hub import (
    hf_hub_download,
    HfApi,
)
import os
import re

# Define required file patterns.
# Adjust these regexes based on the minimum files you need for a model to load.
REQUIRED_PATTERNS = {
    # Essential for all models
    "config": r"^config\.json$",

    # For generation models (e.g. LLaMA, GPT-2, Llama-3.1-8B-Instruct)
    "generation_config": r"^generation_config\.json$",
    # (For BERT you might not need generation_config, so you could comment it out if desired)

    # Model weights: supports both sharded weights (e.g. pytorch_model-00001-of-00033.bin,
    # model-00001-of-00004.safetensors) and single-file weights (e.g. model.safetensors)
    "model_weights": r"^(pytorch_model(-\d+-of-\d+)?\.bin|model(-\d+-of-\d+)?\.safetensors)$",

    # Optional: Model index file for sharded safetensors (if needed)
    "model_index": r"^model\.safetensors\.index\.json$",
    # You can comment this out if you don't require the index file.

    # Tokenizer files:
    # - LLaMA may use tokenizer.model
    # - GPT-2 may use merges.txt and vocab.json
    # - BERT may use vocab.txt or tokenizer.json
    "tokenizer": r"^(tokenizer(\.model)?|merges\.txt|vocab(\.txt|\.json)|tokenizer\.json)$",

    # Tokenizer configuration (common for all models)
    "tokenizer_config": r"^tokenizer_config\.json$",

    # Special tokens mapping – used for models like LLaMA and sometimes others.
    "special_tokens": r"^special_tokens_map\.json$",

    # Vocab file:
    "vocab": r"^vocab\.json$",
}

def download_model_files(repo_id: str, revision: str = None):
    """
    Downloads the minimum required files for a model repository.
    It lists the repo files via HfApi, checks for the minimum required files based on REQUIRED_PATTERNS,
    and downloads the matching files via hf_hub_download.
    
    Returns:
        local_dir (str): the directory where files are saved.
        downloaded_files (list): list of downloaded file paths.
    """
    api = HfApi()
    repo_files = api.list_repo_files(repo_id, revision=revision)
    
    # Create a local directory for downloads (e.g. "models/decapoda-research_llama-7b-hf")
    local_dir = os.path.join("models", repo_id.replace("/", "_"))
    os.makedirs(local_dir, exist_ok=True)
    
    # Find matches for each required pattern.
    matched_files = {key: [] for key in REQUIRED_PATTERNS.keys()}
    for file in repo_files:
        for key, pattern in REQUIRED_PATTERNS.items():
            if re.match(pattern, file):
                matched_files[key].append(file)
    
    # Validate that for each required key (except those you consider optional) there is at least one file.
    missing_keys = []
    for key, files in matched_files.items():
        # For example, "generation_config" might be optional for some models (like BERT).
        if key == "generation_config" and repo_id.startswith("bert-"):
            continue
        if key == "special_tokens" and not files:
            continue  # If a model doesn’t provide special tokens, you may skip it.
        if key == "vocab" and not files:
            continue  # If a model doesn’t provide vocab.json, you may skip it.
        if key == "model_index" and not files:
            continue  # If a model doesn’t provide model_index, you may skip it.
        if not files:
            missing_keys.append(key)
    
    if missing_keys:
        raise Exception(
            f"Missing required file types for keys: {', '.join(missing_keys)}. "
            f"Repo files: {repo_files}"
        )
    
    # Decide which files to download:
    # - For "model_weights", download all matches (to cover sharded weights).
    # - For all other keys, download only the first matching file.
    files_to_download = []
    for key, files in matched_files.items():
        if key == "model_weights":
            files_to_download.extend(files)
        elif files:
            files_to_download.append(files[0])
    
    downloaded_files = []
    for file in files_to_download:
        try:
            file_path = hf_hub_download(
                repo_id=repo_id,
                filename=file,
                revision=revision,
                local_dir=local_dir,
            )
            downloaded_files.append(file_path)
        except Exception as e:
            raise Exception(f"Error downloading file '{file}': {e}")
    
    return local_dir, downloaded_files
