# eCFR Analyzer
### Description:
The eCFR Analyzer is a simple website to analyze U.S. Federal Regulations. The eCFR is available at https://www.ecfr.gov/. The public API is documented at https://www.ecfr.gov/developers/documentation/api/v1.

The website allows a user to download the current eCFR and analyze it for items such as word count per agency and historical changes over time. Custom metrics include an alignment score that measures the **insert technical term** between the regulations and their changes compared to stated goals. 

The website also allows a user to visualize the content, explore the data, and query search terms. 

### Project Structure:
```ecfr_analyzer/
│── src/
│   ├── api/  
│   │   ├── main.py
│   ├── app/    
│   │   ├── main.py
│── tests/  
│── docs/  
│── config/  
│── scripts/  
│── .env.example  
│── Dockerfile  
│── requirements.txt  
│── package.json  
│── README.md  
```

### Setup
1. Check if Python is installed on your machine by opening a terminal window and typing `python` then pressing `Enter`. If Python is not installed you can install it at https://www.python.org/downloads/.

2. Open this project in a text editor like Visual Studio Code. 

3. Open a new terminal in the project root directory and create a new Python virtual environment to isolate package downloads for this project. Activate the environment prior to installing requirements.
```
python -m venv venv
source venv/bin/activate
```
Or on Windows:
`venv\Scripts\activate`

4. Install the Python package requirements for this project.
```
python -m pip install -r requirements.txt
```

5. Download the data (expect this to take several minutes).

```
python src/api/ecrf_client.py
```

6. Load the data into local data warehouse
```
python src/api/db_loader.py
```

7. Start the API
```
python src/api/main.py
```

8. Start the Streamlit App using a new terminal window
```
streamlit run src/app/main.py
```

