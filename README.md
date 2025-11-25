# Humanoid-Sentiment-Analysis

### Names of Team Members
Minu Choi and Patrick Ho

### Data Source
Youtube Videos Related to Humanoid Robots 

### Instructions

After forking this repository, please start a virtual environment and export your own Youtube API as an environment variable. 

Next, run 
```bash
pip install -r requirements.txt
```
to install all packages 

Our entire pipeline is wrapped in a prefect workflow
1. Run
```bash
prefect server start
```
on a seperate terminal
2. Run 
```bash
python pipeline.py
```
or '
```bash
python3 pipeline.py
```
on the main terminal
3. After the prefect flow ends, run 
```bash
streamlit run dashboard.py
```
to see visualizations

### What challenges did this data choice present in data gathering, processing and analysis, and how did you work through them? What methods and tools did you use to work with this data?

With our focus being on sentiment analysis, one of our biggest challenges was addressing the messiness of comments. 

### Analysis of Data 
