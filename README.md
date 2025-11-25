# Humanoid-Sentiment-Analysis

### Names of Team Members
Minu Choi and Patrick Ho

### Data Source
Youtube Videos Related to Humanoid Robots (check videos.yaml)

### Instructions

After forking this repository, please start a virtual environment and export your own Youtube API as an environment variable.

```bash
export YOUTUBE_API_KEY="your_youtube_api_key"
```

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
or 
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

Building a Prefect workflow for our pipeline is challenging because our previous scripts for fetching and processing comments weren’t originally designed for orchestration. It was hard to adjust with Prefect in order to pass data between steps or deal with side effects like file writes and Kafka messages. Handling external systems such as the YouTube API, Kafka, and databases adds layer of complexity through rate limits, connectivity issues, and the need for retries. 

We addressed this challenge by importing the functions from the scripts and running them in a sequence. Each task returns video IDs, which avoids Prefect’s serialization problems and makes the workflow predictable. With log_prints=True, all logs show up directly in the terminal, so we can see exactly what is happening at each step. By keeping the flow clear, it becomes easy to debug and monitor. 

With our focus being on sentiment analysis, one of our other biggest challenges was addressing the messiness of comments. The comments contain emojis, slang, excess whitespace, and non-standard punctuation. 

We implemented a clean_text function using Python to normalize whitespace. With the context of the texts, we deliberately kept punctuation, such as !, because VADER uses exclamation marks to boost sentiment intensity.

### Analysis of Data 
