# Humanoid Robot Sentiment Analysis Pipeline

**DS3022 Data Project 3**  
A real-time sentiment analysis pipeline for analyzing public opinion about humanoid robots from YouTube comments using streaming architecture, workflow orchestration, and NLP.

---

## 📊 Project Overview

This project analyzes **106,759+ YouTube comments** across **19 videos** about humanoid robots (demos, news coverage, and reviews) to understand public sentiment toward AI-powered robotics. The pipeline combines modern data engineering tools with natural language processing to reveal insights about how people perceive this emerging technology.

### Why This Matters

Humanoid robots are transitioning from science fiction to reality (Tesla Optimus, Figure AI, Boston Dynamics). Understanding public sentiment is crucial for:
- **Technology adoption**: Identifying concerns vs. excitement
- **Product development**: Understanding what features resonate
- **Policy & ethics**: Gauging societal readiness for human-robot interaction
- **Market research**: Tracking sentiment shifts over time

---

## 🏗️ Architecture

```
YouTube API → Kafka Stream → DuckDB → Sentiment Analysis → Streamlit Dashboard
     ↓            ↓            ↓              ↓                    ↓
  Fetch       Stream       Store         Analyze            Visualize
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Source** | YouTube Data API v3 | Fetch comments with metadata |
| **Streaming** | Apache Kafka | Real-time comment ingestion |
| **Orchestration** | Prefect | Workflow management, retries, logging |
| **Database** | DuckDB | Analytical queries on structured data |
| **NLP** | NLTK (VADER) + RAKE | Sentiment analysis + phrase extraction |
| **Visualization** | Streamlit + Plotly | Interactive dashboard |
| **Containerization** | Docker Compose | Kafka infrastructure |

---

## 🚀 Setup Instructions

### Prerequisites
- Python 3.9+
- Docker Desktop (for Kafka)
- YouTube Data API Key ([Get one here](https://developers.google.com/youtube/v3/getting-started))

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd Humanoid-Sentiment-Analysis
```

### 2. Install Python Dependencies
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure API Key
Create a `.env` file in the project root:
```bash
YOUTUBE_API_KEY=your_api_key_here
```

### 4. Start Kafka Infrastructure
```bash
docker-compose up -d
```

Verify Kafka is running:
- Kafka UI: http://localhost:8081

### 5. Run the Pipeline
```bash
cd src
python pipeline.py
```

This will:
1. Fetch comments from YouTube (incremental - only new comments after first run)
2. Stream to Kafka topic `raw_comments`
3. Ingest to DuckDB
4. Perform sentiment analysis
5. Extract key phrases

### 6. Launch Dashboard
```bash
streamlit run src/dashboard.py
```

Access at: http://localhost:8501

---

## 📁 Project Structure

```
Humanoid-Sentiment-Analysis/
├── config/
│   └── videos.yaml              # Video IDs organized by category
├── data/
│   ├── raw/                     # JSON files (one per video)
│   └── youtube.duckdb           # Analytical database
├── src/
│   ├── fetch_comments.py        # YouTube API → Kafka
│   ├── process_comments.py      # Kafka → DuckDB
│   ├── sentiment_analysis.py    # VADER + RAKE NLP
│   ├── pipeline.py              # Prefect orchestration
│   └── dashboard.py             # Streamlit visualization
├── docker-compose.yaml          # Kafka + Zookeeper + UI
├── requirements.txt             # Python dependencies
└── README.md
```

---

## 🎯 Key Features

### 1. Incremental Data Fetching
- **First run**: Fetches all historical comments
- **Subsequent runs**: Only fetches new comments (compares against existing IDs)
- **Smart pagination**: Stops early after 3 empty pages in incremental mode

### 2. Streaming Architecture
- Real-time streaming to Kafka for scalability
- Decouples data ingestion from processing
- Enables parallel consumers and replay capability

### 3. Idempotent Pipeline
- Safe to re-run without duplicating data
- Upsert logic handles comment updates (likes, replies)
- Only processes comments where `sentiment_label IS NULL`

### 4. Robust Error Handling
- Prefect retries (2 attempts with 10s delay)
- Graceful degradation (works without Kafka if unavailable)
- API quota management with rate limiting

### 5. Real-Time Dashboard
- 5 interactive visualizations
- Filters by category and sentiment
- Auto-refresh option (30s intervals)

---

## 📈 Data Statistics

### Current Dataset
- **Total Comments**: 106,759
- **Total Videos**: 19
- **Categories**: Demos (6), News Coverage (7), Reviews (6)
- **Date Range**: July 2025 - November 2025

### Sentiment Distribution
| Sentiment | Count | Percentage | Avg Score |
|-----------|-------|------------|-----------|
| Neutral | 53,553 | 50.2% | 0.000 |
| Positive | 35,077 | 32.9% | +0.495 |
| Negative | 18,129 | 17.0% | -0.443 |

### Category Breakdown
| Category | Comments | Avg Sentiment |
|----------|----------|---------------|
| Reviews | 66,078 (61.9%) | +0.088 |
| Demos | 35,769 (33.5%) | +0.091 |
| News Coverage | 4,912 (4.6%) | +0.060 |

---

## 🔍 Key Findings

See [FINDINGS.md](FINDINGS.md) for detailed analysis.

### Highlights:
1. **Overall Positive Trend**: 50.2% neutral, 32.9% positive, only 17% negative
2. **Category Insights**: Reviews and demos have nearly identical positive sentiment (~0.09), while news coverage is more neutral
3. **High Engagement**: Top video has 40K+ comments (MrBeast "World's Fastest Man Vs Robot!" collaboration)
4. **Temporal Spikes**: Major product announcements and viral content drive comment volume
5. **Pop Culture Effect**: "mr beast" and "detroit become human" are top discussed phrases

---

## 🛠️ Challenges Overcome

### 1. API Rate Limits
- **Challenge**: YouTube API quota of 10,000 units/day
- **Solution**: Implemented incremental fetching, pagination delays (0.2s), early stopping

### 2. Scale
- **Challenge**: Processing 100K+ comments efficiently
- **Solution**: Batch updates to DuckDB, used analytical database for fast queries

### 3. Data Quality
- **Challenge**: Comments with special characters, emojis, spam
- **Solution**: Text cleaning pipeline, filler phrase filtering

### 4. Kafka Complexity
- **Challenge**: Managing distributed infrastructure
- **Solution**: Docker Compose for easy setup/teardown, graceful fallback if unavailable

### 5. Phrase Extraction Noise
- **Challenge**: RAKE extracted generic phrases like "looks like", "feels like"
- **Solution**: Custom filter list for filler patterns, min length requirements (5+ chars)

---

## 🎨 Visualizations

The dashboard includes:
1. **Sentiment Distribution by Category** (grouped bar chart)
2. **Word Clouds by Sentiment** (positive/neutral/negative phrases)
3. **Sentiment Over Time** (line chart from July 2025)
4. **Top 20 Most Common Phrases** (horizontal bar chart)
5. **Comment Volume by Video** (horizontal bar with sentiment heatmap)

---

## 🔄 Workflow Orchestration

Prefect manages the pipeline with:
- **Task dependencies**: Fetch → Process → Analyze
- **Retries**: 2 attempts with 10s backoff
- **Logging**: Structured logs for debugging
- **Summary reports**: Post-run statistics

View flow runs:
```bash
prefect server start
# Navigate to http://localhost:4200
```

---

## 📊 Adding New Videos

Edit `config/videos.yaml`:
```yaml
categories:
  demos:
    - video_id: "NEW_VIDEO_ID_HERE"
```

Then re-run `python pipeline.py` - it will automatically fetch the new video.

---

## 🚧 Limitations & Future Work

### Current Limitations
1. **Language**: English-only videos (relies on VADER sentiment)
2. **API Quota**: Limited to ~50K new comments per day
3. **Sentiment Model**: VADER is rule-based, not context-aware
4. **Replies**: Only analyzes top-level comments, not reply threads

### Potential Enhancements
1. Use transformer-based sentiment (BERT, RoBERTa) for better accuracy
2. Add reply thread analysis for deeper context
3. Implement topic modeling (LDA) for theme extraction
4. Create alerting for sentiment anomalies
5. Expand to multilingual support
6. Add video metadata correlation (views, likes, upload date)

---

## 👥 Contributors

- [Minu Choi] - [qce2dp@virginia.edu]
- [Patrick Ho] - [bqu3tr@virginia.edu]

---

## 🙏 Acknowledgments

- YouTube Data API v3 for data access
- Confluent Kafka for streaming infrastructure
- DuckDB team for the analytical database
- Prefect for workflow orchestration
- Streamlit team for visualization framework
