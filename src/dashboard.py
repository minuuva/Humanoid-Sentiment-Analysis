#!/usr/bin/env python3
"""
Real-time Streamlit Dashboard for YouTube Comment Sentiment Analysis
Displays live-updating visualizations from DuckDB
"""

import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
from pathlib import Path
import json
import time

# Page config
st.set_page_config(
    page_title="Humanoid Robot Sentiment Analysis",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 2rem;
        color: #1f77b4;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        text-align: center;
    }
    </style>
""", unsafe_allow_html=True)

# Database connection
@st.cache_resource
def get_db_connection():
    """Get DuckDB connection."""
    return duckdb.connect('data/youtube.duckdb', read_only=True)

@st.cache_data
def load_video_titles():
    """Load actual video titles from JSON files."""
    video_titles = {}
    data_dir = Path('data/raw')
    
    if data_dir.exists():
        for json_file in data_dir.glob('*.json'):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    video_id = json_file.stem
                    video_title = data.get('video_title', video_id)
                    video_titles[video_id] = video_title
            except Exception as e:
                # If we can't read the file, use video ID as fallback
                video_titles[json_file.stem] = json_file.stem
    
    return video_titles

def load_data():
    """Load all data from DuckDB."""
    conn = get_db_connection()
    
    # Main comments data
    df = conn.execute("""
        SELECT 
            comment_id,
            video_id,
            category,
            text,
            like_count,
            published_at,
            sentiment_label,
            sentiment_score,
            phrases
        FROM comments
        WHERE sentiment_label IS NOT NULL
    """).df()
    
    return df

def extract_all_phrases(df):
    """Extract all phrases from the JSON column - only multi-word, meaningful phrases."""
    all_phrases = []
    
    # Filler phrases to exclude
    filler_patterns = {
        'looks like', 'look like', 'feel like', 'feels like', 
        'seems like', 'seem like', 'sounds like', 'sound like',
        'kinda like', 'kind like', 'sorta like', 'sort like',
        'really like', 'really cool', 'really good', 'really nice',
        'thats like', 'thats pretty', 'going like', 'walks like'
    }
    
    for phrases_json in df['phrases'].dropna():
        try:
            phrases = json.loads(phrases_json) if isinstance(phrases_json, str) else phrases_json
            if isinstance(phrases, list):
                for p in phrases:
                    # Only keep multi-word phrases (2+ words)
                    if len(p.split()) >= 2:
                        # Skip filler phrases
                        if p.lower().strip() not in filler_patterns:
                            # Skip if too short
                            if len(p) >= 5:
                                all_phrases.append(p)
        except:
            continue
    return all_phrases

def create_wordcloud(phrases_list, title, colormap='viridis'):
    """Generate a word cloud from phrases."""
    if not phrases_list:
        return None
    
    # Join phrases into a single string
    text = ' '.join(phrases_list)
    
    if not text.strip():
        return None
    
    wordcloud = WordCloud(
        width=800,
        height=400,
        background_color='white',
        colormap=colormap,
        max_words=50,
        relative_scaling=0.5,
        min_font_size=10
    ).generate(text)
    
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.axis('off')
    ax.set_title(title, fontsize=16, fontweight='bold')
    return fig

def main():
    """Main dashboard function."""
    
    # Header
    st.markdown('<h1 class="main-header">🤖 Humanoid Robot Sentiment Analysis Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("Real-time analysis of public sentiment towards AI-powered robots from YouTube comments")
    
    # Sidebar
    st.sidebar.title("⚙️ Dashboard Controls")
    auto_refresh = st.sidebar.checkbox("Auto-refresh (every 30s)", value=False)
    
    if auto_refresh:
        st.sidebar.info("Dashboard will refresh every 30 seconds")
        time.sleep(0.1)  # Small delay to allow UI update
    
    # Load data
    with st.spinner("Loading data from DuckDB..."):
        df = load_data()
    
    if df.empty:
        st.error("No data found in database. Please run the ingestion and analysis pipeline first.")
        return
    
    # Convert published_at to datetime
    df['published_at'] = pd.to_datetime(df['published_at'])
    
    # Sidebar filters
    st.sidebar.markdown("---")
    st.sidebar.subheader("📊 Filters")
    
    categories = ['All'] + sorted(df['category'].unique().tolist())
    selected_category = st.sidebar.selectbox("Category", categories)
    
    sentiments = ['All'] + sorted(df['sentiment_label'].unique().tolist())
    selected_sentiment = st.sidebar.selectbox("Sentiment", sentiments)
    
    # Apply filters
    filtered_df = df.copy()
    if selected_category != 'All':
        filtered_df = filtered_df[filtered_df['category'] == selected_category]
    if selected_sentiment != 'All':
        filtered_df = filtered_df[filtered_df['sentiment_label'] == selected_sentiment]
    
    # Key Metrics
    st.markdown("---")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Comments", f"{len(filtered_df):,}")
    
    with col2:
        positive_pct = (filtered_df['sentiment_label'] == 'positive').sum() / len(filtered_df) * 100
        st.metric("Positive", f"{positive_pct:.1f}%")
    
    with col3:
        neutral_pct = (filtered_df['sentiment_label'] == 'neutral').sum() / len(filtered_df) * 100
        st.metric("Neutral", f"{neutral_pct:.1f}%")
    
    with col4:
        negative_pct = (filtered_df['sentiment_label'] == 'negative').sum() / len(filtered_df) * 100
        st.metric("Negative", f"{negative_pct:.1f}%")
    
    with col5:
        avg_score = filtered_df['sentiment_score'].mean()
        st.metric("Avg Score", f"{avg_score:.3f}")
    
    st.markdown("---")
    
    # VISUALIZATION 1: Sentiment Distribution by Category
    st.subheader("📊 1. Sentiment Distribution by Category")
    
    # Use unfiltered df to show all categories
    sentiment_by_category = df.groupby(['category', 'sentiment_label']).size().reset_index(name='count')
    
    # Ensure we have data
    if not sentiment_by_category.empty:
        fig1 = px.bar(
            sentiment_by_category,
            x='category',
            y='count',
            color='sentiment_label',
            barmode='group',
            title='Sentiment Breakdown Across Categories',
            labels={'count': 'Number of Comments', 'category': 'Category', 'sentiment_label': 'Sentiment'},
            color_discrete_map={'positive': '#2ecc71', 'neutral': '#95a5a6', 'negative': '#e74c3c'},
            height=500
        )
        fig1.update_layout(xaxis_title="Category", yaxis_title="Comment Count")
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.warning("No data available for sentiment by category")
    
    # VISUALIZATION 2: Word Clouds by Sentiment
    st.markdown("---")
    st.subheader("☁️ 2. Top Phrases by Sentiment")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Positive Comments**")
        positive_df = filtered_df[filtered_df['sentiment_label'] == 'positive']
        positive_phrases = extract_all_phrases(positive_df)
        fig_wc_pos = create_wordcloud(positive_phrases, 'Positive Phrases', 'Greens')
        if fig_wc_pos:
            st.pyplot(fig_wc_pos)
        else:
            st.info("No phrases available")
    
    with col2:
        st.markdown("**Neutral Comments**")
        neutral_df = filtered_df[filtered_df['sentiment_label'] == 'neutral']
        neutral_phrases = extract_all_phrases(neutral_df)
        fig_wc_neu = create_wordcloud(neutral_phrases, 'Neutral Phrases', 'Blues')
        if fig_wc_neu:
            st.pyplot(fig_wc_neu)
        else:
            st.info("No phrases available")
    
    with col3:
        st.markdown("**Negative Comments**")
        negative_df = filtered_df[filtered_df['sentiment_label'] == 'negative']
        negative_phrases = extract_all_phrases(negative_df)
        fig_wc_neg = create_wordcloud(negative_phrases, 'Negative Phrases', 'Reds')
        if fig_wc_neg:
            st.pyplot(fig_wc_neg)
        else:
            st.info("No phrases available")
    
    # VISUALIZATION 3: Sentiment Timeline
    st.markdown("---")
    st.subheader("📈 3. Sentiment Over Time")
    
    # Group by date
    timeline_df = filtered_df.copy()
    timeline_df['date'] = timeline_df['published_at'].dt.date
    
    # Filter to start from July 1st, 2025 (tracking recent comments)
    start_date = pd.to_datetime('2025-07-01').date()
    timeline_df = timeline_df[timeline_df['date'] >= start_date]
    
    timeline_sentiment = timeline_df.groupby(['date', 'sentiment_label']).size().reset_index(name='count')
    
    fig3 = px.line(
        timeline_sentiment,
        x='date',
        y='count',
        color='sentiment_label',
        title='Sentiment Trends Over Time',
        labels={'count': 'Number of Comments', 'date': 'Date', 'sentiment_label': 'Sentiment'},
        color_discrete_map={'positive': '#2ecc71', 'neutral': '#95a5a6', 'negative': '#e74c3c'},
        height=500
    )
    fig3.update_layout(xaxis_title="Date", yaxis_title="Comment Count")
    st.plotly_chart(fig3, use_container_width=True)
    
    # VISUALIZATION 4: Top Phrases Overall
    st.markdown("---")
    st.subheader("🔤 4. Most Common Phrases")
    
    all_phrases = extract_all_phrases(filtered_df)
    if all_phrases:
        from collections import Counter
        phrase_counts = Counter(all_phrases).most_common(20)
        phrase_df = pd.DataFrame(phrase_counts, columns=['Phrase', 'Count'])
        
        fig4 = px.bar(
            phrase_df,
            x='Count',
            y='Phrase',
            orientation='h',
            title='Top 20 Most Frequent Phrases',
            labels={'Count': 'Frequency', 'Phrase': 'Phrase'},
            color='Count',
            color_continuous_scale='viridis',
            height=600
        )
        fig4.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("No phrases available for selected filters")
    
    # VISUALIZATION 5: Comment Volume by Video
    st.markdown("---")
    st.subheader("🎥 5. Comment Volume by Video")
    
    # Load actual video titles from JSON files
    video_titles = load_video_titles()
    
    video_counts = filtered_df.groupby('video_id').agg({
        'comment_id': 'count',
        'sentiment_score': 'mean',
        'category': 'first'
    }).reset_index()
    video_counts.columns = ['video_id', 'comment_count', 'avg_sentiment', 'category']
    video_counts = video_counts.sort_values('comment_count', ascending=False)
    
    # Limit to top 12 videos for better readability
    video_counts = video_counts.head(12)
    
    # Add video titles with truncation for display
    video_counts['video_title_full'] = video_counts['video_id'].map(video_titles).fillna(video_counts['video_id'])
    
    # Truncate long titles for display (show full in hover)
    def truncate_title(title, max_length=60):
        if len(title) <= max_length:
            return title
        return title[:max_length].rsplit(' ', 1)[0] + '...'
    
    video_counts['video_title_short'] = video_counts['video_title_full'].apply(truncate_title)
    
    # Sort by comment_count for horizontal bar chart (ascending so highest is on top)
    video_counts = video_counts.sort_values('comment_count', ascending=True)
    
    # Use horizontal bar chart for better label readability
    fig5 = px.bar(
        video_counts,
        y='video_title_short',
        x='comment_count',
        color='avg_sentiment',
        title='Top 12 Videos by Comment Volume & Avg Sentiment',
        labels={
            'comment_count': 'Number of Comments', 
            'video_title_short': 'Video', 
            'avg_sentiment': 'Avg Sentiment Score'
        },
        color_continuous_scale='RdYlGn',
        color_continuous_midpoint=0,
        height=600,
        orientation='h',
        hover_data={
            'video_title_short': False,  # Hide truncated version
            'video_title_full': True,     # Show full title
            'category': True, 
            'video_id': False,
            'comment_count': ':,',
            'avg_sentiment': ':.3f'
        }
    )
    fig5.update_layout(
        xaxis_title="Comment Count", 
        yaxis_title="Video",
        yaxis={'tickfont': {'size': 11}},
        hovermode='closest',
        margin=dict(l=20, r=20, t=50, b=50)
    )
    st.plotly_chart(fig5, use_container_width=True)
    
    # Show total count info
    total_videos = len(filtered_df['video_id'].unique())
    if total_videos > 12:
        st.caption(f"📊 Showing top 12 of {total_videos} total videos (hover bars for full details)")
    
    # Additional Stats
    st.markdown("---")
    st.subheader("📋 Additional Statistics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Sentiment Distribution**")
        sentiment_dist = filtered_df['sentiment_label'].value_counts()
        fig_pie = px.pie(
            values=sentiment_dist.values,
            names=sentiment_dist.index,
            title='Overall Sentiment Distribution',
            color=sentiment_dist.index,
            color_discrete_map={'positive': '#2ecc71', 'neutral': '#95a5a6', 'negative': '#e74c3c'}
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        st.markdown("**Category Distribution**")
        # Use unfiltered df to show all categories
        category_dist = df['category'].value_counts()
        if not category_dist.empty:
            fig_pie2 = px.pie(
                values=category_dist.values,
                names=category_dist.index,
                title='Comments by Category'
            )
            st.plotly_chart(fig_pie2, use_container_width=True)
        else:
            st.warning("No category data available")
    
    # Footer
    st.markdown("---")
    st.markdown(f"**Last updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.markdown("**Data Source:** YouTube Data API v3 | **Analysis:** VADER Sentiment + RAKE Phrase Extraction | **Storage:** DuckDB")
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(30)
        st.rerun()

if __name__ == "__main__":
    main()

