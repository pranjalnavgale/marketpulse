import sqlite3
import random
import datetime
import pandas as pd
from typing import List, Dict, Any
from faker import Faker
from pydantic import BaseModel, Field

# ----------------------------
# 1. SETUP & CONFIGURATION
# ----------------------------
fake = Faker("en_IN")
DB_NAME = "blackant_llama_structured.db"
# MSME_DATA_FILE path is omitted as it points to a local path and is not used in the core logic

# --- Constants updated for better simulation ---
ACTIVITIES = ["Textile & Apparel", "Automobile Parts", "Food Processing", "Rubber Goods", "Electronics Manufacturing", "Pharmaceuticals", "Metal Fabrication"]
HSN_CODES = ["6101", "8708", "2106", "4016", "8542", "3004", "7308"]
CATEGORIES = ["policy", "innovation", "demand_trend", "supply_risk"]
LOCATIONS = ["Mumbai", "Delhi", "Surat", "Jaipur", "Indore", "Chennai"]
LOCATIONS_DISTRICTS = ["South Delhi", "Thane", "Surat", "Jaipur", "Indore", "Chennai Central", "Pune", "Kolkata", "Ahmedabad", "Bangalore"]

# --- Mapping Activity to HSN Code for consistency and integrity ---
ACTIVITY_TO_HSN = dict(zip(ACTIVITIES, HSN_CODES))
DB_INITIALIZED = False 

# --- Pydantic Model for Data Integrity ---
class TrendInsight(BaseModel):
    headline: str
    activity: str
    hsn_code: str
    category: str
    location: str
    trend_score: float = Field(..., ge=0.0, le=100.0)
    monthly_growth_forecast: float
    date: str

# --- Helper functions for DB connection and initialization ---
def get_db_connection():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes the database schema if it hasn't been already."""
    global DB_INITIALIZED
    if DB_INITIALIZED:
        return
        
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS insights (
            id INTEGER PRIMARY KEY,
            headline TEXT NOT NULL,
            activity TEXT NOT NULL,
            hsn_code TEXT NOT NULL,
            category TEXT NOT NULL,
            location TEXT NOT NULL,
            trend_score REAL NOT NULL,
            monthly_growth_forecast REAL NOT NULL,
            date TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    DB_INITIALIZED = True

def get_msme_lookup_data() -> pd.DataFrame:
    """Placeholder for loading MSME lookup data."""
    # This is a simulation/placeholder for a large dataset
    data = []
    for _ in range(1000):
        activity = random.choice(ACTIVITIES)
        data.append({
            "EnterpriseName": fake.company(),
            "District": random.choice(LOCATIONS_DISTRICTS),
            "Industry": activity,
            "HSNCode": ACTIVITY_TO_HSN.get(activity, "0000")
        })
    return pd.DataFrame(data)

# ----------------------------
# 2. DATA GENERATION AND INSERTION
# ----------------------------

def run_ai_simulation(n: int = 50, user_activity: str = None, user_hsn: str = None):
    """Generates 'n' random trend insights and inserts them into the database."""
    init_db()
    conn = get_db_connection()
    c = conn.cursor()
    
    # Clear old data for a fresh run
    c.execute("DELETE FROM insights")

    for i in range(n):
        # Prioritize user's activity for the first third of data
        is_user_priority = i < n / 3
        
        if is_user_priority and user_activity:
            activity = user_activity
        else:
            activity = random.choice(ACTIVITIES)

        if is_user_priority and user_hsn:
            hsn_code = user_hsn
        else:
            hsn_code = ACTIVITY_TO_HSN.get(activity) or random.choice(HSN_CODES)

        category = random.choice(CATEGORIES)
        location = random.choice(LOCATIONS)

        # GENERATING THE EXACT SCORES (Randomly constrained)
        trend_score = round(random.uniform(50.0, 95.0), 2)
        monthly_growth_forecast = round(random.uniform(-5.0, 10.0), 2)
        
        date = (datetime.date.today() - datetime.timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")

        # Generate a semi-sensible headline
        headline = f"Demand surge for eco-friendly fibers, HSN **{hsn_code}** sees +{abs(monthly_growth_forecast):.1f}% forecast."
        if category == "supply_risk":
             headline = f"Raw material price spike for generics, HSN **{hsn_code}** supply under pressure in {location}."
        elif category == "policy":
             headline = f"Policy changes stabilize yarn prices in {location} region for HSN **{hsn_code}**."
        elif category == "innovation":
             headline = f"Bio-tech innovation cluster opens in {location}, offering new collaboration opportunities for HSN **{hsn_code}**."

        insight = TrendInsight(
            headline=headline,
            activity=activity,
            hsn_code=hsn_code,
            category=category,
            location=location,
            trend_score=trend_score,
            monthly_growth_forecast=monthly_growth_forecast,
            date=date
        )

        c.execute("""
            INSERT INTO insights (headline, activity, hsn_code, category, location, trend_score, monthly_growth_forecast, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, tuple(insight.dict().values()))

    conn.commit()
    conn.close()

# ----------------------------
# 3. DATA FETCHING AND RANKING LOGIC
# ----------------------------

def fetch_and_rank_data(user_activity: str = None, limit: int = None) -> pd.DataFrame:
    """
    Fetches data, calculates the composite Index, and ranks the insights.
    If user_activity is provided, it applies a relevance score boost.
    """
    init_db()
    conn = get_db_connection()
    query = "SELECT * FROM insights"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        return df

    # 1. CALCULATE THE COMPOSITE INDEX (The exact score)
    # Index = (Trend Score * 0.7) + (Max(0, Growth Forecast) * 3)
    df['index'] = (
        df['trend_score'] * 0.7 
    ) + (
        df['monthly_growth_forecast'].apply(lambda x: max(0, x)) * 3
    )

    # 2. Apply a Relevance Score Boost for User Activity
    if user_activity:
        df['Relevance_Score'] = df.apply(
            lambda row: 1.2 if row['activity'] == user_activity else 1.0, axis=1
        )
        # Apply boost to the index (e.g., a 20% boost)
        df['index'] = df['index'] * df['Relevance_Score']
    else:
        # Default relevance score for global view
        df['Relevance_Score'] = 1.0 

    # 3. Rank the data based on the calculated Index (descending order)
    df = df.sort_values(by='index', ascending=False).reset_index(drop=True)
    df['Rank'] = df.index + 1
    
    # 4. Final Formatting and Limit
    df['trend_score'] = df['trend_score'].round(2)
    df['monthly_growth_forecast'] = df['monthly_growth_forecast'].round(2)
    df['index'] = df['index'].round(2)

    if limit:
        df = df.head(limit)
        
    # Return the clean, ranked DataFrame
    return df.drop(columns=['id', 'Relevance_Score'], errors='ignore').reset_index(drop=True)

# ----------------------------
# 4. SCRIPT EXECUTION (Testing)
# ----------------------------

if __name__ == "__main__":
    print("\n--- Starting AI Trend Data Simulation ---")
    
    # 1. Run the simulation to generate and insert 50 data points
    run_ai_simulation(n=50, user_activity="Textile & Apparel")

    print("\n--- Fetching and Ranking All Data (Global View) ---")
    # 2. Fetch and display the top 10 ranked insights (globally)
    all_ranked_df = fetch_and_rank_data(limit=10)
    
    if not all_ranked_df.empty:
        print("\nTop 10 Global Market Insights:")
        print(all_ranked_df[['Rank', 'headline', 'activity', 'index', 'monthly_growth_forecast']].to_markdown(index=False))
    else:
        print("No data found in the database.")
        
    print("\n--- Fetching and Ranking Data for 'Automobile Parts' User (Prioritized) ---")
    # 3. Fetch and display data prioritized for a specific user activity
    user_activity = "Automobile Parts"
    user_ranked_df = fetch_and_rank_data(user_activity=user_activity, limit=10)
    
    if not user_ranked_df.empty:
        print(f"\nTop 10 Insights for User in **{user_activity}** (Prioritized):")
        print(user_ranked_df[['Rank', 'headline', 'activity', 'index', 'monthly_growth_forecast']].to_markdown(index=False))
    else:
        print(f"No data found in the database for {user_activity}.")