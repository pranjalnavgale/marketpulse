import sqlite3
import random
import datetime
import json
import os
import pandas as pd
import hashlib 
from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from flask_cors import CORS
from faker import Faker
from pydantic import BaseModel, Field
from typing import List

# ----------------------------
# 1. SETUP & CONFIGURATION
# ----------------------------
fake = Faker("en_IN")
DB_NAME = "blackant_llama_structured.db" 
MSME_DATA_FILE = "csv.csv"  # File for MSME lookup
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(24))
CORS(app) 

# --- Constants updated for better simulation ---
ACTIVITIES = ["Textile & Apparel", "Automobile Parts", "Food Processing", "Rubber Goods", "Electronics Manufacturing", "Pharmaceuticals", "Metal Fabrication"]
HSN_CODES = ["6101", "8708", "2106", "4016", "8542", "3004", "7308"]
CATEGORIES = ["policy", "innovation", "demand_trend", "supply_risk"]
LOCATIONS = ["Mumbai", "Delhi", "Surat", "Jaipur", "Indore", "Chennai"]
LOCATIONS_DISTRICTS = ["South Delhi", "Thane", "Surat", "Jaipur", "Indore", "Chennai Central", "Pune", "Kolkata", "Ahmedabad", "Bangalore"]

# --- Mapping Activity to HSN Code for consistency and integrity ---
ACTIVITY_TO_HSN = dict(zip(ACTIVITIES, HSN_CODES))


# --- User Authentication Functions and Store ---
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

users = { 
    "test@example.com": {
        "password_hash": hash_password("password123"), 
        "name": "Test User", 
        "enterprise_name": "TestCorp MSME Ltd.",
        "activity": "Textile & Apparel", 
        "hsn_code": "6101"
    },
}

# --- MSME Data Generator/Loader (with Fallback) ---
def generate_fake_msme_lookup_data(n: int = 50) -> pd.DataFrame:
    """Generates a fake DataFrame for MSME lookup when csv.csv is missing."""
    data = []
    for _ in range(n):
        industry = random.choice(ACTIVITIES)
        hsn = ACTIVITY_TO_HSN.get(industry, "0000")
        data.append({
            "EnterpriseName": f"{fake.company()} {random.choice(['MSME', 'Pvt Ltd', 'Co.'])}",
            "District": random.choice(LOCATIONS_DISTRICTS),
            "Industry": industry,
            "HSNCode": hsn,
        })
    df = pd.DataFrame(data)
    print(f"✅ SIMULATION: Generated {len(df)} fake MSME lookup records.")
    return df

try:
    msme_data = pd.read_csv(MSME_DATA_FILE, on_bad_lines='skip', engine='python')
    # Add simulation columns if missing
    if 'Industry' not in msme_data.columns:
        msme_data['Industry'] = msme_data['EnterpriseName'].apply(lambda x: random.choice(ACTIVITIES))
    if 'HSNCode' not in msme_data.columns:
        msme_data['HSNCode'] = msme_data['Industry'].apply(lambda x: ACTIVITY_TO_HSN.get(x, random.choice(HSN_CODES)))
    
    print(f"✅ Successfully loaded {len(msme_data)} MSME lookup records.")
except FileNotFoundError:
    print(f"⚠️ MSME data file ({MSME_DATA_FILE}) not found. Generating fake data for lookup.")
    msme_data = generate_fake_msme_lookup_data()
except Exception as e:
    msme_data = pd.DataFrame()
    print(f"⚠️ Error loading MSME data: {e}")
    
DB_INITIALIZED = False 

# --- Pydantic Schema ---
class TrendInsight(BaseModel):
    headline: str 
    name: str 
    activity: str 
    hsn_code: str 
    category: str 
    location: str 
    trend_score: int 
    monthly_growth_forecast: float 
    index: float 
    date: str 

# ----------------------------
# 2. DATABASE & SIMULATION FUNCTIONS
# ----------------------------

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    global DB_INITIALIZED
    if DB_INITIALIZED:
        return
    conn = get_db_connection()
    cursor = conn.cursor()
    # Ensure hsn_code is NOT NULL
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ai_msme_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            headline TEXT NOT NULL,
            name TEXT NOT NULL,
            activity TEXT NOT NULL,
            hsn_code TEXT NOT NULL,      
            category TEXT NOT NULL,
            location TEXT NOT NULL,
            trend_score INTEGER NOT NULL,
            monthly_growth_forecast REAL NOT NULL,
            'index' REAL NOT NULL,
            date TEXT NOT NULL
        );
    """)
    conn.commit()
    conn.close()
    DB_INITIALIZED = True

HEADLINE_TEMPLATES = {
    "Textile & Apparel": [
        "Demand surge for eco-friendly fibers, HSN {hsn} sees +{growth}% forecast.",
        "Policy changes stabilize yarn prices in Surat region.",
    ],
    "Automobile Parts": [
        "EV component supply chain disruption risks, monitor HSN {hsn}.",
        "Government incentives drive Q3 demand for small vehicle parts.",
    ],
    "Food Processing": [
        "Export demand for packaged spices (HSN {hsn}) hits a 5-year high.",
        "New food safety policy requires capital investment for compliance.",
    ],
    "Rubber Goods": [
        "Global commodity price volatility impacts natural rubber sourcing (HSN {hsn}).",
        "Supply chain resilience planning essential due to import bottlenecks."
    ]
}

def generate_fake_insights(n: int = 20, user_activity: str = None, user_hsn: str = None) -> List[TrendInsight]:
    insights = []
    
    main_activity = user_activity if user_activity in ACTIVITIES else random.choice(ACTIVITIES)
    main_hsn = user_hsn if user_hsn in HSN_CODES else ACTIVITY_TO_HSN.get(main_activity, random.choice(HSN_CODES))

    for i in range(n):
        if i < n // 3:
            activity = main_activity
            hsn = main_hsn
        else:
            activity = random.choice(ACTIVITIES)
            hsn = ACTIVITY_TO_HSN.get(activity, random.choice(HSN_CODES))
        
        category = random.choice(CATEGORIES)
        trend_score = random.randint(30, 99)
        monthly_growth = round(random.uniform(-1.5, 7.0), 2)
        composite_index = max(0.1, min(10.0, round((trend_score / 100.0) * (monthly_growth / 3.0 + 1.0), 2)))

        template_list = HEADLINE_TEMPLATES.get(activity, HEADLINE_TEMPLATES["Rubber Goods"])
        headline = random.choice(template_list).format(hsn=hsn, growth=f"{monthly_growth:.1f}")

        insight = TrendInsight(
            headline=headline,
            name=f"{fake.company()} MSME Ltd.",
            activity=activity,
            hsn_code=hsn,                  
            category=category,
            location=random.choice(LOCATIONS),
            trend_score=trend_score,
            monthly_growth_forecast=monthly_growth,
            index=composite_index,
            date=datetime.date.today().strftime('%Y-%m-%d')
        )
        insights.append(insight)
    return insights

def run_ai_simulation(n: int = 20, user_activity: str = None, user_hsn: str = None):
    init_db()
    insights = generate_fake_insights(n, user_activity, user_hsn)
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM ai_msme_data")
    conn.commit()
    
    for insight in insights:
        data = insight.model_dump() 
        cursor.execute(
            """
            INSERT INTO ai_msme_data 
            (headline, name, activity, hsn_code, category, location, trend_score, monthly_growth_forecast, 'index', date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (data['headline'], data['name'], data['activity'], data['hsn_code'], data['category'], 
             data['location'], data['trend_score'], data['monthly_growth_forecast'], 
             data['index'], data['date'])
        )
    
    conn.commit()
    conn.close()

def fetch_and_rank_data(user_activity: str = None):
    init_db()
    conn = get_db_connection()
    
    df = pd.read_sql_query("SELECT * FROM ai_msme_data", conn)

    conn.close()

    if df.empty:
        return pd.DataFrame()

    df['Rank'] = df['index'].rank(method='min', ascending=False).astype(int)
    df.columns = [col.replace("'index'", 'index') for col in df.columns]
    
    if user_activity:
        df['Relevance_Score'] = df['activity'].apply(lambda x: 1 if x == user_activity else 0)
        df = df.sort_values(by=['Relevance_Score', 'index'], ascending=[False, False])
    else:
        df = df.sort_values(by=['index'], ascending=False)
    
    return df.drop(columns=['id', 'Relevance_Score'], errors='ignore')

# ----------------------------
# 3. FRONTEND ROUTES 
# ----------------------------

@app.route("/")
def landing_page():
    return redirect(url_for('show_login'))

@app.route("/login")
def show_login():
    return render_template(
        "index.html", 
        error_message=session.pop('error_message', None),
        signup_success_message=session.pop('signup_success_message', None)
    ) 

@app.route('/dashboard')
def dashboard():
    if 'email' not in session:
        session['error_message'] = "You must log in to view the dashboard."
        return redirect(url_for('show_login'))
    
    user_name = session.get('user_name', 'Guest') 
    user_activity = session.get('activity')
    
    df = fetch_and_rank_data(user_activity)
    
    avg_index = df['index'].mean() if not df.empty else 0
    total_records = len(df)
    
    # Data for the table in dashboard.html
    ranking_data = df.to_dict('records')

    # Data for the trend analysis section (FIX: Ensured this is defined)
    trend_analysis = df.groupby('activity')['trend_score'].mean().sort_values(ascending=False).to_dict() if not df.empty else {}
    
    user_data = {
        'name': user_name,
        'activity': user_activity if user_activity else 'General MSME',
        'hsn': session.get('hsn_code', 'N/A'),
        'enterprise_name': session.get('enterprise_name', 'Your Enterprise'),
    }

    return render_template(
        'dashboard.html', 
        user=user_data['name'],              # Used for a simple 'Welcome, {{ user }}' greeting
        user_data=user_data,                 
        avg_index=f"{avg_index:.2f}",
        total_records=total_records,
        ranking_data=ranking_data,           
        trend_analysis=trend_analysis,       # Correctly passed to prevent UndefinedError
    )

@app.route('/generate_trend', methods=['POST'])
def generate_trend():
    if 'email' not in session:
        return redirect(url_for('show_login'))
    
    user_activity = session.get('activity')
    user_hsn = session.get('hsn_code')
    
    run_ai_simulation(n=random.randint(15, 25), user_activity=user_activity, user_hsn=user_hsn) 
    
    return redirect(url_for('dashboard'))

@app.route('/logout')
def logout():
    session.pop('email', None)
    session.pop('user_name', None)
    session.pop('activity', None)
    session.pop('hsn_code', None)
    session.pop('enterprise_name', None)
    return redirect(url_for('show_login'))

# ----------------------------
# 4. API ROUTES
# ----------------------------

@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json()
    email = data.get("email") 
    password = data.get("password")

    user = users.get(email)
    
    if not user or user.get("password_hash") != hash_password(password):
        return jsonify({"error": "Invalid email or password"}), 401

    session['email'] = email
    session['user_name'] = user['name']
    session['activity'] = user.get('activity')
    session['hsn_code'] = user.get('hsn_code')
    session['enterprise_name'] = user.get('enterprise_name')
    
    return jsonify({"message": "Login successful", "redirect_url": url_for('dashboard')}), 200

@app.route("/api/signup", methods=["POST"])
def api_signup():
    data = request.get_json()
    enterprise_name = data.get("name") 
    email = data.get("email") 
    password = data.get("password")
    activity = data.get("activity") 
    hsn_code = data.get("hsn_code") 

    if not all([enterprise_name, email, password, activity, hsn_code]):
        return jsonify({"error": "Missing required fields"}), 400

    if email in users:
        return jsonify({"error": "User already exists"}), 409

    users[email] = {
        "name": email, 
        "enterprise_name": enterprise_name,
        "activity": activity,
        "hsn_code": hsn_code,
        "password_hash": hash_password(password)
    }

    session['signup_success_message'] = "Account created successfully! Please log in."
    
    return jsonify({"message": "User registered successfully", "action": "show_login"}), 201

@app.route("/api/msme-lookup", methods=["GET"])
def msme_lookup():
    """API endpoint for searching MSME data by Enterprise Name, returns Industry and HSN."""
    query = request.args.get("query", "").strip().lower()

    if msme_data.empty:
        return jsonify([{"error": "MSME data (csv.csv) not available on server"}]), 200

    try:
        results = msme_data[msme_data["EnterpriseName"].astype(str).str.contains(query, case=False, na=False)]
        
        output = []
        for _, row in results.head(10).iterrows():
            output.append({
                "name": row.get("EnterpriseName", "N/A"),
                "district": row.get("District", "N/A"),
                "activity": row.get("Industry", "General"), 
                "hsn_code": row.get("HSNCode", "0000")
            })
        return jsonify(output)
    except Exception as e:
        return jsonify([{"error": f"Search error: {e}"}]), 500

# ----------------------------
# 5. APPLICATION ENTRY POINT
# ----------------------------

if __name__ == "__main__":
    init_db() 
    run_ai_simulation(n=30) 
    app.run(debug=True, port=5000)