# app.py - MarketPulse Main Application

import os
import random
from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from flask_cors import CORS

# Import logic from separated modules
import db_sim
import auth 

# ----------------------------
# 1. SETUP & CONFIGURATION
# ----------------------------
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(24).hex())
CORS(app, supports_credentials=True)

# Register authentication and API routes
auth.register_auth_routes(app, db_sim.get_msme_lookup_data())

# ----------------------------
# 2. FRONTEND ROUTES 
# ----------------------------

@app.route("/")
def landing_page():
    """Landing page route - shows the marketing page"""
    return render_template("landing page.html")

@app.route("/login")
def show_login():
    """Display login/signup page"""
    return render_template(
        "login.html", 
        error_message=session.pop('error_message', None),
        signup_success_message=session.pop('signup_success_message', None)
    )

@app.route('/dashboard')
def dashboard():
    """Main dashboard view - requires authentication"""
    if 'email' not in session:
        session['error_message'] = "You must log in to view the dashboard."
        return redirect(url_for('show_login'))
    
    user_name = session.get('user_name', 'MSME User')
    user_activity = session.get('activity')
    
    # Fetch and rank data based on user's activity
    ranking_data_df = db_sim.fetch_and_rank_data(user_activity)
    
    # Calculate KPIs
    avg_index = ranking_data_df['index'].mean() if not ranking_data_df.empty else 0.0
    total_records = len(ranking_data_df)
    
    # Prepare data for dashboard template
    ranking_data = ranking_data_df.head(10).to_dict('records')
    
    # Trend Analysis: calculate average trend score per activity
    trend_analysis = {}
    if not ranking_data_df.empty:
        trend_analysis = ranking_data_df.groupby('activity')['trend_score'].mean().sort_values(ascending=False).to_dict()
    
    # User data for display
    user_data = {
        'name': user_name,
        'activity': user_activity if user_activity else 'General MSME',
        'hsn': session.get('hsn_code', 'N/A'),
        'enterprise_name': session.get('enterprise_name', 'Your Enterprise'),
    }

    return render_template(
        'dashboard.html', 
        user=user_data['name'],
        user_data=user_data,                 
        avg_index=f"{avg_index:.2f}",
        total_records=total_records,
        ranking_data=ranking_data,           
        trend_analysis=trend_analysis,       
    )

@app.route('/msme-data')
def msme_data_view():
    """MSME Data Intelligence Center view"""
    if 'email' not in session:
        session['error_message'] = "You must log in to view this page."
        return redirect(url_for('show_login'))
    
    return render_template('msme .html')

@app.route('/generate_trend', methods=['POST'])
def generate_trend():
    """Generate new market trend insights"""
    if 'email' not in session:
        return redirect(url_for('show_login'))
    
    user_activity = session.get('activity')
    user_hsn = session.get('hsn_code')
    
    # Generate new insights
    db_sim.run_ai_simulation(
        n=random.randint(15, 25), 
        user_activity=user_activity, 
        user_hsn=user_hsn
    )
    
    return redirect(url_for('dashboard'))

@app.route('/logout')
def logout():
    """Clear session and logout user"""
    session.clear()
    session['error_message'] = "You have been logged out successfully."
    return redirect(url_for('show_login'))

# ----------------------------
# 3. ERROR HANDLERS
# ----------------------------

@app.errorhandler(404)
def not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Internal server error"}), 500

# ----------------------------
# 4. APPLICATION STARTUP
# ----------------------------

if __name__ == '__main__':
    print("🚀 Starting MarketPulse Application...")
    
    # Initialize the database
    db_sim.init_db()
    
    # Run initial simulation to populate data
    print("📊 Generating initial market insights...")
    db_sim.run_ai_simulation(n=50)
    
    print("✅ MarketPulse is ready!")
    print("🌐 Access the application at: http://localhost:8000")
    
    # Run the Flask development server
    app.run(
        debug=True, 
        port=8000,
        host='0.0.0.0'  # Allow external connections
    )