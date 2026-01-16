# auth.py - Authentication and User Management

import hashlib 
import re
from flask import jsonify, request, session, url_for # Removed redirect
from werkzeug.routing import BuildError # Kept in case of other routing issues

# ----------------------------
# 1. PASSWORD UTILITIES
# ----------------------------

def hash_password(password: str) -> str:
    """Hash password using SHA-256"""
    return hashlib.sha256(password.encode()).hexdigest()

def validate_password(password: str) -> tuple[bool, str]:
    """Validate password strength"""
    if len(password) < 8:
        return False, "Password must be at least 8 characters long"
    if not re.search(r"[A-Za-z]", password):
        return False, "Password must contain at least one letter"
    return True, "Valid password"

def validate_email(email: str) -> bool:
    """Basic email validation"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

# ----------------------------
# 2. IN-MEMORY USER STORE
# ----------------------------

# Simulates a database - in production, use a real database
users = { 
    "test@user.com": {
        "password_hash": hash_password("testpassword"),
        "name": "Test User",
        "enterprise_name": "TestCorp MSME Garments Co.",
        "activity": "Textile & Apparel", 
        "hsn_code": "6101"
    },
    "demo@marketpulse.com": {
        "password_hash": hash_password("demo123"),
        "name": "Demo User",
        "enterprise_name": "Demo Electronics Ltd.",
        "activity": "Electronics Manufacturing", 
        "hsn_code": "8542"
    }
}

# ----------------------------
# 3. AUTHENTICATION ROUTES
# ----------------------------

def register_auth_routes(app, msme_data):
    """Register authentication API endpoints"""

    @app.route("/api/login", methods=["POST"])
    def api_login():
        """Handle user login"""
        try:
            data = request.get_json()
            email_or_phone = data.get("emailOrPhone", "").strip()
            password = data.get("password", "")

            # Validation
            if not email_or_phone or not password:
                return jsonify({"error": "Email and password are required"}), 400

            # Check user exists
            user = users.get(email_or_phone)
            if not user:
                return jsonify({"error": "Invalid email or password"}), 401

            # Verify password
            if user.get("password_hash") != hash_password(password):
                return jsonify({"error": "Invalid email or password"}), 401

            # Set session variables
            session['email'] = email_or_phone
            session['user_name'] = user['name']
            session['activity'] = user.get('activity')
            session['hsn_code'] = user.get('hsn_code')
            session['enterprise_name'] = user.get('enterprise_name')
            session.permanent = True  # Make session persistent
            
            return jsonify({
                "message": "Login successful", 
                "redirect_url": url_for('dashboard'),
                "user": {
                    "name": user['name'],
                    "activity": user.get('activity'),
                    "enterprise": user.get('enterprise_name')
                }
            }), 200

        except Exception as e:
            print(f"Login error: {e}")
            return jsonify({"error": "An error occurred during login"}), 500

    @app.route("/api/signup", methods=["POST"])
    def api_signup():
        """Handle user registration"""
        try:
            data = request.get_json()
            
            # Extract data
            email_or_phone = data.get("emailOrPhone", "").strip()
            password = data.get("password", "")
            name = data.get("name", "").strip()
            enterprise_name = data.get("enterprise_name", "").strip()
            activity = data.get("activity", "").strip()
            hsn_code = data.get("hsn_code", "").strip()

            # Validation
            if not all([enterprise_name, email_or_phone, password, activity, hsn_code]):
                return jsonify({"error": "All fields are required"}), 400

            # Validate email format
            if "@" in email_or_phone and not validate_email(email_or_phone):
                return jsonify({"error": "Invalid email format"}), 400

            # Validate password strength
            is_valid, message = validate_password(password)
            if not is_valid:
                return jsonify({"error": message}), 400

            # Check if user already exists
            if email_or_phone in users:
                return jsonify({"error": "User already exists"}), 409

            # Use email prefix as default name if not provided
            if not name:
                name = email_or_phone.split('@')[0] if '@' in email_or_phone else "User"

            # Register new user
            users[email_or_phone] = {
                "name": name,
                "enterprise_name": enterprise_name,
                "activity": activity,
                "hsn_code": hsn_code,
                "password_hash": hash_password(password)
            }

            print(f"✅ New user registered: {email_or_phone} ({enterprise_name})")

            return jsonify({
                "message": "Account created successfully! Please log in.",
                "action": "show_login"
            }), 201

        except Exception as e:
            print(f"Signup error: {e}")
            return jsonify({"error": "An error occurred during registration"}), 500

    @app.route("/api/msme-lookup", methods=["GET"])
    def msme_lookup():
        """API endpoint for searching MSME data"""
        try:
            query = request.args.get("query", "").strip().lower()

            if msme_data.empty:
                return jsonify([{
                    "name": "No Data Found", 
                    "district": "", 
                    "industry": "General MSME", 
                    "hsn": "0000"
                }]), 200

            if not query or len(query) < 2:
                return jsonify([]), 200

            # Search in EnterpriseName column
            results = msme_data[
                msme_data["EnterpriseName"].astype(str).str.contains(
                    query, case=False, na=False
                )
            ]
            
            # Format output
            output = []
            for _, row in results.head(10).iterrows():
                output.append({
                    "name": row.get("EnterpriseName", "N/A"),
                    "district": row.get("District", "N/A"),
                    "industry": row.get("Industry", "General"), 
                    "hsn": row.get("HSNCode", "0000")
                })
            
            return jsonify(output), 200

        except Exception as e:
            print(f"MSME Lookup Error: {e}")
            return jsonify([{
                "error": "An error occurred during search"
            }]), 500

    @app.route("/api/user/profile", methods=["GET"])
    def get_user_profile():
        """Get current user profile"""
        if 'email' not in session:
            return jsonify({"error": "Not authenticated"}), 401
        
        email = session.get('email')
        user = users.get(email)
        
        if not user:
            return jsonify({"error": "User not found"}), 404
        
        return jsonify({
            "email": email,
            "name": user['name'],
            "enterprise_name": user.get('enterprise_name'),
            "activity": user.get('activity'),
            "hsn_code": user.get('hsn_code')
        }), 200

    @app.route("/api/users/count", methods=["GET"])
    def get_users_count():
        """Get total registered users count (admin endpoint)"""
        return jsonify({
            "total_users": len(users),
            "message": "Total registered users"
        }), 200