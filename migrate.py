"""
Run once after deploy to seed users from env vars into PostgreSQL.
Usage: python3 migrate.py
Requires DATABASE_URL env var to be set.
"""
import os
from main import app, _USERS_DB, APP_PASSWORD
from models import db, User

ADMIN_USER = "zyni"

with app.app_context():
    db.create_all()
    print("Tables ensured.\n")

    for username, password in _USERS_DB.items():
        existing = User.query.filter_by(username=username).first()
        if existing:
            print(f"  Skipped  : {username} (already exists)")
            continue
        role = "admin" if username == ADMIN_USER else "user"
        u = User(username=username, role=role, status="active")
        u.set_password(password)
        db.session.add(u)
        db.session.commit()
        print(f"  Created  : {username} ({role})")

    print("\nDone.")
