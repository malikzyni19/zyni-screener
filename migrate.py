"""
Run once to seed the admin user into PostgreSQL.
Usage: python3 migrate.py
Requires DATABASE_URL env var to be set.
"""
import os
from main import app
from models import db, User

with app.app_context():
    db.create_all()
    existing = User.query.filter_by(username="zyni").first()
    if not existing:
        u = User(username="zyni", role="admin", status="active")
        u.set_password(os.environ.get("APP_PASSWORD", "Ulta8900"))
        db.session.add(u)
        db.session.commit()
        print("Admin user 'zyni' created.")
    else:
        print("Admin user 'zyni' already exists.")
