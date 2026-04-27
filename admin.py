import re
from flask import Blueprint, render_template, redirect, url_for, request, session, flash, jsonify
from flask_login import login_user, logout_user, current_user
from functools import wraps
from datetime import datetime, timezone

from models import db, User, AdminLog

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")

_USERNAME_RE = re.compile(r'^[a-zA-Z0-9_]{3,30}$')


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            return redirect(url_for("admin.login"))
        return f(*args, **kwargs)
    return decorated


def _get_ip():
    return request.headers.get("X-Forwarded-For", request.remote_addr or "unknown").split(",")[0].strip()


def _log_action(action: str, details: str = None, target_user_id: int = None):
    try:
        entry = AdminLog(
            admin_id=current_user.id,
            action=action,
            target_user_id=target_user_id,
            details=details,
            ip_address=_get_ip(),
        )
        db.session.add(entry)
        db.session.commit()
    except Exception as e:
        print(f"[ADMIN-LOG] Failed to log action: {e}")


def _admin_count():
    try:
        return User.query.filter_by(role="admin", status="active").count()
    except Exception:
        return 1


# ── Login ──────────────────────────────────────────────────────────
@admin_bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated and current_user.is_admin:
        return redirect(url_for("admin.dashboard"))

    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")

        user = User.query.filter_by(username=username).first()

        if not user or not user.check_password(password):
            error = "Invalid username or password."
        elif not user.is_admin:
            error = "Access denied — admin only."
        elif user.status != "active":
            error = f"Account is {user.status}. Contact support."
        else:
            login_user(user, remember=False)
            session["is_admin"] = True

            user.last_login_at = datetime.now(timezone.utc)
            user.last_login_ip = _get_ip()
            db.session.commit()

            return redirect(url_for("admin.dashboard"))

    return render_template("admin/login.html", error=error)


# ── Logout ─────────────────────────────────────────────────────────
@admin_bp.route("/logout")
def logout():
    if current_user.is_authenticated:
        _log_action("logout")
    logout_user()
    session.pop("is_admin", None)
    return redirect(url_for("admin.login"))


# ── Dashboard ──────────────────────────────────────────────────────
@admin_bp.route("/")
@admin_required
def dashboard():
    try:
        total_users  = User.query.filter(User.role != "admin").count()
        active_users = User.query.filter_by(status="active").filter(User.role != "admin").count()
        paused_users = User.query.filter_by(status="paused").count()
        admin_count  = User.query.filter_by(role="admin").count()

        today = datetime.now(timezone.utc).date()
        logs_today = AdminLog.query.filter(
            db.func.date(AdminLog.created_at) == today
        ).count()

        recent_logs = (
            AdminLog.query
            .order_by(AdminLog.created_at.desc())
            .limit(10)
            .all()
        )

        recent_users = (
            User.query
            .order_by(User.created_at.desc())
            .limit(5)
            .all()
        )

        db_connected = True
    except Exception as e:
        print(f"[ADMIN-DASH] DB error: {e}")
        total_users = active_users = paused_users = admin_count = logs_today = 0
        recent_logs = []
        recent_users = []
        db_connected = False

    return render_template(
        "admin/dashboard.html",
        total_users=total_users,
        active_users=active_users,
        paused_users=paused_users,
        admin_count=admin_count,
        logs_today=logs_today,
        recent_logs=recent_logs,
        recent_users=recent_users,
        db_connected=db_connected,
        server_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        app_version="1.0.0",
    )


# ── Users List ─────────────────────────────────────────────────────
@admin_bp.route("/users")
@admin_required
def users():
    try:
        status_filter = request.args.get("status")
        role_filter   = request.args.get("role")

        q = User.query.order_by(User.created_at.desc())
        if status_filter:
            q = q.filter_by(status=status_filter)
        if role_filter:
            q = q.filter_by(role=role_filter)

        all_users   = q.all()
        total_count = User.query.count()
    except Exception as e:
        print(f"[ADMIN-USERS] DB error: {e}")
        all_users   = []
        total_count = 0

    return render_template(
        "admin/users.html",
        users=all_users,
        total_count=total_count,
        status_filter=status_filter,
        role_filter=role_filter,
    )


# ── Create User ────────────────────────────────────────────────────
@admin_bp.route("/users/create", methods=["GET", "POST"])
@admin_required
def users_create():
    errors = {}

    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")
        confirm  = request.form.get("confirm_password", "")
        role     = request.form.get("role", "user")
        status   = request.form.get("status", "active")
        notes    = request.form.get("notes", "").strip()

        if not username:
            errors["username"] = "Username is required."
        elif not _USERNAME_RE.match(username):
            errors["username"] = "3–30 chars: letters, numbers, underscore only."
        elif User.query.filter_by(username=username).first():
            errors["username"] = f"Username '{username}' is already taken."

        if not password:
            errors["password"] = "Password is required."
        elif len(password) < 8:
            errors["password"] = "Password must be at least 8 characters."

        if not confirm:
            errors["confirm_password"] = "Please confirm the password."
        elif password and password != confirm:
            errors["confirm_password"] = "Passwords do not match."

        if role not in ("user", "admin", "guest"):
            role = "user"
        if status not in ("active", "paused"):
            status = "active"

        if not errors:
            try:
                new_user = User(
                    username=username,
                    role=role,
                    status=status,
                    notes=notes or None,
                )
                new_user.set_password(password)
                db.session.add(new_user)
                db.session.commit()
                _log_action("create_user", f"{username} ({role})", target_user_id=new_user.id)
                flash(f"User '{username}' created successfully.", "success")
                return redirect(url_for("admin.users"))
            except Exception as e:
                db.session.rollback()
                errors["_general"] = f"Database error: {e}"

    return render_template("admin/users/create.html", errors=errors,
                           form=request.form if request.method == "POST" else {})


# ── Edit User ──────────────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/edit", methods=["GET", "POST"])
@admin_required
def users_edit(user_id):
    user = User.query.get_or_404(user_id)
    errors = {}
    changed = []

    if request.method == "POST":
        new_username = request.form.get("username", "").strip().lower()
        new_role     = request.form.get("role", user.role)
        new_status   = request.form.get("status", user.status)
        new_notes    = request.form.get("notes", "").strip()
        new_password = request.form.get("new_password", "")
        confirm_pwd  = request.form.get("confirm_password", "")

        if not new_username:
            errors["username"] = "Username is required."
        elif not _USERNAME_RE.match(new_username):
            errors["username"] = "3–30 chars: letters, numbers, underscore only."
        elif new_username != user.username and User.query.filter_by(username=new_username).first():
            errors["username"] = f"Username '{new_username}' is already taken."

        if new_password:
            if len(new_password) < 8:
                errors["new_password"] = "Password must be at least 8 characters."
            elif new_password != confirm_pwd:
                errors["confirm_password"] = "Passwords do not match."

        if new_role not in ("user", "admin", "guest"):
            new_role = user.role
        if new_status not in ("active", "paused", "banned"):
            new_status = user.status

        # Guard: cannot demote last admin
        if user.role == "admin" and new_role != "admin" and _admin_count() <= 1:
            errors["role"] = "Cannot change role — this is the last active admin."

        if not errors:
            try:
                if new_username != user.username:
                    changed.append(f"username: {user.username}→{new_username}")
                    user.username = new_username
                if new_role != user.role:
                    changed.append(f"role: {user.role}→{new_role}")
                    user.role = new_role
                if new_status != user.status:
                    changed.append(f"status: {user.status}→{new_status}")
                    user.status = new_status
                if new_notes != (user.notes or ""):
                    user.notes = new_notes or None
                if new_password:
                    user.set_password(new_password)
                    changed.append("password updated")

                db.session.commit()
                detail = ", ".join(changed) if changed else "no changes"
                _log_action("edit_user", detail, target_user_id=user.id)
                flash(f"User '{user.username}' updated.", "success")
                return redirect(url_for("admin.users"))
            except Exception as e:
                db.session.rollback()
                errors["_general"] = f"Database error: {e}"

    return render_template("admin/users/edit.html", user=user, errors=errors)


# ── Delete User ────────────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/delete", methods=["POST"])
@admin_required
def users_delete(user_id):
    user = User.query.get_or_404(user_id)

    if user.id == current_user.id:
        flash("You cannot delete your own account.", "error")
        return redirect(url_for("admin.users"))

    if user.role == "admin" and _admin_count() <= 1:
        flash("Cannot delete the last admin account.", "error")
        return redirect(url_for("admin.users"))

    confirm = request.form.get("confirm_username", "").strip().lower()
    if confirm != user.username:
        flash("Confirmation username did not match. User not deleted.", "error")
        return redirect(url_for("admin.users_edit", user_id=user_id))

    try:
        uname = user.username
        urole = user.role
        _log_action("delete_user", f"{uname} ({urole})", target_user_id=user.id)
        db.session.delete(user)
        db.session.commit()
        flash(f"User '{uname}' deleted.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Delete failed: {e}", "error")

    return redirect(url_for("admin.users"))


# ── Toggle Status ──────────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/toggle-status", methods=["POST"])
@admin_required
def users_toggle_status(user_id):
    user = User.query.get_or_404(user_id)

    if user.id == current_user.id:
        return jsonify({"error": "Cannot change your own status."}), 400

    if user.role == "admin" and user.status == "active" and _admin_count() <= 1:
        return jsonify({"error": "Cannot pause the last active admin."}), 400

    try:
        new_status = "paused" if user.status == "active" else "active"
        user.status = new_status
        db.session.commit()
        action = "pause_user" if new_status == "paused" else "unpause_user"
        _log_action(action, user.username, target_user_id=user.id)
        return jsonify({"success": True, "status": new_status})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# ── Reset Password ─────────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/reset-password", methods=["POST"])
@admin_required
def users_reset_password(user_id):
    user = User.query.get_or_404(user_id)
    data = request.get_json(force=True) or {}

    new_password = data.get("new_password", "")
    confirm      = data.get("confirm_password", "")

    if not new_password:
        return jsonify({"error": "New password is required."}), 400
    if len(new_password) < 8:
        return jsonify({"error": "Password must be at least 8 characters."}), 400
    if new_password != confirm:
        return jsonify({"error": "Passwords do not match."}), 400

    try:
        user.set_password(new_password)
        db.session.commit()
        _log_action("reset_password", f"Admin reset password for {user.username}", target_user_id=user.id)
        return jsonify({"success": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500
