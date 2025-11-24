"""
Database module for ETL Studio user engagement features.
Handles newsletter signups and contact form submissions.
"""

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
import bcrypt

# Get database path
DB_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(DB_DIR, 'etl_studio.db')
DATABASE_URL = f'sqlite:///{DB_PATH}'

# Create engine and session
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Models
class NewsletterSignup(Base):
    """Model for email newsletter signups"""
    __tablename__ = 'newsletter_signups'
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<NewsletterSignup(email='{self.email}', created_at='{self.created_at}')>"


class ContactSubmission(Base):
    """Model for contact form submissions"""
    __tablename__ = 'contact_submissions'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False, index=True)
    message = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<ContactSubmission(name='{self.name}', email='{self.email}')>"


class User(Base):
    """Model for user accounts"""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, nullable=False, index=True)
    email = Column(String, unique=True, nullable=False, index=True)
    password_hash = Column(String, nullable=False)
    is_admin = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<User(username='{self.username}', email='{self.email}', is_admin={self.is_admin})>"


class AuthenticationLog(Base):
    """Model for authentication event logging"""
    __tablename__ = 'authentication_logs'
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=True)  # Nullable for failed logins
    username = Column(String, nullable=False, index=True)
    action = Column(String, nullable=False, index=True)  # 'login', 'logout', 'failed_login', 'signup'
    ip_address = Column(String, nullable=True)
    user_agent = Column(Text, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    
    def __repr__(self):
        return f"<AuthenticationLog(username='{self.username}', action='{self.action}', timestamp='{self.timestamp}')>"


# Database initialization
def init_db():
    """Initialize database tables"""
    Base.metadata.create_all(bind=engine)
    # Create default admin if none exists
    init_default_admin()


def init_default_admin():
    """Create default admin account if no admin exists"""
    session = SessionLocal()
    try:
        # Check if any admin exists
        admin_exists = session.query(User).filter_by(is_admin=True).first()
        if not admin_exists:
            # Create default admin
            password_hash = bcrypt.hashpw('admin123'.encode('utf-8'), bcrypt.gensalt())
            admin = User(
                username='admin',
                email='admin@etlstudio.local',
                password_hash=password_hash.decode('utf-8'),
                is_admin=True
            )
            session.add(admin)
            session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error creating default admin: {str(e)}")
    finally:
        session.close()


# API Functions
def create_signup(email: str) -> tuple[bool, str]:
    """
    Create a new newsletter signup.
    
    Args:
        email: Email address to sign up
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    session = SessionLocal()
    try:
        # Check if email already exists
        existing = session.query(NewsletterSignup).filter_by(email=email).first()
        if existing:
            return False, "This email is already subscribed!"
        
        # Create new signup
        signup = NewsletterSignup(email=email)
        session.add(signup)
        session.commit()
        return True, "Successfully subscribed to newsletter!"
    except Exception as e:
        session.rollback()
        return False, f"Error: {str(e)}"
    finally:
        session.close()


def create_contact_submission(name: str, email: str, message: str) -> tuple[bool, str]:
    """
    Create a new contact form submission.
    
    Args:
        name: Name of the person
        email: Email address
        message: Message content
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    session = SessionLocal()
    try:
        submission = ContactSubmission(name=name, email=email, message=message)
        session.add(submission)
        session.commit()
        return True, "Message sent successfully! We'll get back to you soon."
    except Exception as e:
        session.rollback()
        return False, f"Error: {str(e)}"
    finally:
        session.close()


def get_signup_count() -> int:
    """Get total number of newsletter signups"""
    session = SessionLocal()
    try:
        return session.query(NewsletterSignup).count()
    finally:
        session.close()


def get_contact_count() -> int:
    """Get total number of contact submissions"""
    session = SessionLocal()
    try:
        return session.query(ContactSubmission).count()
    finally:
        session.close()


# Authentication API Functions
def create_user(username: str, email: str, password: str, is_admin: bool = False) -> tuple[bool, str]:
    """
    Create a new user account.
    
    Args:
        username: Unique username
        email: Email address
        password: Plain text password (will be hashed)
        is_admin: Whether user has admin privileges
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    session = SessionLocal()
    try:
        # Check if username or email already exists
        existing_user = session.query(User).filter(
            (User.username == username) | (User.email == email)
        ).first()
        
        if existing_user:
            if existing_user.username == username:
                return False, "Username already exists"
            else:
                return False, "Email already registered"
        
        # Hash password
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        
        # Create user
        user = User(
            username=username,
            email=email,
            password_hash=password_hash.decode('utf-8'),
            is_admin=is_admin
        )
        session.add(user)
        session.commit()
        
        # Log the signup
        log_authentication(user.id, username, 'signup', None, None)
        
        return True, "Account created successfully!"
    except Exception as e:
        session.rollback()
        return False, f"Error: {str(e)}"
    finally:
        session.close()


def authenticate_user(username: str, password: str) -> tuple[bool, dict, str]:
    """
    Authenticate a user with username and password.
    
    Args:
        username: Username
        password: Plain text password
        
    Returns:
        Tuple of (success: bool, user_data: dict, message: str)
    """
    session = SessionLocal()
    try:
        # Find user
        user = session.query(User).filter_by(username=username).first()
        
        if not user:
            # Log failed attempt
            log_authentication(None, username, 'failed_login', None, None)
            return False, {}, "Invalid username or password"
        
        # Verify password
        if bcrypt.checkpw(password.encode('utf-8'), user.password_hash.encode('utf-8')):
            # Log successful login
            log_authentication(user.id, username, 'login', None, None)
            
            user_data = {
                'id': user.id,
                'username': user.username,
                'email': user.email,
                'is_admin': user.is_admin
            }
            return True, user_data, "Login successful!"
        else:
            # Log failed attempt
            log_authentication(None, username, 'failed_login', None, None)
            return False, {}, "Invalid username or password"
    except Exception as e:
        return False, {}, f"Error: {str(e)}"
    finally:
        session.close()


def log_authentication(user_id: int, username: str, action: str, ip_address: str = None, user_agent: str = None):
    """
    Log an authentication event.
    
    Args:
        user_id: User ID (None for failed logins)
        username: Username
        action: Action type ('login', 'logout', 'failed_login', 'signup')
        ip_address: IP address (optional)
        user_agent: User agent string (optional)
    """
    session = SessionLocal()
    try:
        log_entry = AuthenticationLog(
            user_id=user_id,
            username=username,
            action=action,
            ip_address=ip_address,
            user_agent=user_agent
        )
        session.add(log_entry)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error logging authentication: {str(e)}")
    finally:
        session.close()


def get_authentication_logs(limit: int = 100) -> list:
    """
    Get recent authentication logs.
    
    Args:
        limit: Maximum number of logs to return
        
    Returns:
        List of authentication log dictionaries
    """
    session = SessionLocal()
    try:
        logs = session.query(AuthenticationLog).order_by(
            AuthenticationLog.timestamp.desc()
        ).limit(limit).all()
        
        return [{
            'id': log.id,
            'user_id': log.user_id,
            'username': log.username,
            'action': log.action,
            'ip_address': log.ip_address,
            'user_agent': log.user_agent,
            'timestamp': log.timestamp
        } for log in logs]
    finally:
        session.close()


def get_user_by_username(username: str) -> dict:
    """
    Get user information by username.
    
    Args:
        username: Username to look up
        
    Returns:
        User data dictionary or None
    """
    session = SessionLocal()
    try:
        user = session.query(User).filter_by(username=username).first()
        if user:
            return {
                'id': user.id,
                'username': user.username,
                'email': user.email,
                'is_admin': user.is_admin,
                'created_at': user.created_at
            }
        return None
    finally:
        session.close()


def get_user_count() -> int:
    """Get total number of users"""
    session = SessionLocal()
    try:
        return session.query(User).count()
    finally:
        session.close()


def get_auth_stats() -> dict:
    """Get authentication statistics"""
    session = SessionLocal()
    try:
        total_logins = session.query(AuthenticationLog).filter_by(action='login').count()
        failed_logins = session.query(AuthenticationLog).filter_by(action='failed_login').count()
        total_users = session.query(User).count()
        
        return {
            'total_users': total_users,
            'total_logins': total_logins,
            'failed_logins': failed_logins
        }
    finally:
        session.close()


def change_password(user_id: int, old_password: str, new_password: str) -> tuple[bool, str]:
    """
    Change user password.
    
    Args:
        user_id: User ID
        old_password: Current password for verification
        new_password: New password
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    session = SessionLocal()
    try:
        # Get user
        user = session.query(User).filter_by(id=user_id).first()
        
        if not user:
            return False, "User not found"
        
        # Verify old password
        if not bcrypt.checkpw(old_password.encode('utf-8'), user.password_hash.encode('utf-8')):
            return False, "Current password is incorrect"
        
        # Validate new password
        if len(new_password) < 6:
            return False, "New password must be at least 6 characters"
        
        # Hash new password
        new_password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
        
        # Update password
        user.password_hash = new_password_hash.decode('utf-8')
        session.commit()
        
        return True, "Password changed successfully!"
    except Exception as e:
        session.rollback()
        return False, f"Error: {str(e)}"
    finally:
        session.close()
