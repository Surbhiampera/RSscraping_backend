"""Quick script to promote an existing user to ADMIN role.

Usage: python -m backend.promote_admin <email>
"""
# import sys
# from backend.db.session import SessionLocal
# from backend.db.models import User

# def main():
#     if len(sys.argv) < 2:
#         print("Usage: python -m backend.promote_admin <email>")
#         sys.exit(1)

#     email = sys.argv[1]
#     db = SessionLocal()
#     try:
#         user = db.query(User).filter(User.email == email).first()
#         if not user:
#             print(f"No user found with email: {email}")
#             sys.exit(1)
#         user.role = "ADMIN"
#         db.commit()
#         print(f"✅ User '{user.name}' ({email}) promoted to ADMIN")
#     finally:
#         db.close()

# if __name__ == "__main__":
#     main()
