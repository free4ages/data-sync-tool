import os
import sys

# Add the root directory of the project to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

os.environ["POSTGRES_HOST"] = os.environ.get('POSTGRES_HOST', 'localhost')
os.environ["POSTGRES_PORT"] = os.environ.get('POSTGRES_PORT', '5436')
os.environ["POSTGRES_USER"] = os.environ.get('POSTGRES_USER', 'synctool')
os.environ["POSTGRES_PASSWORD"] = os.environ.get('POSTGRES_PASSWORD', 'synctool')
os.environ["POSTGRES_DB"] = os.environ.get('POSTGRES_DB', 'synctool')
