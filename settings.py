from dotenv import load_dotenv
import os

load_dotenv(verbose=True)

CONSUMER_API_KEY = os.getenv("CONSUMER_API")
CONSUMER_API_SECRET = os.getenv("CONSUMER_API_SECRET")
ACCESS_TOKEN_KEY = os.getenv("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET")
SOCKET_HOST = os.getenv("SOCKET_HOST")
SOCKET_PORT = int(os.getenv("SOCKET_PORT"))
