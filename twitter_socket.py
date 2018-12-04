import os
import sys
import socket
import time
import json

from signal import signal, SIGPIPE, SIG_DFL

from settings import CONSUMER_API_KEY, CONSUMER_API_SECRET, ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET, SOCKET_HOST, SOCKET_PORT

from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener

# Stop program if sigpipe detected
signal(SIGPIPE, SIG_DFL)


class TwitterStreamListener(StreamListener):
    def __init__(self, conn):
        super().__init__()
        self.conn = conn

    def on_data(self, _data):
        try:
            data = _data.replace(r'\n', '')
            all_data = json.loads(data)
            if(("text" in all_data) and ("retweeted_status" not in all_data)):
                username = all_data["user"]["screen_name"]
                print((username, all_data["text"]))
                self.conn.send(data.encode())
        except BaseException as e:
            print("Error on_data: %s" % str(e))

    def on_error(self, status):
        if status == 420:
            print("Stream Disconnected")
            return False


class Twitter():
    def __init__(self, conn):
        self.auth = OAuthHandler(CONSUMER_API_KEY, CONSUMER_API_SECRET)
        self.auth.set_access_token(ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET)
        self.stream = Stream(
            self.auth, TwitterStreamListener(conn))

    def filter(self, array_of_track=[]):
        self.stream.filter(track=array_of_track)

    def sample(self):
        self.stream.sample()


if __name__ == "__main__":
    try:
        s = socket.socket()
        s.bind((SOCKET_HOST, SOCKET_PORT))
        print("Listening on port : ", str(SOCKET_PORT))

        s.listen(5)
        conn, addr = s.accept()
        print("Received request from:", str(addr))

        twitter = Twitter(conn)
        twitter.filter(["jokowi", "prabowo"])
        twitter.sample()
    except KeyboardInterrupt:
        print()
        print("Bye bye :(")
        sys.exit()
