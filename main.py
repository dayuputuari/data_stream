import os
import socket
import time
import json

from signal import signal, SIGPIPE, SIG_DFL

from settings import CONSUMER_API_KEY, CONSUMER_API_SECRET, ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET, SOCKET_HOST, SOCKET_PORT

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

# Stop program if sigpipe detected
signal(SIGPIPE, SIG_DFL)


class TwitterStreamListener(StreamListener):
    def __init__(self, conn):
        super().__init__()
        self.conn = conn

    def on_data(self, data):
        try:
            all_data = json.loads(data)
            tweet = all_data["text"]
            if(tweet):
                username = all_data["user"]["screen_name"]
                print((username, tweet))
                self.conn.send(data.encode())

            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))

    def on_error(self, status):
        print(status)
        if status == 420:
            print("Stream Disconnected")
            return False


class Twitter():
    def __init__(self, conn):
        self.auth = OAuthHandler(CONSUMER_API_KEY, CONSUMER_API_SECRET)
        self.auth.set_access_token(ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET)
        self.stream = Stream(
            auth=self.auth, listener=TwitterStreamListener(conn))

    def filter(self, array_of_track=[]):
        self.stream.filter(track=array_of_track)

    def send_data(self):
        self.stream.sample()


if __name__ == "__main__":
    s = socket.socket()
    s.bind((SOCKET_HOST, SOCKET_PORT))
    print("Listening on port:", str(SOCKET_PORT))

    s.listen(5)
    conn, addr = s.accept()
    print("Received request from:", str(addr))

    twitter = Twitter(conn)
    twitter.filter(["jokowi"])
    twitter.send_data()
