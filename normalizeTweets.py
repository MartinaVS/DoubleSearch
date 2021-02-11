from pymongo import MongoClient
import urllib.parse
import re

credPath = 'credentialsMongo.txt'
credFile = open(credPath, 'r')
loginMA = urllib.parse.quote_plus(credFile.readline().strip())
passwordMA = urllib.parse.quote_plus(credFile.readline()).strip()
client = MongoClient("mongodb+srv://" +
                     loginMA + ":" +
                     passwordMA +
                     "@doublesearchintwitter-m3qge.mongodb.net")
db = client.twitter_database
tweets = db.tweets

allTweets = tweets.find().sort("text", 1)
keepAllTweets = []
for doc in allTweets:
    text = doc['text'].replace("\n", " ")
    text = re.sub("@\w+", "[user]", text)
    text = re.sub("http[^ \n]+", "[link]", text)
    text = text.strip()
    if text:
        keepAllTweets.append(text.lower())
with open('normalizedTweets.txt', 'w') as file:
    for tweet in keepAllTweets:
        file.write(tweet + '\n')
