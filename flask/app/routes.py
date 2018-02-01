import psycopg2
import json
import configparser

from app import app
from flask import jsonify
from flask import render_template
from pymongo import MongoClient

@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/posts')
def post():
    return render_template('posts.html')

# read config from file
config = configparser.ConfigParser()
config.read('app.conf')

# PostgreSQL database connection info
hostname = config['postgres']['hostname']
username = config['postgres']['username']
password = config['postgres']['password']
database = config['postgres']['database']
port = config['postgres']['port']
print hostname
print username
myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)

# MongoDB connection info
uri = config['mongo']['uri']
print uri
client = MongoClient(uri)
db = client.get_database()
posts = db.recommendedPost

@app.route('/api/<user>')
def get_posts(user):
    # final dict to be sent over
    res = {}

    # get the recommended posts from MongoDB
    mongoCur = posts.find({"_id": user})
    recPosts = []
    for i in range(5):
        recPosts.append(mongoCur[0]['posts'][i])

    # get post information from Posts table at PostgreSQL
    cur = myConnection.cursor()
    for i in range(5):
        p = {}
        stmt = "SELECT * FROM posts WHERE id=%s"
        cur.execute(stmt, [recPosts[i]['_1']])
        row = cur.fetchone()
        p['id'] = row[0]
        p['title'] = row[2]
        p['body'] = row[3]
        p['post_date'] = row[4]
        p['owner_user_id'] = row[5]
        p['score'] = row[7]
        p['favorite_count'] = row[10]
        p['view_count'] = row[11]
        p['connection'] = recPosts[i]['_2']
        res[i] = p

    # get user information from the Users table
    stmt = "SELECT * FROM users WHERE id=%s"
    cur.execute(stmt, [user])
    row = cur.fetchone()
    u = {}
    u['id'] = user
    u['name'] = row[1]
    u['location'] = row[4]
    u['reputation'] = row[5]
    u['upVotes'] = row[6]
    u['downVotes'] = row[7]
    u['views'] = row[8]
    res['user'] = u

    return json.dumps(res)
