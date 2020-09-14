#!/usr/bin/env python
import apsw
from collections import OrderedDict
from flask import Flask
import json
import requests
import subprocess
import time

app = Flask(__name__)

conn = apsw.Connection("/home/brewer/lbry-sdk-0.80.0/lbryum_data/claims.db",
                       flags=apsw.SQLITE_OPEN_READONLY)
conn.setbusytimeout(60000)
db = conn.cursor()

@app.route("/")
def hello_world():
   return "Hello World"

@app.route("/recent_channels")
def recent_channels():
    now = int(time.time())

    channels = OrderedDict()
    for row in db.execute("SELECT claim_id, claim_name,\
                           (? - creation_timestamp)/86400.0 age_days\
                           FROM claim\
                           WHERE claim_type = 2 AND age_days <= 60.0;",
                           (now, )):
        claim_id, claim_name, age_days = row
        channels[claim_id] = dict(claim_name=claim_name, age_days=age_days)

    claim_hashes = [bytes.fromhex(key)[::-1] for key in channels]

    # Filter down to those who have published two streams per week
    channels2 = OrderedDict()
    for row in db.execute(f"SELECT channels.claim_id, COUNT(*) num FROM claim streams\
                                     INNER JOIN claim channels ON\
                                     channels.claim_hash = streams.channel_hash\
                            WHERE channels.claim_hash IN\
                            ({','.join('?' for _ in claim_hashes)})\
                            AND streams.claim_type = 1\
                            GROUP BY channels.claim_hash\
                            HAVING num >= 2;",
                            claim_hashes):
        claim_id, publications = row
        if row[1] >= (2/7)*channels[claim_id]["age_days"]:
            channels2[claim_id] = channels[claim_id]
            channels2[claim_id]["publications"] = publications
    channels = channels2

    # Now count the followers
    claim_ids = ",".join(list(channels.keys()))
    response = requests.post("https://api.lbry.com/subscription/sub_count",
                         data={"auth_token": "D18DoyrNVG6eAT1TTtzVbPqkiZoRAyPu",
                               "claim_id": claim_ids})
    if response.status_code != 200:
        return json.dumps(dict(error="Something went wrong."))

    followers = response.json()["data"]

    # Filter again - one follower per day
    channels2 = OrderedDict()
    i = 0
    for key in channels:
        if followers[i] >= 1.0*channels[key]["age_days"] and followers[i] >= 10:
            channels2[key] = channels[key]
            channels2[key]["followers"] = followers[i]
        i += 1
    channels = channels2

    # Convert to list
    channels2 = []
    for key in channels:
        chan = channels[key]
        channels2.append({"url": "https://odysee.com/" + chan["claim_name"]
                            + ":" + key,
                          "claim_name": chan["claim_name"],
                          "age_days": chan["age_days"],
                          "followers": chan["followers"]})
    channels = channels2
    channels = sorted(channels, key = lambda chan: chan["age_days"])

    html = """
            <html><head><title>Hi</title></head><body>
                <h1>Channels created in the last 60 days</h1>\n
                Fulfilling these criteria:
                <ul>
                    <li>At least two publications per week</li>
                    <li>At least one follower per day</li>
                    <li>At least two publications and ten followers overall</li>
                </ul>\n"""
    for channel in channels:
        print(channel)
        html += f"<a href=\"{channel['url']}\" target=\"_blank\" rel=\"noopener noreferrer\">Channel\
                  {channel['claim_name']} with {channel['followers']} followers,\
                  created {channel['age_days']:.3f} days ago.</a><br>\n"

    return html + "</body></html>"


@app.route("/frontpage/<claim_ids>/<page_size>")
def get(claim_ids, page_size):
    page_size = int(page_size)
    if page_size > 100:
        return json.dumps(dict(error="Page size is too big."))

    # Remove commas
    channels = claim_ids.split(",")
    channels = [bytes.fromhex(channel)[::-1] for channel in channels]

    mix_query = f"""
    SELECT * FROM
        (SELECT claim_id, release_time FROM claim
                   WHERE channel_hash IN
                        ({','.join('?' for _ in channels)})
                   ORDER BY release_time DESC LIMIT {page_size})
    UNION
    SELECT * FROM
        (SELECT claim_id, release_time FROM claim
                   WHERE channel_hash IN
                        ({','.join('?' for _ in channels)})
                   ORDER BY trending_group, trending_mixed DESC LIMIT {page_size})
    ORDER BY release_time DESC LIMIT {page_size};
    """

    result = db.execute(mix_query, channels + channels).fetchall()
    result = [r[0] for r in result]

    return json.dumps(result)




if __name__ == "__main__":
    app.run(host="0.0.0.0", port="5001")

