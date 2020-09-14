#!/usr/bin/env python
import apsw
from flask import Flask
import json
import requests
import subprocess
import time

app = Flask(__name__)

conn = apsw.Connection("/home/brewer/lbry-sdk-0.80.0/lbryum_data/claims.db",
                       flags=apsw.SQLITE_OPEN_READONLY)
db = conn.cursor()

@app.route("/")
def hello_world():
   return "Hello World"

@app.route("/recent_channels")
def recent_channels():
    now = int(time.time())
    channels = db.execute("SELECT claim_hash FROM claim\
                           WHERE claim_type = 2 AND creation_timestamp >= ?;",
                           (now - 86400*3, )).fetchall()
    channels = [c[0] for c in channels]

    # Filter down to those whove published five streams
    channels2 = []
    for channel in channels:
        count = db.execute("SELECT COUNT(claim_hash) FROM claim\
                            WHERE channel_hash = ? AND claim_type = 1;",
                           (channel, )).fetchone()[0]
        if count >= 5:
            channels2.append(channel)

    # Now count the followers
    claim_ids = [channel[::-1].hex() for channel in channels2]
    claim_ids = ",".join(claim_ids)
    response = requests.post("https://api.lbry.com/subscription/sub_count",
                         data={"auth_token": "D18DoyrNVG6eAT1TTtzVbPqkiZoRAyPu",
                               "claim_id": claim_ids})

    if response.status_code != 200:
        return json.dumps(dict(error="Something went wrong."))


    followers = response.json()["data"]

    # Filter again
    channels3 = []
    for i in range(len(followers)):
        if followers[i] >= 20:
            channels3.append(channels2[i])

    result = []
    stuff = db.execute(f"SELECT claim_name, claim_id FROM claim\
                        WHERE claim_hash\
                            IN ({','.join('?' for _ in channels3)})",
                        channels3).fetchall()
    for row in stuff:
        result.append(dict(url="https://odysee.com/" + row[0] + ":"\
                            + row[1]))

    return tuple(result)


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

