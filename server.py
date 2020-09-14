#!/usr/bin/env python
import apsw
from flask import Flask
import json
import subprocess
import time

app = Flask(__name__)

conn = apsw.Connection("/home/brewer/lbry-sdk-0.80.0/lbryum_data/claims.db",
                       flags=apsw.SQLITE_OPEN_READONLY)
db = conn.cursor()

@app.route("/")
def hello_world():
   return "Hello World"

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

