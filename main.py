import apsw
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time

conn = apsw.Connection("/home/brewer/lbry-sdk-0.80.0/lbryum_data/claims.db",
                       flags=apsw.SQLITE_OPEN_READONLY)
db = conn.cursor()

# Claim IDs from https://github.com/lbryio/lbry-desktop/commit/d920e412a142eb6fa94df7be42cb313b198a3409#diff-749c271e5a1ef01df27ea3a58cd4f269R819
channels = ['47c6a778ea4836b0987ccc4ce27b26d5f886ec1d',
  '1bd0adfcf1c75bafc1ba3fc9b65a1a0470df6a91',
  '7e1a7afadc8734b33a3e219f5668470715fb063d',
  'fb364ef587872515f545a5b4b3182b58073f230f',
  'c5cd9b63e2ba0abc191feae48238f464baecb147',
  'ba79c80788a9e1751e49ad401f5692d86f73a2db',
  '589276465a23c589801d874f484cc39f307d7ec7',
  'fddb293b297417d753d0175be69a11e59b22ad57',
  '5fbfcf517d3df749bd032a44c1946b2baa738ecb',
  '43fa9aec16c55f537e395f27b7185c0d812dd89a',
  '35045f3a701c49be2d0494f08f90943f3d7c2edb',
  '55f552c153a6922798ce9f6d429b69c87c4c992a',
  '87b2669c65c60a36aa408f0177517a192db194a7',
  'b79c5dee207f6429d677d65999aed96449e02092',
  '5fc52291980268b82413ca4c0ace1b8d749f3ffb',
  '07e4546674268fc0222b2ca22d31d0549dc217ee',
  '545c86494bd5f7d9aeade8e58342e2a7ecf2f803',
  'de0fcd76d525b1db36f24523e75c28b542e92fa2',
  '74333143a3dcc001a5602aa524583fc75a013d75',
  '4967034d0978c239b6e0bd4b1fea04d918c13a10',
  'f54cc6a6a214ea183db11c47d7f5dc464e5bc9ef',
  '4dbbe5e945ced9756327160b78c807007c2e9d72',
  'a52425228572850f40651d2f8fe965a7d1f7d003',
  'fa5b58aeba19dee98eb5c78cbc8c1d30f99acb8b',
  '0d4e104ffc0ff0a6c8701e67cf13760f4e0335a8',
  '6e83f36dfc16e44d8f48cd27d698ca49a6cd1402',
  'ef5eba855aabebe9292bfc10fa9b0884337ab52c',
  'c3cf3780c73279f8a20b764f2c7edea1cf380055',
  '0e8d245734aab8c6825b6529a47d4cacfb3a53a8',
  '918be99daff84a69e1458cfabfda219f2a05271b',
  'f82b1ddf6353454b9ba07d8c9cd758e9d4a6c148',
  '46948b772a8d4eadeedbf2eadf0f6f37332cd7b8',
  'd0b97ba2a5eb024f4cc1d972b1c52896f37c32ed',
  'd4cf14c542ade693a79de689d24ec29ad73aee93',
  '1feef57a100df13c84b2c03e2683498287e6781a',
  '4fd4b60a7f00778ebbd150029164302fe84b7e56',
  'bd70f930e75b5886c3b66ace2d0ca31262d43a6f',
  'fe938863cb867c4e369d270bcfb062bb8281db2d',
  '9826a0c0a781ce1beed3067202f8677a2740e3ba',
  'dd42c26d24c17ea326df2e4e4cb1f0e243f878a7',
  '5327b233d7128e8d806266f2f486a4e48ed325c1',
  'b9288432bd089c6f332145aab08a56eec155f307',
  '26c9b54d7e47dc8f7dc847821b26fce3009ee1a0']

# Claim hashes
channels = [bytes.fromhex(channel)[::-1] for channel in channels]

mix_query = f"""
SELECT * FROM
    (SELECT claim_hash, claim_id, claim_name,
               release_time, effective_amount FROM claim
               WHERE claim_type = 1 AND channel_hash IN
                    ({','.join('?' for _ in channels)})
               ORDER BY release_time DESC LIMIT 20)
UNION
SELECT * FROM
    (SELECT claim_hash, claim_id, claim_name,
               release_time, effective_amount FROM claim
               WHERE claim_type = 1 AND channel_hash IN
                    ({','.join('?' for _ in channels)})
               ORDER BY effective_amount DESC LIMIT 5)
UNION
SELECT * FROM
    (SELECT claim_hash, claim_id, claim_name,
               release_time, effective_amount FROM claim
               WHERE claim_type = 1 AND channel_hash IN
                    ({','.join('?' for _ in channels)})
               ORDER BY trending_group, trending_mixed DESC LIMIT 5)
ORDER BY release_time DESC;
"""

result = pd.DataFrame(db.execute(mix_query, channels + channels + channels))

print(result)
