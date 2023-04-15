import os.path
import sys, json
import requests
import subprocess
import numpy as np
import pandas as pd
import plotly.express as px
from requests.exceptions import RequestException
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from collections import namedtuple
Response = namedtuple('Response', ['url', 'error'])
def get_tunnel():
    try:
        Tunnel = subprocess.Popen(['./notebooks/ngrok','http','8050'])
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        res = session.get('http://localhost:4040/api/tunnels')
        res.raise_for_status()
        tunnel_str = res.text
        tunnel_cfg = json.loads(tunnel_str)
        tunnel_url = tunnel_cfg['tunnels'][0]['public_url']
        return Response(url=tunnel_url, error=None)
    except RequestException as e:
        return Response(url=None, error=str(e))
tunnel = get_tunnel()
print(tunnel)