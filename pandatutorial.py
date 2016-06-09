import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import requests
import io

url="https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
s=requests.get(url).content
mo = pd.read_csv(io.StringIO(s.decode('utf-8')))



