import pandas as pd
import numpy as np
from datetime import datetime, timedelta

sop = pd.read_excel('MLF_SERIAL_OPER1.xlsx')
sid_list = np.unique(sop['SERIAL_ID'])
sid_count = len(sid_list)
disc_sid = []

for i in int(range(sid_count) * 0.2):
    disc_sid.append( sid_list[int(np.round(np.random.rand()*sid_count))] )