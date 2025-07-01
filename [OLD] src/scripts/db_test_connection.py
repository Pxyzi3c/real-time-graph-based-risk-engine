import os
import pandas as pd
import sys

script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
sys.path.append(project_root)

from src.common.db import get_engine
import pandas as pd

engine = get_engine()
df = pd.read_sql("SELECT * FROM credit_card_fraud", con=engine)
print(df)