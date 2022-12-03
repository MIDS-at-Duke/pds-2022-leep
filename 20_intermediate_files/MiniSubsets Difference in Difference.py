import pandas as pd
import datetime as dt
import numpy as np



# DATES - TRANSACTION_DATE_DT column

def postprocessing(df_from_your_subset):
    
    df_from_your_subset["TRANSACTION_DATE_DT"] = pd.to_datetime(
    df_from_your_subset["TRANSACTION_DATE_DT"])

    return df_from_your_subset

# or this one, have not tested yet
def postpostprocessing2(df_from_your_subset):

    df_from_your_subset["TRANSACTION_DATE_DT"] = pd.to_datetime(
    df_from_your_subset["TRANSACTION_DATE_DT"], format = %Y-%m-%d)

    return df_from_your_subset

 
 ################################################### 

#  functions for formatting the columns and grouping the datasets

 ####################################################

