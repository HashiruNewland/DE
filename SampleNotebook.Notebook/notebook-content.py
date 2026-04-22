# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "faa7e05c-763a-4e8c-9d35-214c69ce8636",
# META       "default_lakehouse_name": "SampleLakehouse",
# META       "default_lakehouse_workspace_id": "8d919179-54dc-4df2-8d9f-940230d115c3",
# META       "known_lakehouses": [
# META         {
# META           "id": "faa7e05c-763a-4e8c-9d35-214c69ce8636"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "a2d3df9b-8d68-bbd7-44c6-304ad80cd4c2",
# META       "known_warehouses": [
# META         {
# META           "id": "a2d3df9b-8d68-bbd7-44c6-304ad80cd4c2",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DataFrame cleaning helper
import pandas as pd
from IPython.display import display

def clean_df(df, drop_dup=True, num_strategy='median', cat_strategy='mode'):
    df = df.copy()
    if drop_dup:
        df = df.drop_duplicates(ignore_index=True)
    # numeric columns
    num_cols = df.select_dtypes(include='number').columns
    for c in num_cols:
        if num_strategy == 'median':
            val = df[c].median()
        elif num_strategy == 'mean':
            val = df[c].mean()
        else:
            val = num_strategy
        df[c].fillna(val, inplace=True)
    # categorical columns
    cat_cols = df.select_dtypes(include=['object','category']).columns
    for c in cat_cols:
        if cat_strategy == 'mode':
            m = df[c].mode(dropna=True)
            fill = m[0] if not m.empty else ""
        else:
            fill = cat_strategy
        df[c].fillna(fill, inplace=True)
    return df

# Auto-detect pandas DataFrame in globals and clean it
candidates = [name for name, val in globals().items() if isinstance(val, pd.DataFrame)]
if not candidates:
    print("No pandas DataFrame found in globals. Load or create a DataFrame and re-run.")
    print("If you have a DataFrame variable, set it like: df = your_dataframe")
else:
    df_name = candidates[0]
    if len(candidates) > 1:
        print("Multiple DataFrames found, using the first:", candidates)
    df = globals()[df_name]
    print("Detected DataFrame:", df_name, "shape:", df.shape)
    cleaned = clean_df(df)
    cleaned_name = f"{df_name}_clean"
    globals()[cleaned_name] = cleaned
    print("Cleaned DataFrame saved to variable:", cleaned_name)
    display(cleaned.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
