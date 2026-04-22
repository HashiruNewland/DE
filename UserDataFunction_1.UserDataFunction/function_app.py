import datetime
import fabric.functions as fn
import logging

udf = fn.UserDataFunctions()

@udf.function()
def hello_fabric(name: str) -> str:
    logging.info('Python UDF trigger function processed a request.')

    return f"Welcome to Fabric Functions, {name}, at {datetime.datetime.now()}!"


import pandas as pd 

@udf.function()
def manipulate_data(data: list) -> list:
    '''
    Description: Manipulate data using pandas to group by age categories and calculate mean ages.

    Args:
    - data (list): List of dictionaries containing Name, Age, and Gender fields
      Example: [{"Name": "John", "Age": 22, "Gender": "male"}, {"Name": "Jane", "Age": 17, "Gender": "female"}]

    Returns: list: JSON records with AgeGroup (Adult/Minor) and mean Age values
    '''
    # Convert the data dictionary to a DataFrame
    df = pd.DataFrame(data)
        # Perform basic data manipulation
    # Example: Add a new column 'AgeGroup' based on the 'Age' column    
    df['AgeGroup'] = df['Age'].apply(lambda x: 'Adult' if x >= 18 else 'Minor')
    
    # Example: Filter rows where 'Age' is greater than 30
    # df_filtered = df[df["Age"] > 30]

    # Example: Group by 'AgeGroup' and calculate the mean age
    df_grouped = df.groupby("AgeGroup")["Age"].mean().reset_index()

    return df_grouped.to_json(orient='records')

    # This function standardizes inconsistent product category names from different data sources

@udf.function()
def standardize_category(productName: str, rawCategory: str) -> dict:
    # Define category mappings for common variations
    category_mapping = {
        "electronics": ["electronic", "electronics", "tech", "devices"],
        "clothing": ["clothes", "clothing", "apparel", "fashion"],
        "home_goods": ["home", "household", "home goods", "furniture"],
        "food": ["food", "grocery", "groceries", "snacks"],
        "books": ["book", "books", "reading", "literature"]
    }
    
    # Normalize the input
    raw_lower = rawCategory.lower().strip()
    
    # Find the standardized category
    standardized = "other"
    for standard_name, variations in category_mapping.items():
        if raw_lower in variations:
            standardized = standard_name
            break
    
    return {
        "product_name": productName,
        "original_category": rawCategory,
        "standardized_category": standardized,
        "needs_review": standardized == "other"
    }
