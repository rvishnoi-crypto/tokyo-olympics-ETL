import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL Database connection details
DB_URL = 'postgres://postgres:test123@de-db.crmige0ue.us-east-2.rds.amazonaws.com/tokyo_olympics_db'

engine = create_engine(DB_URL)

# load data into PostgreSQL
def load_data_from_csv(file_path, table_name, column_mapping=None):
    df = pd.read_csv(file_path)
    
    # rename columns to match the database schema
    if column_mapping:
        df.rename(columns=column_mapping, inplace=True)
    
    df.columns = [col.lower() for col in df.columns]

    # Load the DataFrame into the corresponding table in PostgreSQL
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data from {file_path} loaded into {table_name} successfully!")

# Mappings
athletes_column_mapping = {
    'athlete id': 'athlete_id',
    'name': 'name',
    'discipline': 'discipline',
    'country': 'noc'
}
load_data_from_csv('', 'athletes', athletes_column_mapping)

coaches_column_mapping = {
    'coach name': 'name',
    'country': 'noc',
    'event': 'event'
}
load_data_from_csv('path_to_coaches.csv', 'coaches', coaches_column_mapping)

entries_gender_column_mapping = {
    'discipline': 'discipline',
    'female': 'female',
    'male': 'male',
    'total': 'total'
}
load_data_from_csv('path_to_entries_gender.csv', 'entries_gender', entries_gender_column_mapping)

medals_column_mapping = {
    'rank': 'rank',
    'country': 'noc',
    'gold': 'gold',
    'silver': 'silver',
    'bronze': 'bronze',
    'total': 'total',
    'rank by total': 'rank_by_total'
}
load_data_from_csv('path_to_medals.csv', 'medals', medals_column_mapping)

teams_column_mapping = {
    'name': 'name',
    'discipline': 'discipline',
    'country': 'noc',
    'event': 'event'
}
load_data_from_csv('path_to_teams.csv', 'teams', teams_column_mapping)

print("All data loaded successfully!")
