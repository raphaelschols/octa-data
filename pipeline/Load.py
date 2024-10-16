from pipeline import Transform
from pipeline import Extract
import pandas as pd
import sqlite3

ufc_fighters = pd.read_csv(r"data/input/ufc_fighters.csv")

# a function that create sql table from the info_table and record_table
def create_sql_table(info_table: pd.DataFrame, record_table: pd.DataFrame) -> None:
    """Create SQL table from info_table and record_table"""
    
    # create connection to the database
    conn = sqlite3.connect("fighter.db")
    
    # create the info_table
    info_table.to_sql("fighter_info", conn, if_exists="replace", index=False)
    
    # create the record_table
    record_table.to_sql("fighter_record", conn, if_exists="replace", index=False)
    
    # close the connection
    conn.close()
    
    return "SQL tables created successfully"

def main():

    list_ufc_fighters = ufc_fighters['Fighter Name'].tolist()

    info_table, record_table = Transform.main(list_ufc_fighters)

    create_sql_table(info_table, record_table)

    return "SQL tables created successfully"

main()