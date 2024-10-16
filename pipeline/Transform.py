from pipeline import Extract
import pandas as pd
import re
from datetime import datetime as dt


# name = "Max Holloway"
# info_table = Harvester.harvest_fighter_info(name)
# record_table = Harvester.harvest_record_table(name)


def create_unique_fighter_key(
    info_table: pd.DataFrame, record_table: pd.DataFrame
) -> list[pd.DataFrame, pd.DataFrame]:
    """Create a unique fighter key for each fighter in the info_table and record_table"""

    # Extract the fighter name from the info_table
    fighter_name = info_table.columns[0]
    first_3_hash_name = str(abs(hash(fighter_name)))[:3]

    birth_data = info_table[info_table.iloc[:, 0].isin(["Born"])].iloc[:, 1].values[0]

    date_of_birth = re.search(
        r"\b(\d{1,2}\s+\w+\s+\d{4}|\w+\s+\d{1,2},?\s+\d{4})\b", birth_data
    ).group(1)
    formatted_dob = pd.to_datetime(date_of_birth).strftime("%Y%m%d")

    info_table[["FighterKey", "Fighter"]] = (
        int(first_3_hash_name + formatted_dob),
        fighter_name,
    )
    record_table[["FighterKey", "Fighter"]] = (
        int(first_3_hash_name + formatted_dob),
        fighter_name,
    )
    return info_table, record_table


def process_info_table(info_table: pd.DataFrame) -> pd.DataFrame:

    # info_table = create_unique_fighter_key(info_table, record_table)[0]

    name_first_column = info_table.columns[0]
    name_second_column = info_table.columns[1]

    # pivot the info_table
    melted_info_table = info_table.pivot(
        index=["FighterKey", "Fighter"],
        columns=name_first_column,
        values=name_second_column,
    ).reset_index()

    req_col = [
        "FighterKey",
        "Fighter",
        "Born",
        "Division",
        "Height",
        "Reach",
        "Stance",
        "Weight",
    ]

    melted_info_table = melted_info_table.reindex(columns=req_col)

    # remove index
    melted_info_table.columns.name = None

    return melted_info_table


def process_record_table(record_table: pd.DataFrame) -> pd.DataFrame:

    # record_table = create_unique_fighter_key(info_table, record_table)[1]

    req_columns = [
        "FighterKey",
        "Fighter",
        "Res.",
        "Record",
        "Opponent",
        "Method",
        "Event",
        "Date",
        "Round",
        "Time",
        "Location",
    ]

    record_table = record_table[req_columns]

    return record_table


def main(name: list) -> pd.DataFrame:
    """ " Main function to process the info_table and record_table for each fighter in the list of fighters"""

    fighter_info = pd.DataFrame()
    fighter_record = pd.DataFrame()

    for name in name:
        info_table = Extract.harvest_fighter_info(name)
        record_table = Extract.harvest_record_table(name)

        info_table, record_table = create_unique_fighter_key(info_table, record_table)

        processed_info_table_row = process_info_table(info_table)
        processed_record_table = process_record_table(record_table)

        fighter_info = pd.concat([fighter_info, processed_info_table_row])
        fighter_record = pd.concat([fighter_record, processed_record_table])

    return fighter_info.reset_index(), fighter_record.reset_index()


# name = ["Max Holloway", "Conor McGregor"]

# info_table, record_table = main(name)

# info_table.columns
