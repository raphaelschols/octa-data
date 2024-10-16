import wikipediaapi
import pandas as pd


def fetch_wiki_tables(url: str) -> list[pd.DataFrame]:
    """Fetches tables from a wikipedia url"""
    tables = pd.read_html(url)

    return tables


def is_ufc_fighter(page: wikipediaapi.WikipediaPage) -> bool:
    """Checks if a wikipedia page is a UFC fighter page"""

    return "UFC" in page.summary


def is_disambiguation_page(page: wikipediaapi.WikipediaPage) -> bool:
    """Checks if a wikipedia page is a disambiguation page"""

    return "may refer to:" in page.summary


def harvest_fighter_page(name: str) -> wikipediaapi.WikipediaPage:
    
    """harvests the fighter page from a wikipedia url"""

    wiki_wiki = wikipediaapi.Wikipedia("agent_perry")

    page = wiki_wiki.page(name)

    # If the page is a disambiguation page, we need to find the fighter page
    if is_disambiguation_page(page):
        print(f"{name} is causing ambiguity looking for the fighter page")

        name_pages = [wiki_wiki.page(link) for link in page.links]
        fighter_page = next(page for page in name_pages if is_ufc_fighter(page))
    else:
        fighter_page = page

    return fighter_page


def harvest_record_table(name: str) -> pd.DataFrame:

    fighter_page = harvest_fighter_page(name)

    figher_url = fighter_page.fullurl

    tables = fetch_wiki_tables(figher_url)

    expected_column_names = [
        "Res.",
        "Record",
        "Opponent",
        "Method",
        "Event",
        "Date",
        "Round",
        "Time",
        "Location",
        "Notes",
    ]

    # Filter tables that contain the expected column names and have the word "UFC" in the "Event" column
    ufc_table = [
        table
        for table in tables
        if list(table.columns) == expected_column_names
        and table["Event"].str.contains("UFC").sum() > 0
    ]

    # Assert that we don't have more than 1 table
    if len(ufc_table) != 1:
        raise ValueError(f"Error: More than 1 table found ({len(ufc_table)} tables)")

    return ufc_table[0]


def harvest_fighter_info(name: str) -> pd.DataFrame:
    """harvests the fighter info table from a wikipedia url"""

    # harvest the fighter page
    fighter_page = harvest_fighter_page(name)
    figher_url = fighter_page.fullurl

    tables = fetch_wiki_tables(figher_url)

    info_table = tables[0]

    req_values = [
        "Born",
        "Nickname",
        "Height",
        "Weight",
        "Division",
        "Reach",
        "Style",
        "Mixed martial arts record",
        "Total",
        "Wins",
        "Losses",
        "By knockout",
        "By submission",
        "By decision",
        "Draws",
        "No contests",
    ]

    req_rows = info_table[info_table.iloc[:,0].isin(req_values)]

    [x for x in info_table.iloc[:,0]]

    req_rows = info_table.drop(info_table.index[8:])

    req_rows.loc["Fighter"] = fighter_page.title

    return req_rows


