import pandas as pd
import random as rd
import holidays
import string
from datetime import timedelta

def add_primary_key_values(
    relation: pd.DataFrame,
    pk_column_name: str,
    starting_id_value: int = 1
    ):
    """
        Create a new column in a dataset (relation) with specified 
        name (pk_column_name) and fill it with incremental integers 
        starting at a specified integer (starting_id_value).
        Returns the relation with the new column.
    """
    relation.insert(
        0,
        pk_column_name,
        range(starting_id_value, starting_id_value + len(relation))
    )
    return relation


# years = appointments['appt_date'].apply(lambda x: x.year).unique()
def get_country_holidays(
    years: list,
    country_code: str = 'JO'):
    """
        Returns the list of public holidays dates for specified years
        in specified country (country_code). For list of country codes 
        accepted, refer to https://pypi.org/project/holidays/.
        Default country_code is 'JO' for Jordan
    """
    supported = holidays.list_supported_countries()
    supported_codes = list(supported.keys())
    if country_code not in supported_codes:
        raise ValueError(f"The country code specified is not covered by"
                         f"package holidays, please refer to the package's"
                         f"documentation: https://pypi.org/project/holidays/")
    country_holidays = holidays.country_holidays(
        country_code,
        years=years
    )
    return country_holidays

def generate_random_strings(
    n: int,
    length: int = 10
    ):
    """
        Returns a list of size n of randomly generated strings
        of specified length.
        Used mainly to generate fake vet lience numbers.
    """
    chars = string.ascii_letters + string.digits
    return [''.join(rd.choices(chars, k=length)) for _ in range(n)]


def select_random_date_in_range(
    min_date: pd.Timestamp,
    max_date: pd.Timestamp
    ):
    """
        Returns a date of format pd.Timestamp randomly selected
        between min_date and max_date
    """
    delta = (max_date - min_date).days
    random_days = rd.randint(0, delta)
    random_date = min_date + timedelta(days=random_days)
    return random_date

def generate_random_dates(
    min_date: pd.Timestamp,
    max_date: pd.Timestamp,
    n: int):
    """
        Returns a list of size n containing dates of format pd.Timestamp
        randomly selected between min_date and max_date
    """
    random_dates = [select_random_date_in_range(min_date, max_date) for _ in range(n)]
    return random_dates
