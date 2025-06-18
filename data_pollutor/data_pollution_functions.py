import pandas as pd
import numpy as np
import random as rd
import nlpaug.augmenter.char as nac
import re
from datetime import datetime, date
from faker import Faker
fake = Faker()
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="- %(levelname)s - %(asctime)s - %(message)s"
)

#----------------------------------------------------------------
def insert_char_between_xchars(
    string: str,
    x: int,
    new_char: str
    ):
    """
        Transform a string by inserting a specified character
        following a pattern: every x characters. For example,
        insert_char_between_xchars('111',1,'_') = '1_1_1'
    """
    modified_string = new_char.join(
        string[i:i+x] for i in range(0, len(string), x)
    )
    return modified_string

#----------------------------------------------------------------
def append_xchars(
    string: str,
    new_char: str,
    x: int = 1
    ):
    """
        Transform a string by adding a specified character at the
        end of the string. The added character can be added x times.
        For example, append_xchars('111','_',2) ='111__'
    """
    modified_string = string + (new_char * x)
    return modified_string

#----------------------------------------------------------------
def transform_string_to_lower(string: str):
    """
        Transform all characters of a string to lower characters
    """
    return string.lower()

#----------------------------------------------------------------
def transform_string_to_upper(string: str):
    """
        Transform all characters of a string to upper characters
    """
    return string.upper()

#----------------------------------------------------------------
def move_leading_digits_to_end(
    string: str,
    add_str: bool = False
    ):
    """
        Transform a string starting with digits by moving them
        to the end of the string. Function used to modify the
        format of addresses, including  the possibility to add
        the characters 'str.' before the digits. For example, 
        move_leading_digits_to_end('22 X') = 'X 22', and 
         move_leading_digits_to_end('22 X', True) = 'X str. 22'
    """
    match = re.match(r'^(\d+)(.*)', string)
    if match:
        digits, rest = match.groups()
        if add_str:
            modified_string = rest.strip() + ' str. ' + digits
        else:    
            modified_string = rest.strip() + ' ' + digits
    else:
        modified_string = string
    return modified_string

#----------------------------------------------------------------
def replace_chars(
    string: str,
    old_char: str,
    new_char: str,
    end_only: bool = False
    ):
    """
        Transform a string by replacing existing characters by 
        new ones. Can be applied to characters located at the end
        of the string or anywhere in the string. For example,
        replace_chars('banana', 'na', 'bo', True) = 'banabo', and
        replace_chars('banana', 'na', 'bo', False) = 'babobo'
    """
    if end_only:
        if string.endswith(old_char):
            modified_string = string[:-len(old_char)] + new_char
        else:
            modified_string = string
    else:
        modified_string = string.replace(old_char, new_char)
    return modified_string

#----------------------------------------------------------------
def undouble_letters(string:str):
    """
        Transform a string containing doubled letters by removing
        one of the two letters.
        For example: undouble_letters('letter') = 'leter'
    """
    modified_string = re.sub(r'(.)\1+', r'\1', string)
    return modified_string

#----------------------------------------------------------------
def randomly_double_letter(
    string: str,
    letter: str
    ):
    """
        Transform a string by doubling a specified letter if when 
        found is not already doubled. For example: 
        randomly_double_letter('banana', 'a') = 'banaana'
    """
    if string.count(letter) >= 1 and letter * 2 not in string:
        indices = [i for i, char in enumerate(string) if char == letter]
        index_to_double = rd.choice(indices)
        modified_string = string[:index_to_double + 1] + letter + string[index_to_double + 1:]
    else:
        modified_string = string
    return modified_string

#----------------------------------------------------------------
def randomly_double_letters(
    string: str,
    letters: list
    ):
    """
        Transform a string by doubling specified letters if when 
        found that are not already doubled. For example: 
        randomly_double_letter('banana', ['a','n']) = 'baannana'
    """
    eligible_letters = [
        letter for letter in letters if string.count(letter) >= 1 
        and letter * 2 not in string
    ]
    if not eligible_letters:
        return string  # No eligible letters to double
    letter_to_double = rd.choice(eligible_letters)
    indices = [i for i, char in enumerate(string) if char == letter_to_double]
    if indices:
        index_to_double = rd.choice(indices)
        modified_string = (
            string[:index_to_double + 1] +
            letter_to_double +
            string[index_to_double + 1:]
        )
    else:
        modified_string = string
    return modified_string

#----------------------------------------------------------------
def augment_alpha_only(
    string: str,
    aug # instantiated augmenter from nlpaug
    ):
    """
        Transform a string by randomly injecting an alphanumeric
        character. aug is an instance of BaseAugmenter, instantiated 
        as follow: nac.RandomCharAug(action="insert", aug_char_max).
        For example: augment_alpha_only('test', aug) = 'tesjt'
    """
    def replace_alpha(match):
        return aug.augment(match.group(0))[0]
    modified_string = re.sub(r'[A-Za-z]+', replace_alpha, string)
    return modified_string

#----------------------------------------------------------------
def swap_char(
    string: str,
    aug_swap # instantiated augmenter from nlpaug
    ):
    """
        Transform a string by randomly swaping characters. aug is 
        an instance of BaseAugmenter, instantiated as follow: 
        nac.RandomCharAug(action="swap", aug_char_max).
        For example: swap_char('test', aug) = 'tets'
    """

    modified_string = aug_swap.augment(string)[0]
    return modified_string

#----------------------------------------------------------------
def replace_with_random(
    original_value,
    replacement_list: list
    ):
    """
       Transform a string by modifying it by another one randomly
       selected from a list. For example: 
       replace_with_random(['test', '//']) = '//'
    """

    if not replacement_list:
        raise ValueError("replacement_list cannot be empty.")
    modified_string = rd.choice(replacement_list)
    return modified_string

#----------------------------------------------------------------
def partially_permute_cell(
    dataframe: pd.DataFrame,
    column: str,
    fraction: float, 
    seed = None
    ):
    """
        Permute the values on a column of a sample of randomly
        selected rows in a DataFrame. The size of the sample is
        specified as a fraction.
    """
    logging.info(f"Permute the values of {fraction*100}% of the values "
                 f"of attribute {column}.")

    if not (0 < fraction <= 1):
        raise ValueError("fraction must be between 0 and 1")

    dataframe_copy = dataframe.copy()
    n = len(dataframe_copy)
    num_to_permute = int(np.floor(fraction * n))

    rng = np.random.default_rng(seed)
    indices = rng.choice(n, size=num_to_permute, replace=False)

    original_values = dataframe_copy.loc[indices, column].values
    permuted_values = rng.permutation(original_values)

    dataframe_copy.loc[indices, column] = permuted_values

    return dataframe_copy

#----------------------------------------------------------------
def update_to_none_random(
    dataframe: pd.DataFrame,
    column: str,
    portion: float
    ):
    """
        
    """
    logging.info(f"Replace {portion*100}% of the values of attribute "
                 f"{column} by Null.")

    dataframe_copy = dataframe.copy()
    num_indices = int(len(dataframe_copy) * portion)
    num_indices = min(num_indices, len(dataframe_copy))
    if num_indices == 0:
        return dataframe_copy
    random_indices = rd.sample(list(dataframe_copy.index), k=num_indices)
    dataframe_copy.loc[random_indices, column] = None
    return dataframe_copy

#----------------------------------------------------------------
def set_day_to_first(
    date: date,
    relative: str = None,
    relative_date: date = None
    ):
    """
        Trasnform a date by setting it to the first day of the
        same month. Possibility to only apply the transformation if
        the date is earlier or later than a reference date. 
        For example, set_day_to_first('2020'05-19') = '2020-05-01'
        and set_day_to_first('2020'05-19','before','2021-01-01')
        = '2020'05-19'
    """

    if relative and relative not in ['before', 'after']:
        raise ValueError("the relative attribute should be 'before' or 'after'")
    if relative and not relative_date:
        raise ValueError("please provide a relative date")
        
    date = pd.to_datetime(date)
    
    if relative and relative == 'before':
        if date < pd.to_datetime(relative_date):
            return date.replace(day=1).date()
        else:
            return date.date()
    elif relative and relative == 'after':
        if date > pd.to_datetime(relative_date):
            return date.replace(day=1).date()
        else:
            return date.date()
    else:
        return date.date()

#----------------------------------------------------------------
def swap_day_month(date_input: date):
    """
        Transform a date by swapping its day and month if the day
        is between 1 and 12. 
        For example, swap_day_month('2020-05-09') = '2020-09-05'
        
    """

    if isinstance(date_input, str):
        date = datetime.strptime(date_input, "%Y-%m-%d")
    elif isinstance(date_input, datetime):
        date = date_input
    else:
        date = datetime.combine(date_input, datetime.min.time())

    day = date.day
    month = date.month

    if day <= 12 and month <= 12:
        try:
            swapped = date.replace(day=month, month=day)
            return swapped.date()
        except ValueError:
            pass

    return date.date()

#----------------------------------------------------------------
def replace_year_within_range(
    date_input: date,
    year_start: int,
    year_end: int
    ):
    """
        Transform a date by replacing its year by another year
        between range. For example, 
        replace_year_within_range('2020-05-09', 2015, 2022) = '2018-05-09'
    """

    if isinstance(date_input, str):
        date = datetime.strptime(date_input, "%Y-%m-%d")
    elif isinstance(date_input, datetime):
        date = date_input
    else:
        date = datetime.combine(date_input, datetime.min.time())

    new_year = rd.randint(year_start, year_end)
    try:
        return date.replace(year=new_year).date()
    except ValueError:
        if date.month == 2 and date.day == 29:
            return date.replace(year=new_year, day=28).date()
        else:
            raise

#----------------------------------------------------------------
def apply_function_to_fraction(
    dataframe: pd.DataFrame,
    column: str,
    function: str,
    fraction: float,
    seed = None,
    *args, **kwargs
    ):
    """
        Allows to apply a specific transformation function to a
        fraction of a dataframe's columns.
    """
    logging.info(f"Polluting the values of attribute {column} "
                 f"by applying function {function} to "
                 f"{fraction*100}% of its values.")

    if not (0 < fraction <= 1):
        raise ValueError("fraction must be between 0 and 1")

    dataframe_copy = dataframe.copy()
    n = len(dataframe_copy)
    dataframe_indices = dataframe_copy.index.values.tolist()
    shuffled_indices = np.random.permutation(dataframe_indices)
    num_to_modify = int(np.floor(fraction * n))

    rng = np.random.default_rng(seed)
    indices = rd.sample(list(shuffled_indices), k=num_to_modify)

    dataframe_copy.loc[indices, column] = dataframe_copy.loc[
        indices, column
    ].apply(lambda x: function(x, *args, **kwargs))

    return dataframe_copy

#----------------------------------------------------------------
def replace_random_attribute(
    dataframe: pd.DataFrame,
    column: str,
    name_map: dict
    ):
    """
        Find the subset of the DataFrame where specified column is
        equal to one of the name_map dictionnary's keys and remplace
        a portion of it (from one randomly selected row and all the
        following ones) by the associated value in name_map.
    """
    logging.info(f"Replace specific values within attribute {column} "
                 f"with associated values from specified dictionnary: "
                 f"{name_map}.")

    dataframe_copy = dataframe.copy()
    for old_name, new_name in name_map.items():
        matches = dataframe_copy[
            dataframe_copy[column].str.lower() == old_name.lower()
        ]
        
        if not matches.empty:
            match_positions = matches.index.tolist()
            chosen_position = rd.choice(match_positions)

            for i in match_positions:
                if i >= chosen_position:
                    if dataframe_copy.at[i, column].lower() == old_name.lower():
                        dataframe_copy.at[i, column] = new_name
    return dataframe_copy