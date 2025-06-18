# BEFORE EXECUTING THIS FILE, PLEASE FILL THE DICTIONNARY BELOW
# Change the name of several female doctors to similuate a change of last_name
# following a wedding - this is done in several steps
# first, get the list of all unique full names (fist_name and last_name)
# unique_doctors = doctor_au_dirty[['first_name', 'last_name']].drop_duplicates()
# unique_doctors_comb = unique_doctors.apply(lambda row: [row['first_name'], row['last_name']], axis=1)

# Fill the dictionnary below to dictate the change of names to operate
last_names_to_change = {
    'Smith': 'Levesques',
    'Odonnell': 'Assaf'
    # 'Pena': 'Andrews',
    # 'Peterson': 'Bologn',
    # 'Soto': 'Andrzej',
    # 'Norton': 'Khoury',
    # 'Pennington': 'Konate',
    # 'Nguyen': 'Borgia',
    # 'Robinson': 'Rastapopoulos',
    # 'Osborne': 'Aladdin',
    # 'Sellers': 'Momani',
    # 'Frazier': 'Etoum',
    # 'Riggs': 'Martin',
    # 'Simpson': 'Levesques',
    #: 'Zimmer',
    #: 'Bennett',
    # : 'Hweitat',
    # 'Romero': 'Wang',
    # 'Greer': 'Mercado'
}

import pandas as pd
#import nlpaug.augmenter.char as nac
from  data_pollutor.data_pollution_functions import *
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="- %(levelname)s - %(asctime)s - %(message)s"
)

# Import data from relations polluted with artificial unicity
logging.info(f"Importing csv files containing the data of the relations "
             f"of which attribute will be polluted ")

microchip_code_au = pd.read_csv('working_data/microchip_code_au.csv')
microchip_au = pd.read_csv('working_data/microchip_au.csv')
animal_au = pd.read_csv('working_data/animal_au.csv')
owner_au = pd.read_csv('working_data/owner_au.csv')
appointment_au = pd.read_csv('working_data/appointment_au.csv')
doctor_au = pd.read_csv('working_data/doctor_au.csv')
service_au = pd.read_csv('working_data/service_au.csv')

# Preparation of required arguments
# Instantiate BaseAugmenter to perform random insertions
logging.info(f"Instantiating BaseAugmenter to perform random insertions.")
aug_insert = nac.RandomCharAug(
    action="insert",
    aug_char_max=1
)
# Instantiate BaseAugmenter to perform random swaps
logging.info(f"Instantiating BaseAugmenter to perform random swaps.")
aug_swap = nac.RandomCharAug(
    action="swap",
    aug_char_max=1
)
# Prepare the list of characters to use for random replacements
replacement_list = ['/', '//', '///', '*', '**', '***', '-', '--', '---', '##', ' ', '   ', '.', '..', '...']

#================================================================
# POLLUTE DATA OF RELATION ANIMAL
logging.info(f"Starting to pollute Animal relation.")
animal_au_dirty = animal_au.copy()

#----------------------------------------------------------------------------
# Pollute attribute 'name'
logging.info(f"Polluting attribute 'name' in relation Animal.")

#  Transform all characters of names to lower case for 40% of the rows
animal_au_dirty= apply_function_to_fraction(
    dataframe=animal_au_dirty,
    column='name',
    function=transform_string_to_lower,
    fraction=0.4
)
# Randomly double one of these letters : 'l', 'n', 'b' if they are found in 
# the name for 10% of the rows
animal_au_dirty = apply_function_to_fraction(
    dataframe=animal_au_dirty,
    column='name',
    function=randomly_double_letters,
    fraction=0.1,
    letters=['l','n','b']
)
# Replace 'y' to 'ie' at the end of the name for 20% of rows if the name
# ends by 'y'
animal_au_dirty = apply_function_to_fraction(
    dataframe=animal_au_dirty,
    column='name',
    function=replace_chars,
    fraction=0.2,
    old_char='y',
    new_char='ie',
    end_only=True
)
# Replace 'oo' to 'ou' at the end of the name for 10% of rows if the name
# ends by 'oo'
animal_au_dirty = apply_function_to_fraction(
    dataframe=animal_au_dirty,
    column='name',
    function=replace_chars,
    fraction=0.1,
    old_char='oo',
    new_char='ou',
    end_only=False
)
# Randomly insert a letter in 5% of the rows
animal_au_dirty = apply_function_to_fraction(
    dataframe=animal_au_dirty,
    column='name',
    function=augment_alpha_only,
    fraction=0.05,
    aug=aug_insert
)
# Transform all characters of names to upper case for 2% of the rows
animal_au_dirty= apply_function_to_fraction(
    dataframe=animal_au_dirty,
    column='name',
    function=transform_string_to_upper,
    fraction=0.02
)
# Randomly swap characters in 5% of the rows
animal_au_dirty = apply_function_to_fraction(
    dataframe=animal_au_dirty,
    column='name',
    function=swap_char,
    fraction=0.05,
    aug_swap=aug_swap
)
#----------------------------------------------------------------------------
# Pollute attribute 'species'
logging.info(f"Polluting attribute 'species' in relation Animal.")

# Permute the values of species on 0.5% of the rows
animal_au_dirty = partially_permute_cell(
    dataframe=animal_au_dirty,
    column='species',
    fraction=0.005
)
#----------------------------------------------------------------------------
# Pollute attribute 'breed'
logging.info(f"Polluting attribute 'breed' in relation Animal.")

# Permute the values of breeds on 5% of the rows
animal_au_dirty= partially_permute_cell(
    dataframe=animal_au_dirty,
    column='breed',
    fraction=0.05
)
#----------------------------------------------------------------------------
# Pollute attribute 'gender'
logging.info(f"Polluting attribute 'gender' in relation Animal.")

# Permute the values of genders on 1% of the rows
animal_au_dirty= partially_permute_cell(
    dataframe=animal_au_dirty,
    column='gender',
    fraction=0.01
)
#----------------------------------------------------------------------------
# Pollute attribute 'weight'
logging.info(f"Polluting attribute 'weight' in relation Animal.")

# Transform the values of weight to Null/None for 10% of the rows
animal_au_dirty = update_to_none_random(
    dataframe=animal_au_dirty,
    column='weight',
    portion=0.1
)
#----------------------------------------------------------------------------
# Pollute attribute 'dob'
logging.info(f"Polluting attribute 'dob' in relation Animal.")

# Transform the dob to first day of the month for 78% of the rows for which
# original dob was earlier than 2019-01-01
animal_au_dirty = apply_function_to_fraction(
    dataframe=animal_au_dirty,
    column='dob',
    function=set_day_to_first,
    fraction=0.78,
    relative='before',
    relative_date='2019-01-01'
)
# Swap the day and the month when the day <= 12 for 10% of the rows
animal_au_dirty = apply_function_to_fraction(
    dataframe=animal_au_dirty,
    column='dob',
    function=swap_day_month,
    fraction=0.1
)
# Replace the year of a dob with a year beteen 2005 and 2015 for 10%
# of the rows
animal_au_dirty = apply_function_to_fraction(
    dataframe=animal_au_dirty,
    column='dob',
    function=replace_year_within_range,
    fraction=0.1,
    year_start=2005,
    year_end=2025
)
# Transform the values of dob to Null/None for 20% of the rows
animal_au_dirty = update_to_none_random(
    dataframe=animal_au_dirty,
    column='dob',
    portion=0.2
)

#================================================================
# POLLUTE DATA OF RELATION MICROCHIP_CODE
logging.info(f"Starting to pollute Microchip_Code relation.")
microchip_code_au_dirty = microchip_code_au.copy()
microchip_code_au_dirty['code'] = microchip_code_au_dirty['code'].astype(str)
#----------------------------------------------------------------------------
# Pollute attribute 'code'
logging.info(f"Polluting attribute 'code' in relation Microchip_code.")

# Transform the code by inserting character '.' between each digit in 10% of rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='code',
    function=insert_char_between_xchars,
    fraction=0.1,
    x=1,
    new_char='.'
)
# Transform the code by inserting character '/' between each digit in 5% of rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='code',
    function=insert_char_between_xchars,
    fraction=0.05,
    x=1,
    new_char='/'
)
# Transform the code by inserting character '-' between each digit in 5% of rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='code',
    function=insert_char_between_xchars,
    fraction=0.05,
    x=1,
    new_char='-'
)
# Add character '-' at the end of the code in 5% of the rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='code',
    function=append_xchars,
    fraction=0.05,
    x=1,
    new_char='-'
)
# Add character '/' at the end of the code in 5% of the rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='code',
    function=append_xchars,
    fraction=0.05,
    x=1,
    new_char='/'
)
# Add space (' ') at the end of the code in 5% of the rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='code',
    function=append_xchars,
    fraction=0.05,
    x=1,
    new_char=' '
)
#----------------------------------------------------------------------------
# Pollute attribute 'brand'
logging.info(f"Polluting attribute 'brand' in relation Microchip_code.")

#  Transform all characters of brand to lower case for 60% of the rows
microchip_code_au_dirty= apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='brand',
    function=transform_string_to_lower,
    fraction=0.6
)
# Randomly swap characters in 10% of the rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='brand',
    function=swap_char,
    fraction=0.1,
    aug_swap=aug_swap
)
# Replace '-' found in brand by a space for 10% of rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='brand',
    function=replace_chars,
    fraction=0.1,
    old_char='-',
    new_char='',
    end_only=False
)
# Remove spaces found in brand by for 10% of rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='brand',
    function=replace_chars,
    fraction=0.1,
    old_char=' ',
    new_char='',
    end_only=False
)
# Replace '-' found in brand by '_' for 40% of rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='brand',
    function=replace_chars,
    fraction=0.4,
    old_char='-',
    new_char='_',
    end_only=False
)
#----------------------------------------------------------------------------
# Pollute attribute 'provider'
logging.info(f"Polluting attribute 'provider' in relation Microchip_code.")

# Randomly swap characters in 10% of the rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='provider',
    function=swap_char,
    fraction=0.1,
    aug_swap=aug_swap
)
#  Transform all characters of provider to lower case for 40% of the rows
microchip_code_au_dirty= apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='provider',
    function=transform_string_to_lower,
    fraction=0.6
)
# Randomly insert a letter in 10% of the rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='provider',
    function=augment_alpha_only,
    fraction=0.1,
    aug=aug_insert
)
# Transform the values of provider to Null/None for 10% of the rows
microchip_code_au_dirty = update_to_none_random(
    dataframe=microchip_code_au_dirty,
    column='provider',
    portion=0.1
)
#----------------------------------------------------------------------------
# Pollute attribute 'country'
logging.info(f"Polluting attribute 'country' in relation Microchip_code.")

# Replace country's value by 'UK' when country = 'United Kingdom' in 50% of the rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='country',
    function=replace_chars,
    fraction=0.5,
    old_char='United Kingdom',
    new_char='UK',
    end_only=False
)
# Replace country's value by 'U.S.A' when country = 'USA' in 50% of the rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='country',
    function=replace_chars,
    fraction=0.5,
    old_char='USA',
    new_char='U.S.A',
    end_only=False
)
#  Transform all characters of country to lower case for 40% of the rows
microchip_code_au_dirty= apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='country',
    function=transform_string_to_lower,
    fraction=0.5
)
# Randomly swap characters in 10% of the rows
microchip_code_au_dirty = apply_function_to_fraction(
    dataframe=microchip_code_au_dirty,
    column='country',
    function=swap_char,
    fraction=0.1,
    aug_swap=aug_swap
)
# Transform the values of country to Null/None for 10% of the rows
microchip_code_au_dirty = update_to_none_random(
    dataframe=microchip_code_au_dirty,
    column='country',
    portion=0.1
)
#================================================================
# POLLUTE DATA OF RELATION OWNER
logging.info(f"Starting to pollute Owner relation.")
owner_au_dirty = owner_au.copy()
#----------------------------------------------------------------------------
# Pollute attribute 'first_name'
logging.info(f"Polluting attribute 'first_name' in relation Owner.")

#  Transform all characters of first_name to lower case for 40% of the rows
owner_au_dirty= apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='first_name',
    function=transform_string_to_lower,
    fraction=0.4
)
# Randomly double one of these letters : 'l', 'n', 'b' if they are found in 
# the first_name for 10% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='first_name',
    function=randomly_double_letters,
    fraction=0.1,
    letters=['l','n','b']
)
# Replace 'y' to 'ie' at the end of the first_name for 20% of rows if the name
# ends by 'y'
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='first_name',
    function=replace_chars,
    fraction=0.2,
    old_char='y',
    new_char='ie',
    end_only=True
)
# Replace 'ie' to 'y' at the end of the first_name for 20% of rows if the name
# ends by 'ie'
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='first_name',
    function=replace_chars,
    fraction=0.2,
    old_char='ie',
    new_char='y',
    end_only=True
)
# Randomly insert a letter in 5% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='first_name',
    function=augment_alpha_only,
    fraction=0.05,
    aug=aug_insert
)
# Transform all characters of first_name to upper case for 5% of the rows
owner_au_dirty= apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='first_name',
    function=transform_string_to_upper,
    fraction=0.05
)
# Randomly swap characters in 15% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='first_name',
    function=swap_char,
    fraction=0.15,
    aug_swap=aug_swap
)
#----------------------------------------------------------------------------
# Pollute attribute 'last_name'
logging.info(f"Polluting attribute 'last_name' in relation Owner.")

#  Transform all characters of last_name to lower case for 40% of the rows
owner_au_dirty= apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='last_name',
    function=transform_string_to_lower,
    fraction=0.4
)
# Randomly double one of these letters : 'l', 'n', 'b' if they are found in 
# the last_name for 10% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='last_name',
    function=randomly_double_letters,
    fraction=0.1,
    letters=['l','n','b']
)
# Replace 'y' to 'ie' at the end of the last_name for 20% of rows if the name
# ends by 'y'
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='last_name',
    function=replace_chars,
    fraction=0.2,
    old_char='y',
    new_char='ie',
    end_only=True
)
# Replace 'ie' to 'y' at the end of the last_name for 20% of rows if the name
# ends by 'ie'
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='last_name',
    function=replace_chars,
    fraction=0.2,
    old_char='ie',
    new_char='y',
    end_only=True
)
# Randomly insert a letter in 5% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='last_name',
    function=augment_alpha_only,
    fraction=0.05,
    aug=aug_insert
)
# Transform all characters of last_name to upper case for 20% of the rows
owner_au_dirty= apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='last_name',
    function=transform_string_to_upper,
    fraction=0.2
)
# Randomly swap characters in 15% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='last_name',
    function=swap_char,
    fraction=0.15,
    aug_swap=aug_swap
)
# Replace the last_name value with a random string for 12% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='last_name',
    function=replace_with_random,
    fraction=0.12,
    replacement_list=replacement_list
)
#----------------------------------------------------------------------------
# Pollute attribute 'address'
logging.info(f"Polluting attribute 'address' in relation Owner.")

# Add '*' at the end of the address in 5% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='address',
    function=append_xchars,
    fraction=0.05,
    new_char='*',
    x=1
)
# Add '-' at the end of the address in 1% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='address',
    function=append_xchars,
    fraction=0.01,
    new_char='-',
    x=1
)
#  Transform all characters of address to lower case for 60% of the rows
owner_au_dirty= apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='address',
    function=transform_string_to_lower,
    fraction=0.6
)
# Transform all characters of address to upper case for 5% of the rows
owner_au_dirty= apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='address',
    function=transform_string_to_upper,
    fraction=0.05
)
# Randomly double one of these letters : 'l', 'n', 'b', 's' if they are 
# found in the address for 10% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='address',
    function=randomly_double_letters,
    fraction=0.1,
    letters=['l','n','b', 's']
)
# move the number to the end preceeded by 'str.' in 35% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='address',
    function=move_leading_digits_to_end,
    fraction=0.35,
    add_str=True
)
# move the number to the end in 20% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='address',
    function=move_leading_digits_to_end,
    fraction=0.2,
    add_str=False
)
# Randomly insert a letter in 5% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='address',
    function=augment_alpha_only,
    fraction=0.05,
    aug=aug_insert
)
# Randomly swap characters in 5% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='address',
    function=swap_char,
    fraction=0.05,
    aug_swap=aug_swap
)
# Replace the address value with a random string for 17% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='address',
    function=replace_with_random,
    fraction=0.17,
    replacement_list=replacement_list
)
# Transform the values of address to Null/None for 7% of the rows
owner_au_dirty = update_to_none_random(
    dataframe=owner_au_dirty,
    column='address',
    portion=0.07
)
#----------------------------------------------------------------------------
# Pollute attribute 'city'
logging.info(f"Polluting attribute 'city' in relation Owner.")

# Add '*' at the end of the city in 2% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='city',
    function=append_xchars,
    fraction=0.02,
    new_char='*',
    x=1
)
#  Transform all characters of city to lower case for 60% of the rows
owner_au_dirty= apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='city',
    function=transform_string_to_lower,
    fraction=0.6
)
# Transform all characters of city to upper case for 25% of the rows
owner_au_dirty= apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='city',
    function=transform_string_to_upper,
    fraction=0.25
)
# Randomly double one of these letters : 'l', 'n', 'b', 's' if they are 
# found in the city for 10% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='city',
    function=randomly_double_letters,
    fraction=0.1,
    letters=['l','n','b', 's']
)
# Randomly insert a letter in 5% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='city',
    function=augment_alpha_only,
    fraction=0.05,
    aug=aug_insert
)
# Randomly swap characters in 5% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='city',
    function=swap_char,
    fraction=0.05,
    aug_swap=aug_swap
)
# Replace '-' found in brand by '_' for 20% of rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='city',
    function=replace_chars,
    fraction=0.2,
    old_char=' ',
    new_char='-',
    end_only=True
)
#----------------------------------------------------------------------------
# Pollute attribute 'postal_code'
logging.info(f"Polluting attribute 'postal_code' in relation Owner.")

owner_au_dirty['postal_code'] = owner_au_dirty['postal_code'].astype(str)
# Transform the postal_code by inserting character '-' between eevery two
#  digits in 5% of rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='postal_code',
    function=insert_char_between_xchars,
    fraction=0.05,
    x=2,
    new_char='-'
)
# Randomly swap characters in 5% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='postal_code',
    function=swap_char,
    fraction=0.05,
    aug_swap=aug_swap
)
# Randomly insert a letter in 2% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='postal_code',
    function=augment_alpha_only,
    fraction=0.02,
    aug=aug_insert
)
# Replace the postal_code value with a random string for 6% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='postal_code',
    function=replace_with_random,
    fraction=0.06,
    replacement_list=replacement_list
)
# Transform the values of postal_code to Null/None for 21% of the rows
owner_au_dirty = update_to_none_random(
    dataframe=owner_au_dirty,
    column='postal_code',
    portion=0.21
)
#----------------------------------------------------------------------------
# Pollute attribute 'phone_number'
logging.info(f"Polluting attribute 'phone_number' in relation Owner.")

# Replace the phone_number value with a random string for 1% of the rows
owner_au_dirty = apply_function_to_fraction(
    dataframe=owner_au_dirty,
    column='phone_number',
    function=replace_with_random,
    fraction=0.01,
    replacement_list=replacement_list
)
# Transform the values of phone_number to Null/None for 20% of the rows
owner_au_dirty = update_to_none_random(
    dataframe=owner_au_dirty,
    column='phone_number',
    portion=0.02
)

#================================================================
# POLLUTE DATA OF RELATION MICROCHIP
logging.info(f"Starting to pollute Microchip relation.")

microchip_au_dirty = microchip_au.copy()
#----------------------------------------------------------------------------
# Pollute attribute 'number'
logging.info(f"Polluting attribute 'number' in relation Microchip.")

microchip_au_dirty['number'] = microchip_au_dirty['number'].astype(str)
# Transform the number by inserting character '-' between every three
#  digits in 18% of rows
microchip_au_dirty = apply_function_to_fraction(
    dataframe=microchip_au_dirty,
    column='number',
    function=insert_char_between_xchars,
    fraction=0.18,
    x=3,
    new_char='-'
)
# Transform the number by inserting character '.' between every three
#  digits in 7% of rows
microchip_au_dirty = apply_function_to_fraction(
    dataframe=microchip_au_dirty,
    column='number',
    function=insert_char_between_xchars,
    fraction=0.07,
    x=3,
    new_char='.'
)
# Transform the number by inserting character '/' between every three
#  digits in 3% of rows
microchip_au_dirty = apply_function_to_fraction(
    dataframe=microchip_au_dirty,
    column='number',
    function=insert_char_between_xchars,
    fraction=0.03,
    x=3,
    new_char='/'
)
# Transform the number by inserting a space between every three
#  digits in 11% of rows
microchip_au_dirty = apply_function_to_fraction(
    dataframe=microchip_au_dirty,
    column='number',
    function=insert_char_between_xchars,
    fraction=0.11,
    x=3,
    new_char=' '
)
# Add character '*' at the end of the number in 2% of the rows
microchip_au_dirty = apply_function_to_fraction(
    dataframe=microchip_au_dirty,
    column='number',
    function=append_xchars,
    fraction=0.02,
    new_char='*',
    x=1
)
# Randomly swap characters in 5% of the rows
microchip_au_dirty = apply_function_to_fraction(
    dataframe=microchip_au_dirty,
    column='number',
    function=swap_char,
    fraction=0.05,
    aug_swap=aug_swap
)
# Replace the number value with a random string for 2% of the rows
microchip_au_dirty = apply_function_to_fraction(
    dataframe=microchip_au_dirty,
    column='number',
    function=replace_with_random,
    fraction=0.02,
    replacement_list=replacement_list
)

#----------------------------------------------------------------------------
# Pollute attribute 'implant_date'
logging.info(f"Polluting attribute 'implant_date' in relation Microchip.")

microchip_au_dirty['implant_date'] = pd.to_datetime(microchip_au_dirty['implant_date']).dt.date
# Transform the implant_date to first day of the month for 78% of the rows
# for whichoriginal implant_date was earlier than 2019-01-01
microchip_au_dirty = apply_function_to_fraction(
    dataframe=microchip_au_dirty,
    column='implant_date',
    function=set_day_to_first,
    fraction=0.78,
    relative='before',
    relative_date='2019-01-01'
)
# Swap the day and the month when the day <= 12 for 10% of the rows
microchip_au_dirty = apply_function_to_fraction(
    dataframe=microchip_au_dirty,
    column='implant_date',
    function=swap_day_month,
    fraction=0.1
)
# Replace the year of implant_date with a year beteen 2005 and 2015 for 10%
# of the rows
microchip_au_dirty = apply_function_to_fraction(
    dataframe=microchip_au_dirty,
    column='implant_date',
    function=replace_year_within_range,
    fraction=0.1,
    year_start=2005,
    year_end=2025
)
# Transform the values of implant_date to Null/None for 1% of the rows
microchip_au_dirty = update_to_none_random(
    dataframe=microchip_au_dirty,
    column='implant_date',
    portion=0.01
)

#----------------------------------------------------------------------------
# Pollute attribute 'location'
logging.info(f"Polluting attribute 'location' in relation Microchip.")

# Permute the values of location on 50% of the rows
microchip_au_dirty= partially_permute_cell(
    dataframe=microchip_au_dirty,
    column='location',
    fraction=0.5
)

#================================================================
# POLLUTE DATA OF RELATION DOCTOR
logging.info(f"Starting to pollute Doctor relation.")

doctor_au_dirty = doctor_au.copy()

#----------------------------------------------------------------------------
# Pollute attribute 'license_number'
logging.info(f"Polluting attribute 'license_number' in relation Doctor.")

# Transform the license_number by inserting character '-' between every three
#  digits in 8% of rows
doctor_au_dirty = apply_function_to_fraction(
    dataframe=doctor_au_dirty,
    column='license_number',
    function=insert_char_between_xchars,
    fraction=0.08,
    x=3,
    new_char='-'
)
# Transform the license_number by inserting character '.' between every three
#  digits in 2% of rows
doctor_au_dirty = apply_function_to_fraction(
    dataframe=doctor_au_dirty,
    column='license_number',
    function=insert_char_between_xchars,
    fraction=0.02,
    x=3,
    new_char='.'
)
# Transform the license_number by inserting a space between every three
#  digits in 2% of rows
doctor_au_dirty = apply_function_to_fraction(
    dataframe=doctor_au_dirty,
    column='license_number',
    function=insert_char_between_xchars,
    fraction=0.02,
    x=3,
    new_char=' '
)
# Randomly swap characters in 15% of the rows
doctor_au_dirty = apply_function_to_fraction(
    dataframe=doctor_au_dirty,
    column='license_number',
    function=swap_char,
    fraction=0.15,
    aug_swap=aug_swap
)
# Replace the license_number value with a random string for 2% of the rows
doctor_au_dirty = apply_function_to_fraction(
    dataframe=doctor_au_dirty,
    column='license_number',
    function=replace_with_random,
    fraction=0.02,
    replacement_list=replacement_list
)

#----------------------------------------------------------------------------
# Pollute attribute 'first_name'
logging.info(f"Polluting attribute 'first_name' in relation Doctor.")

#  Transform all characters of first_name to lower case for 25% of the rows
doctor_au_dirty= apply_function_to_fraction(
    dataframe=doctor_au_dirty,
    column='first_name',
    function=transform_string_to_lower,
    fraction=0.25
)
# Transform all characters of first_name to upper case for 5% of the rows
doctor_au_dirty= apply_function_to_fraction(
    dataframe=doctor_au_dirty,
    column='first_name',
    function=transform_string_to_upper,
    fraction=0.05
)
# Randomly swap characters in 5% of the rows
doctor_au_dirty = apply_function_to_fraction(
    dataframe=doctor_au_dirty,
    column='first_name',
    function=swap_char,
    fraction=0.05,
    aug_swap=aug_swap
)

#----------------------------------------------------------------------------
# Pollute attribute 'last_name'
logging.info(f"Polluting attribute 'last_name' in relation Doctor.")

# Replace the selected last_name with the associated new ones
doctor_au_dirty = replace_random_attribute(
    doctor_au_dirty,
    'last_name',
    last_names_to_change
)

#  Transform all characters of last_name to lower case for 10% of the rows
doctor_au_dirty= apply_function_to_fraction(
    dataframe=doctor_au_dirty,
    column='last_name',
    function=transform_string_to_lower,
    fraction=0.1
)
# Transform all characters of last_name to upper case for 25% of the rows
doctor_au_dirty= apply_function_to_fraction(
    dataframe=doctor_au_dirty,
    column='last_name',
    function=transform_string_to_upper,
    fraction=0.25
)
# Randomly swap characters in 10% of the rows
doctor_au_dirty = apply_function_to_fraction(
    dataframe=doctor_au_dirty,
    column='last_name',
    function=swap_char,
    fraction=0.1,
    aug_swap=aug_swap
)
# Replace the last_name value with a random string for 2% of the rows
doctor_au_dirty = apply_function_to_fraction(
    dataframe=doctor_au_dirty,
    column='last_name',
    function=replace_with_random,
    fraction=0.02,
    replacement_list=replacement_list
)

#================================================================
# POLLUTE DATA OF RELATION SERVICE
logging.info(f"Starting to pollute Service relation.")

service_au_dirty = service_au.copy()

#----------------------------------------------------------------------------
# Pollute attribute 'service_name'
logging.info(f"Polluting attribute 'service_name' in relation Service.")

#  Transform all characters of service_name to lower case for 50% of the rows
service_au_dirty= apply_function_to_fraction(
    dataframe=service_au_dirty,
    column='service_name',
    function=transform_string_to_lower,
    fraction=0.5
)
# Transform all characters of service_name to upper case for 20% of the rows
service_au_dirty= apply_function_to_fraction(
    dataframe=service_au_dirty,
    column='service_name',
    function=transform_string_to_upper,
    fraction=0.2
)
# Randomly swap characters in 30% of the rows
service_au_dirty = apply_function_to_fraction(
    dataframe=service_au_dirty,
    column='service_name',
    function=swap_char,
    fraction=0.3,
    aug_swap=aug_swap
)

#================================================================
# Save relations data in csv files
logging.info(f"Saving polluted relations' data into new csv files.")

## Relation microchip_code
microchip_code_au_dirty.to_csv('working_data/microchip_code_au_dirty.csv')
## Relation microchip
microchip_au_dirty.to_csv('working_data/microchip_au_dirty.csv')
## Relation owner
owner_au_dirty.to_csv('working_data/owner_au_dirty.csv')
## Relation animal
animal_au_dirty.to_csv('working_data/animal_au_dirty.csv')
## Relation doctor
doctor_au_dirty.to_csv('working_data/doctor_au_dirty.csv')
## Relation service
service_au_dirty.to_csv('working_data/service_au_dirty.csv')