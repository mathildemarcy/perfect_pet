import pandas as pd
import numpy as np
import random as rd
from datetime import date, timedelta
from shared_functions import add_primary_key_values
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="- %(levelname)s - %(asctime)s - %(message)s"
)

class MicrochipCode():
    """
        This class is used to create the relation microchip_code
        and progressively generate the required data.
        The self contained DataFrame microchip_code_data is updated
        as data generation progresses.
    """
    def __init__(self,
                 microchip_code_data: pd.DataFrame):
        """
            Initialize the instance with the data contained in
            csv file microchip_codes_data.csv included in the 
            package.
            
        """
        logging.info("Instantiating object from class MicrochipCode")

        self.microchip_code_data = microchip_code_data
        self.microchip_code_data['market_year'] = self.microchip_code_data['market_year'].astype('Int64')
        logging.info(f"Generated Microchip_Code relation of size "
                     f"{len(self.microchip_code_data)}.")

    #---------------------------------------------------------------- 
    def generate_microchip_code_data_df(self):
        """
            Return the current state of the DataFrame microchip_code_data
        """
        logging.info("Return the Microchip_Code relation's data")
        return self.microchip_code_data

    #---------------------------------------------------------------- 
    def assign_id_code(self,
        microchip_code_data: pd.DataFrame = None,
        pk_column_name: str = 'id_code',
        starting_id_value: int = 1,
        ):
        """
            Creates a new column ('id_code') to the DataFrame microchip_code_data
            to serve as main identifier. Returns the DataFrame with the
            newly added id. Order of rows does not matter, no need to sort first.
        """
        logging.info("Adding PK id_code to relation Microchip_Code")

        if microchip_code_data is None or len(microchip_code_data) == 0:
            microchip_code_data = self.microchip_code_data

        microchip_code_data = add_primary_key_values(
            relation=microchip_code_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        self.microchip_code_data = microchip_code_data

        return microchip_code_data


#================================================================
class Microchip():
    """
        This class is used to create the relation microchip
        and progressively generate the required data.
        The self contained DataFrame microchip_data is updated
        as data generation progresses.
    """
    def __init__(self,
        animal_data: pd.DataFrame,
        microchip_code_data: pd.DataFrame
        ):
        """
            Initialize the instance with the previouly generated
            DataFrames animal_data and microchip_code_data.
        """
        logging.info("Instantiating object from class Microchip")

        if len(np.setdiff1d(
            ['id_tmp', 'dob'], animal_data.columns)) > 0:
            raise KeyError(f"Required columns 'id_tmp', 'dob',"
                           f"not found in DataFrame animal_data")

        self.microchip_data = animal_data[['id_tmp', 'dob']]
        logging.info(f"Generated Microchip relation of size "
                     f"{len(self.microchip_data)}.")

        self.microchip_code_data = microchip_code_data

    #---------------------------------------------------------------- 
    def generate_microchip_data_df(self):
        """
            Return the current state of the DataFrame microchip_data
        """
        logging.info("Return the Microchip relation's data")
        return self.microchip_data

    #---------------------------------------------------------------- 
    def assign_implant_date_based_on_dob(self,
        microchip_data: pd.DataFrame = None,
        dob_microchip_gap: int = 100
        ):
        """
            Creates a new column ('implant_date') to the DataFrame
            microchip_data and fills it randomly with dates that are
            between the date of birth plus one to two times the specified
            number of days (dob_microchip_gap).
            Returns DataFrame microchip_data with newly added and filled
            column.
        """
        logging.info(f"Start assigning microchip implant date to the "
                     f"microchips based on the associated animals' date "
                     f"of birth.")

        if microchip_data is None or len(microchip_data) == 0:
            microchip_data = self.microchip_data

        microchip_data['implant_date'] = None
        microchip_data['implant_date'] = [
            microchip_data['dob'].iloc[i] + pd.Timedelta(
                days=2*dob_microchip_gap - rd.randrange(dob_microchip_gap)
                )
            for i in range(len(microchip_data))
        ]

        self.microchip_data = microchip_data
        return microchip_data

    #----------------------------------------------------------------
    def assign_random_microchip_number(self,
        microchip_data: pd.DataFrame = None
        ):
        """
            Creates a new column ('microchip_number') to the DataFrame
            microchip_data and fills it with random series of 10 digits.
            Returns DataFrame microchip_data with newly added and filled
            column.
        """
        logging.info(f"Adding randomly generated microchip numbers to "
                     f"relation Microchip.")

        if microchip_data is None or len(microchip_data) == 0:
            microchip_data = self.microchip_data

        microchip_data['microchip_number'] = [
            rd.randint(10**11, 10**12 - 1) for _ in range(len( microchip_data))
        ]

        self.microchip_data = microchip_data
        return microchip_data

    #---------------------------------------------------------------- 
    def assign_fk_microchip_code(self,
        microchip_data: pd.DataFrame = None
        ):
        """
            Creates a new column ('id_code') to the DataFrame
            microchip_data which represents the relation's foreign key
            towards relation microchip_code.
            Returns DataFrame microchip_data with newly added and filled
            foreign key column.
        """
        logging.info("Adding randomly FK id_code to relation Microchip")

        if microchip_data is None or len(microchip_data) == 0:
            microchip_data = self.microchip_data

        def get_valid_code(
            implant_date: date
            ):
            implant_year = pd.to_datetime(implant_date).year
            valid_codes = self.microchip_code_data[
                self.microchip_code_data['market_year'].isna() 
                | (self.microchip_code_data['market_year'] <= implant_year)
            ]['id_code'].tolist()
            return rd.choice(valid_codes)

        microchip_data['id_code'] = microchip_data['implant_date'].apply(
            get_valid_code
        )

        self.microchip_data = microchip_data
        return microchip_data

    #---------------------------------------------------------------- 
    def assign_implant_location(self,
        microchip_data: pd.DataFrame = None,
        locations_dict: dict = None
        ):
        """
            Creates a new column ('location') to the DataFrame
            microchip_data and assign to it values from they keys of
            dictionnary locations_dict, following the given distribution.
            Returns DataFrame microchip_data with newly added and filled
            column.
        """
        logging.info(f"Adding randomly implant locations to tuples in "
                     f"relation Microchip.")

        if microchip_data is None or len(microchip_data) == 0:
            microchip_data = self.microchip_data

        if not locations_dict:
            locations_dict = {
                'between_shoulders':0.9, 
                'midline_cervicals':0.01, 
                'left_lateral_neck':0.08, 
                'right_lateral_neck':0.01
            }

        microchip_data['location'] = None

        indices_microchip_list = microchip_data.index.values.tolist()
        indices_microchip_list = np.random.permutation(indices_microchip_list)

        nb_microchip = len(indices_microchip_list)
        used_indices = []
        for k, v in locations_dict.items():
            nb_cat_microchip = int(nb_microchip * v)
            available_indices = np.setdiff1d(
                indices_microchip_list,
                used_indices
            )
            nb_cat_microchip = min(nb_cat_microchip, len(available_indices))

            if nb_cat_microchip == 0:
                continue

            selected_indices = rd.sample(list(available_indices), k=nb_cat_microchip)
            microchip_data.loc[selected_indices, 'location'] = str(k)
            used_indices.extend(selected_indices)

        self.microchip_data = microchip_data
        return microchip_data

    #---------------------------------------------------------------- 
    def sort_microchip_by_appt_date(self,
        appointment_data: pd.DataFrame,
        microchip_data: pd.DataFrame = None
        ):
        """
            Sorts rows of the DataFrame microchip_data in ascending
            order of first appointment data, by joigning it to working
            DataFrame appointment_data containing the attribute
            'appt_date_1'. Used to sort rows in microchip_data before
            assigning them values of id id_microchip.
            Returns DataFrame microchip_data sorted.
        """
        logging.info("Sorting tuples of relation Microchip by appointment date.")

        if microchip_data is None or len(microchip_data) == 0:
            microchip_data = self.microchip_data

        if 'id_tmp' not in microchip_data.columns:
            raise KeyError(f"Required column 'id_tmp' not found"
                           f"in DataFrame 'appointment_data'")

        microchip_columns = microchip_data.columns
        microchip_data_tosort = microchip_data.merge(
            appointment_data[['id_tmp', 'appt_date']],
            on=['id_tmp'],
            how='left'
        )

        microchip_data_tosort = microchip_data_tosort.sort_values(
            ['appt_date', 'id_tmp']
        ).reset_index(drop=[True, True])

        microchip_data = microchip_data_tosort[microchip_columns].drop_duplicates().reset_index(drop=True)

        self.microchip_data = microchip_data
        return microchip_data

    #---------------------------------------------------------------- 
    def assign_id_microchip(self,
        microchip_data: pd.DataFrame = None,
        pk_column_name: str = 'id_microchip',
        starting_id_value: int = 1,
        ):
        """
            Creates a new column ('id_microchip') to the DataFrame
            microchip_data to serve as main identifier. Returns the 
            DataFrame with the newly added id.
            The DataFrame microchip_data should be sorted by appointment
            date before - see function 'sort_microchip_by_appt_date'.
        """
        logging.info("Adding PK id_microchip to relation Microchip")

        if microchip_data is None or len(microchip_data) == 0:
            microchip_data = self.microchip_data

        microchip_data = add_primary_key_values(
            relation=microchip_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        self.microchip_data = microchip_data
        return microchip_data