import pandas as pd
import numpy as np
import random as rd
import math
import string
from faker import Faker
fake = Faker()
from shared_functions import add_primary_key_values
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="- %(levelname)s - %(asctime)s - %(message)s"
)

class Owner:
    """
        This class is used to create the relations owner and
        animal_owner which acts as a joint table between relations owner
        and animal, and progressively generate the required data. 
        The self contained DataFrames owner_data and animal_owner_data
        are updated as data generation progresses.
    """
    def __init__(self,
        nb_animals: int,
        prop_nb_animal_household: dict = None
        ):
        """
            Initialize the instance with the number of animals used to
            instantiate class Animal, and an indication of the 
            distribution of the proportion of households owning a certain 
            number of pets provided in a dictionnary (prop_nb_animal_household).
        """
        logging.info("Instantiating object from class Owner")

        if prop_nb_animal_household is None:
            prop_nb_animal_household = {
                4:0.05,
                3:0.1,
                2:0.25,
                1:0.60
            } 

        self.nb_animals = nb_animals
        self.prop_nb_animal_household = prop_nb_animal_household
        self.nb_households = 0
        self.nb_owners = 0
        self.owner_data = []
        self.animal_owner_data = []
        self.additional_owners = []
        self.household_several_appt = []
        self.microchips_with_many_appointments = []

    #----------------------------------------------------------------
    def compute_nb_owners(self,
        prop_household_several_owner: float = 0.3
        ):
        """
            Compute the number of households the patients are associated
            to following a distribution of the proportion of households
            owning a certain number of pets.
            The number of owners is then adjusted to account for some
            animals being brought to the clinic by different owners from
            the same household
        """
        logging.info(f"Computing the number of owners to be represented "
                     f"in the clean database instance.")
        nb_households = 0
        for k,v in self.prop_nb_animal_household.items():
            nb = round((v*self.nb_animals)/k)
            nb_households = nb_households + nb

        nb_owners = round(nb_households*(1+prop_household_several_owner))
        self.nb_households = nb_households
        self.nb_owners = nb_owners

        return nb_owners

    #----------------------------------------------------------------
    def generate_owner_profile(self,
        nb_owners: int = None,
        nb_cities: int = 10 ,
        ratio_city_streets: int = 25
        ):
        """
            Create a DataFrame containing the informations on the owners:
            first_name, last_name, address, city, postal_code, phone_number.
            All values are randomly generated using the package Faker.
        """
        logging.info(f"Star generating the owners' profiles.")
        if nb_owners is None:
            nb_owners = self.nb_owners

        owners_first_names=[]
        for _ in range(nb_owners):
            owners_first_names.append(
                fake.first_name()
            )
        owners_last_names=[]
        for _ in range(nb_owners):
            owners_last_names.append(
                fake.last_name()
            )
        cities=[]
        for _ in range(nb_cities):
            cities.append(
                fake.city()
            )
        postal_codes=[]
        for _ in range(nb_cities):
            postal_codes.append(
                fake.postcode()
            )
        streets=[]
        for _ in range(nb_cities*ratio_city_streets):
            streets.append(
                fake.street_name()
            )
        phone_numbers=[]
        for _ in range(nb_owners):
            phone_numbers.append(
                fake.phone_number()
            )

        def assign_unique_values(
            keys,
            values,
            min_vals=5,
            max_vals=50
            ):
            """
                Create a dictionnary to map values from a list to values
                from anotehr list. Used to map cities with street names.
            """
            if len(values) < len(keys) * min_vals:
                raise ValueError("Not enough elements in list2 to satisfy minimum requirements.")

            rd.shuffle(values)
            assignment = {}
            index = 0
            for key in keys:
                # Calculate how many values we can still assign
                remaining_keys = len(keys) - len(assignment)
                max_assignable = len(values) - index - (remaining_keys - 1) * min_vals
                count = rd.randint(min_vals, min(max_vals, max_assignable))
                assigned_values = values[index: index + count]
                assignment[key] = assigned_values
                index += count
            return assignment

        addresses = assign_unique_values(cities, streets)

        if len(cities) != len(postal_codes):
            raise ValueError("cities and postal_codes must be of the same length")
        if len(owners_first_names) != len(owners_last_names):
            raise ValueError("owners_first_names and owners_last_names must be of the same length")

        city_to_postal = dict(zip(cities, postal_codes))
        addresses = [(street, city) for city, streets in addresses.items() for street in streets]
        selected_addresses = rd.choices(addresses, k=len(owners_first_names))

        data = []
        for i in range(len(owners_first_names)):
            first_name = owners_first_names[i]
            last_name = owners_last_names[i]
            street, city = selected_addresses[i]
            address = f"{rd.randint(1, 150)} {street}"
            postal_code = city_to_postal[city]
            phone_number = phone_numbers[i]
            
            data.append({
                'first_name': first_name,
                'last_name': last_name,
                'address': address,
                'city': city,
                'postal_code': postal_code,
                'phone_number': phone_number
            })

        owner_data = pd.DataFrame(data)

        logging.info(f"Generated Owner relation of size "
                     f"{len(owner_data)}.")
        self.owner_data = owner_data
        return owner_data

    #----------------------------------------------------------------
    def assign_id_owner_tmp(self,
        owner_data: pd.DataFrame,
        pk_column_name: str = 'id_owner_tmp',
        starting_id_value: int = 1,
        ):
        """
            Creates a new column ('id_owner_tmp') to the dataframe (owner_data)
            to store an incremental temporary id to be used during 
            data generation until the final id (id_owner) is created.
            Return the dataframe with newly cretaed and filled 'id_owner_tmp'.
            This id_temp will be used to associate appointments and animals.
            His role is crucial in maintening integrity throughout the data 
            generation process and allow to sort the dataframes by appointment
            date before assigning final id_owner.
        """
        logging.info("Adding temporary id to relation Owner")
        owner_data = add_primary_key_values(
            relation=owner_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        self.owner_data = owner_data
        return owner_data

    #----------------------------------------------------------------
    def sort_owner_by_appt_date(self,
        appointment_data: pd.DataFrame,
        owner_data: pd.DataFrame = None
        ):
        """
            Sorts rows of the DataFrame animal_data in ascending
            order of first appointment data, by joigning it to working
            DataFrame appointment_data containing the attribute
            'appt_date_1'. Used to sort rows in animal_data before
            assigning them values of final id id_animal.
            Returns DataFrame animal_data sorted.
        """
        logging.info("Sorting tuples of relation Owner by appointment date.")
        if owner_data is None or len(owner_data) == 0:
            owner_data = self.owner_data
        
        if 'id_owner_tmp' not in owner_data.columns:
            raise KeyError(f"Required column 'id_owner_tmp' not found"
                           f"in DataFrame 'appointment_data'")

        owner_columns = owner_data.columns
        owner_data_tosort = owner_data.merge(
            appointment_data[['id_owner_tmp', 'appt_date']],
            on=['id_owner_tmp'],
            how='left'
        )
        owner_data_tosort = owner_data_tosort.sort_values(
            ['appt_date', 'id_owner_tmp']
        ).reset_index(drop=[True, True])
        owner_data = owner_data_tosort[owner_columns].drop_duplicates().reset_index(drop=True)

        self.owner_data = owner_data
        return owner_data

    #----------------------------------------------------------------
    def assign_animal_to_household(self,
        owner_data: pd.DataFrame,
        microchip_data: pd.DataFrame
        ):
        """
            Create the DataFrame animal_owner_data to associate animals
            to main owners. This function only assigns one owner to each
            animal, not taking into account the possibility of multiple
            owners (see function assign_animal_to_additional_owner).
        """
        logging.info("Start assigning animals to households/owners.")

        list_microchip_id = microchip_data['id_microchip'].unique()
        indices_microchips = microchip_data.index.values.tolist()
        rd.shuffle(indices_microchips)

        list_owner_id = owner_data['id_owner_tmp'].unique()
        indices_owners = owner_data.index.values.tolist()
        rd.shuffle(indices_owners)

        h = 1
        index_microchip = 0
        index_owner = 0
        rows=[]
        for k,v in self.prop_nb_animal_household.items():
            nb_cat_animals = int(v*self.nb_animals)
            nb_cat_owners = int(nb_cat_animals/k)
            indices_cat_owners = indices_owners[index_owner:index_owner+nb_cat_owners]

            for o in indices_cat_owners:
                indices_cat_animals = indices_microchips[index_microchip:index_microchip+k]
                for m in indices_cat_animals:
                    rows.append({
                        'id_microchip': list_microchip_id[m],
                        'id_owner_tmp': list_owner_id[o],
                        'id_household': h,
                        'i': None
                    })
                h=h+1
                index_microchip=index_microchip+k
            index_owner=index_owner+nb_cat_owners

        animal_owner_col=['id_microchip', 'id_owner_tmp', 'id_household','i']
        animal_owner_data = pd.DataFrame(
            data = rows,
            columns=animal_owner_col
        ).reset_index(drop=True)

        logging.info(f"Initial version relation Animal_Owner of size "
                     f"{len(animal_owner_data)}.")
        self.animal_owner_data = animal_owner_data
        return animal_owner_data

    #----------------------------------------------------------------
    def get_additional_owners_id(self,
        owner_data: pd.DataFrame,
        animal_owner_data: pd.DataFrame
        ):
        """
            Get the owners from DataFrame owner_data that have not yet
            been assigned to animals in the DataFrame animal_owner_data
            to be randomly assigned to animals as additional owners
        """
        logging.info("Selecting owners not assigned yet to any animal.")

        id_assigned_owners = animal_owner_data['id_owner_tmp'].unique()
        additional_owners = owner_data[~owner_data['id_owner_tmp'].isin(id_assigned_owners)]['id_owner_tmp'].to_list()
        rd.shuffle(additional_owners)
        self.additional_owners = additional_owners

        return additional_owners

    #----------------------------------------------------------------
    def get_list_animals_several_appt(self,
        appointment_data: pd.DataFrame,
        animal_data: pd.DataFrame,
        animal_owner_data: pd.DataFrame,
        min_nb_appt: int = 3 # should be at least 2
        ):
        """
            Select households with animals who visited the clinic more 
            than a specified number of times (min_nb_appt), 
            to be assigned to additional owners.
        """
        logging.info(f"Select animals who have been to more than {min_nb_appt} "
                     f"appointments to be assigned to more than one owner.")
        animals_app_count = appointment_data['id_animal'].value_counts()
        animals_with_many_appointments = animals_app_count[
            animals_app_count > min_nb_appt
        ].index.tolist()
        microchips_with_many_appointments = animal_data[
            animal_data['id_animal'].isin(animals_with_many_appointments)
        ]['id_microchip'].tolist()
        household_several_appt = animal_owner_data[
            animal_owner_data['id_microchip'].isin(microchips_with_many_appointments)
        ]['id_household'].unique()

        self.microchips_with_many_appointments = microchips_with_many_appointments
        self.household_several_appt = household_several_appt

        return household_several_appt

    #----------------------------------------------------------------
    def assign_animal_to_additional_owner(self,
        animal_owner_data: pd.DataFrame,
        additional_owners: list = None,
        household_several_appt: int = None
        ):
        """
            Assign animals from household_several_appt to selected
            additional_owners.
        """
        logging.info("Start assigning selected animals to a second owner.")
        if additional_owners is None:
            additional_owners = self.additional_owners
        if household_several_appt is None:
            household_several_appt = self.household_several_appt
        nb_additional_owners = len(additional_owners)

        households_2_owners = rd.choices(
            household_several_appt,
            k=nb_additional_owners)

        rows=[]
        i=0
        while i <= nb_additional_owners-1:
            a=int(households_2_owners[i])
            household_micro = animal_owner_data[
                (animal_owner_data['id_household']==a)
                & (animal_owner_data['id_microchip'].isin(self.microchips_with_many_appointments))
            ]['id_microchip'].tolist()
            nb_household_animals = len(household_micro)
            if nb_household_animals == 1:
                nb_animals_2_owners = 1
            else:
                nb_animals_2_owners = rd.randrange(1,nb_household_animals)
            animals_2_owners = rd.choices(household_micro,k=nb_animals_2_owners)

            for m in animals_2_owners:
                rows.append({
                    'id_microchip': m,
                    'id_owner_tmp': additional_owners[i],
                    'id_household': a,
                    'i':i})
            i+=1
        animal_additional_owner = pd.DataFrame(rows)

        logging.info(f"{len(animal_additional_owner)} animals assigned "
                     f"to more than one owner.")
        return animal_additional_owner

    #----------------------------------------------------------------
    def assign_left_animals(self,
        animal_data: pd.DataFrame,
        animal_owner_data: pd.DataFrame
        ):
        """
            In the case some animals were not assigned to any owner,
            assign them randomly to a owner.
        """
        logging.info("Start assigning left animals to owners.")
        microchip_id_not_assigned = np.setdiff1d(
            animal_data['id_microchip'].unique(), 
            animal_owner_data['id_microchip'].unique()
        )
        if len(microchip_id_not_assigned) == 0:
            raise ValueError("All animals were assigned to owners.")
        else:
            rows=[]
            for element in microchip_id_not_assigned:
                rnd_id_owner = int(rd.choice(animal_owner_data['id_owner_tmp'].unique()))
                rows.append({
                    'id_microchip': int(element),
                    'id_owner_tmp': rnd_id_owner,
                    'id_household': None,
                    'i': None
                })
            
            left_animal_owner = pd.DataFrame(rows)
            logging.info(f"{len(left_animal_owner)} new animals assigned "
                     f"to owners.")
            return left_animal_owner
        
    #----------------------------------------------------------------
    def assign_id_owner(self,
        owner_data: pd.DataFrame,
        pk_column_name: str = 'id_owner',
        starting_id_value: int = 1,
        ):
        """
            Creates a new column ('id_owner') to the DataFrame
            owner_data to serve as main identifier. 
            Returns the DataFrame with the newly added id.
            Order of rows does not matter, no need to sort first.
        """
        logging.info(f"Adding PK id_owner to relation Owner.")
        owner_data = add_primary_key_values(
            relation=owner_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        self.owner_data = owner_data
        return owner_data

    #----------------------------------------------------------------
    def assign_animal_owner_id_owner(self,
        animal_owner_data: pd.DataFrame,
        owner_data: pd.DataFrame = None
        ):
        """
            Creates a new column ('id_microchip') to the DataFrame
            (animal_data) which represents the relation's foreign key
            towards relation microchip, and fill it with the values of
            id_microchip from DataFrame microchip_data associated to
            the animals through values of attribute 'id_tmp'.
            Returns DataFrame microchip_data with newly added and filled
            foreign key column
    
        """
        logging.info("Adding FK id_owner to relation Animal_Owner.")
        if owner_data is None or len(owner_data) == 0:
            owner_data = self.owner_data

        animal_owner_data = animal_owner_data.merge(
            owner_data[['id_owner_tmp', 'id_owner']],
            on='id_owner_tmp',
            how='left'
        )

        self.animal_owner_data = animal_owner_data
        return animal_owner_data

    #----------------------------------------------------------------
    def assign_id_animal_owner(self,
        animal_owner_data: pd.DataFrame,
        pk_column_name: str = 'id_animal_owner',
        starting_id_value: int = 1
        ):
        """
            Creates a new column ('id_animal_owner') to the DataFrame
            animal_owner_data to serve as main identifier. 
            Returns the DataFrame with the newly added id.
            Order of rows does not matter, no need to sort first.
        """
        logging.info("Adding FK id_animal to relation Animal_Owner.")
        animal_owner_data = animal_owner_data.sort_values(
            ['id_microchip', 'id_owner']
        ).reset_index(drop=[True, True])

        animal_owner_data = add_primary_key_values(
            relation=animal_owner_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        return animal_owner_data


        