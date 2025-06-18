import pandas as pd
import numpy as np
import random as rd
from datetime import date, timedelta
from faker import Faker
fake = Faker()
from shared_functions import (
    add_primary_key_values,
    generate_random_dates)
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="- %(levelname)s - %(asctime)s - %(message)s"
)

class Animal():
    """
        This class is used to create the relation animal
        and progressively generate the required data.
        Animals are referred to as "patients".
        The self contained DataFrame animal_data is updated
        as data generation progresses.
    """
    def __init__(self,
                base_animal_data: pd.DataFrame,
                nb_animals: int,
                clinic_start_year: int = 2015,
                last_operation_date: date = date.today(),
                initial_augmentation_seed: int = None):
        """
            Initialize the instance with the data contained in csv
            file animal_list.csv included in the package, and the
            desired number of unique animals to be represented
            in the clean database.
            The breeds represented in the base data correspond to
            the breeds for which a weight range was specified in csv
            files cat_breed_weight_range and dog_breed_weight_range
            also included in package. If the breeds are modified in
            the base file, then these two files should be modified
            accordingly to document the weight range of added breeds.
        """
        logging.info("Instantiating object from class Animal")

        nb_base_animals = len(base_animal_data)
        if nb_base_animals == 0:
            raise ValueError(f"The initial dataframe of base animals'"
                             f"profile cannot be empty.")
        if nb_animals < 100:
            raise ValueError(f"The number of animals should be"
                             f"at least 100.")
        if clinic_start_year < 2000 | clinic_start_year > date.today().year:
            raise ValueError(f"The clinic opening year should be between'"
                             f"2000 and the current year.")
        if last_operation_date.year - clinic_start_year < 5:
            raise ValueError(f"The clinic should have been opened for "
                             f"at leat five years. Please modify "
                             f"clinic_start_year or last_operation_date")

        self.nb_animals = nb_animals
        self.clinic_start_year = clinic_start_year
        self.last_operation_date = last_operation_date

        nb_animal_base = nb_base_animals
        if nb_animals <= nb_animal_base:
            animal_data = base_animal_data.sample(
                n=nb_animals,
                replace=True,
                random_state=augmentation_seed
            )
        else:
            animal_additional = nb_animals - nb_animal_base
            duplicated_animal_data = base_animal_data.sample(
                n=animal_additional,
                replace=True,
                random_state=initial_augmentation_seed
            )
            animal_data = pd.concat(
                [base_animal_data, duplicated_animal_data],
                ignore_index=True
            )
        self.animal_data = animal_data

    #----------------------------------------------------------------    
    def generate_animal_data_df(self):
        """
            Return the current state of the DataFrame animal_data
        """
        logging.info("Return the Animal relation's data")
        return self.animal_data

    #----------------------------------------------------------------
    def assign_date_of_birth(self,
        animal_data: pd.DataFrame = None,
        prop_born_before_opening: float = 0.3,
        max_animal_age_at_opening: int = 10
        ):
        """
            Creates a new column ('dob') to the dataframe (animal_data)
            and fill it to randomly generated dates of birth, following
            a certain distribution that respects the rules:
            1- most patients should start their medical care at the clinic (i.e
                they are born after the openign of the clinic)
            2- acquired patients born before the opening of the clinic should 
                mainly be young: the distribution of patients is decreasing with
                their age at the time of opening, which reflect that pets' owners
                tend to stick more to one clinic while their pets are ageing,
                but are more inclined to change when their pets are younger
            3- 
            The proportion of patients born before the clinic's opening
            year and the maximal age of the patients at the opening
            are required.
            Returns the dataset with newly created and filled column 'dob'.
        """
        logging.info("Start assigning date of births to the animals")

        if animal_data is None or len(animal_data) < self.nb_animals:
            animal_data = self.animal_data

        if prop_born_before_opening >= 0.5:
            raise ValueError(f"The proportion of patients born before'"
                             f"the clinic's opening should be less than 50%.")

        animal_data['dob'] = None
        nb_animals_born_before_opening = int(self.nb_animals*prop_born_before_opening)
        nb_animals_born_after_opening = self.nb_animals - nb_animals_born_before_opening

        indices_animals_born_before_opening = np.random.choice(
            animal_data.index,
            size=nb_animals_born_before_opening,
            replace=False
        )
        indices_animals_born_after_opening = np.setdiff1d(
            animal_data.index,
            indices_animals_born_before_opening
        )

        def distribute_quantity_per_year_progressive(
                min_year: int,
                max_year: int,
                nb_elements: int):
            """
                This nested function distribute the total number of elements
                between years in a period of time. It returns the list of years
                and the list of number of elements per year.
                It is used to distribute the number of patients born
                each year of a period between two specified years.
            """
            years = list(range(min_year, max_year))
            years_range = np.arange(1, len(years))
            weights = years_range / years_range.sum()
            counts = (weights * nb_elements).astype(int)
            while counts.sum() < nb_elements:
                counts[np.argmax(weights)] += 1
            return zip(years, counts)

        # Distibute the number of animals born before the opening per year
        # between the minimal dob and the opening's year
        logging.info("Define the distribution of animals per year of birth")
        before_years_counts= distribute_quantity_per_year_progressive(
            min_year=self.clinic_start_year-max_animal_age_at_opening,
            max_year=self.clinic_start_year,
            nb_elements=nb_animals_born_before_opening)
    
        def generate_random_dates_following_yearly_distribution(
                years_counts: zip,
                max_date: date
            ):
            """
                This function is used to generate a list of random
                dates in a specific period of time, respecting a
                given yearly distribution, ensure that none is higher
                than a spcified max date.
            """
            generated_dates = []
            for year, count in years_counts:
                start_date = date(year, 1, 1)
                end_date = date(year + 1, 1, 1) if year < max_date.year else max_date
                days_range = (end_date - start_date).days
                year_dates = [start_date + timedelta(
                    days=rd.randrange(days_range)) for _ in range(count)
                ]
                generated_dates.extend(year_dates)
            rd.shuffle(generated_dates)
            return generated_dates

        # Randomly generate date of births for each year in range
        logging.info("Randomly generate dates of births for animals born before the clinic opened")
        generated_dob_before_opening = generate_random_dates_following_yearly_distribution(
            years_counts = before_years_counts,
            max_date = self.last_operation_date
        )
        
        animal_data.loc[
            indices_animals_born_before_opening,
            'dob'
            ] = generated_dob_before_opening
        # Randomly generate date of births after opening
        logging.info("Randomly generate dates of births for animals born after the clinic opened")
        generated_dob_after_opening = generate_random_dates(
            min_date = date(self.clinic_start_year, 1, 1),
            max_date = self.last_operation_date,
            n = nb_animals_born_after_opening
        )
        animal_data.loc[
            indices_animals_born_after_opening,
            'dob'
            ] = generated_dob_after_opening

        animal_data['dob'] = pd.to_datetime(
            animal_data['dob'],
            errors='coerce'
        )

        logging.info(f"Generated Animal relation of size {len(animal_data)}")
        self.animal_data = animal_data
        return animal_data

    #----------------------------------------------------------------
    def assign_hash_id(self,
        animal_data: pd.DataFrame = None
        ):
        """
            Creates a new column ('hash_id') to the dataframe (animal_data)
            and fill it with randomly generated hash values using package Faker.
        """
        logging.info("Adding SK hash_id to relation Animal")
        if animal_data is None or len(animal_data) < self.nb_animals:
            animal_data = self.animal_data
        hashes = []
        for _ in range(self.nb_animals):
            hashes.append(
                fake.uuid4()
            )
        animal_data['hash_id'] = np.array(hashes)
        return animal_data
    
    #----------------------------------------------------------------
    def assign_tmp_id(self,
        animal_data: pd.DataFrame = None,
        pk_column_name='id_tmp',
        starting_id_value=1
        ):
        """
            Creates a new column ('id_tmp') to the dataframe (animal_data)
            to store an incremental temporary id to be used during 
            data generation until the final id (id_animal) is created.
            Return the dataframe with newly cretaed and filled 'id_tmp'.
            This id_temp will be used to associate microchip and appointments
            to animals, his role is crucial in maintening integrity
            throughout the data genration process.
        """
        logging.info("Adding temporary id to relation Animal")
        if animal_data is None or len(animal_data) < self.nb_animals:
            animal_data = self.animal_data
        animal_data = add_primary_key_values(
            relation=animal_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        self.animal_data = animal_data
        return animal_data

    #----------------------------------------------------------------
    def sort_animal_by_appt_date(self,
        appointment_data: pd.DataFrame,
        animal_data: pd.DataFrame = None
        ):
        """
            Sorts rows of the DataFrame animal_data in ascending
            order of first appointment data, by joigning it to working
            DataFrame appointment_data containing the attribute
            'appt_date_1'. Used to sort rows in animal_data before
            assigning them values of final id id_animal.
            Returns DataFrame animal_data sorted.
        """
        logging.info("Sorting tuples of relation Animal by appointment date.")
        if animal_data is None or len(animal_data) < self.nb_animals:
            animal_data = self.animal_data
        
        if 'id_tmp' not in animal_data.columns:
            raise KeyError(f"Required column 'id_tmp' not found"
                           f"in DataFrame 'appointment_data'")

        animal_columns = animal_data.columns
        animal_data_tosort = animal_data.merge(
            appointment_data[['id_tmp', 'appt_date']],
            on=['id_tmp'],
            how='left'
        )
        animal_data_tosort = animal_data_tosort.sort_values(
            ['appt_date', 'id_tmp']
        ).reset_index(drop=[True, True])
        animal_data = animal_data_tosort[animal_columns].drop_duplicates().reset_index(drop=True)

        self.animal_data = animal_data
        return animal_data

#----------------------------------------------------------------
    def assign_id_microchip(self,
        microchip_data: pd.DataFrame,
        animal_data: pd.DataFrame = None
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
        logging.info("Adding FK id_microchip to relation Animal")
        if animal_data is None or len(animal_data) < self.nb_animals:
            animal_data = self.animal_data

        if len(microchip_data) != len(animal_data):
            raise ValueError("DataFrames microchip_data and animal_data "
                             f"must be of the same size")

        animal_data = animal_data.merge(
            microchip_data[['id_tmp', 'id_microchip']],
            on='id_tmp',
            how='left'
        )

        self.animal_data = animal_data
        return animal_data

    #----------------------------------------------------------------
    def assign_id_animal(self,
        animal_data: pd.DataFrame = None,
        pk_column_name: str = 'id_animal',
        starting_id_value: int = 1,
        ):
        """
            Creates a new column ('id_animal') to the DataFrame animal_data
            to serve as main identifier. Returns the DataFrame with the
            newly added id.
            The DataFrame animal_data should be sorted by appointment date 
            before - see function 'sort_animal_by_appt_date'.
        """
        logging.info("Adding PK id_animal to relation Animal")
        if animal_data is None or len(animal_data) < self.nb_animals:
            animal_data = self.animal_data

        animal_data = add_primary_key_values(
            relation=animal_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        self.animal_data = animal_data
        return animal_data


#================================================================
class AnimalWeigth():
    """
        This class is used to create the relation animal_weight
        and progressively generate the required data.
        The self contained DataFrame animal_weight_data is updated
        as data generation progresses.
    """
    def __init__(self,
                animal_data: pd.DataFrame,
                appointment_data: pd.DataFrame,
                cat_breed_weight_range: pd.DataFrame,
                dog_breed_weight_range: pd.DataFrame):
        """
            Initialize the instance with the previouly generated
            DataFrames animal_data and appointment_data and the
            weight ranges for all represented breeds (csv files
            cat_breed_weight_range and dog_breed_weight_range
            included in package).
        """
        logging.info("Instantiating object from class AnimalWeight")

        if len(np.setdiff1d(
            ['id_animal', 'species', 'breed', 'gender'], 
            animal_data.columns)) > 0:
            raise KeyError(f"Required columns 'id_animal', 'species',"
                           f"'breed', 'gender' not found in DataFrame"
                           f"'animal_data'")
        if len(np.setdiff1d(
            ['id_animal', 'id_appointment', 'appt_date'], 
            appointment_data.columns)) > 0:
            raise KeyError(f"Required columns 'id_animal', 'id_appointment',"
                           f"'appt_date' not found in DataFrame"
                           f"'appointment_data'")
        if len(animal_data) < 100:
            raise ValueError(f"The number of rows of DataFrame animal_data "
                             f"should be at least 100.")
        if len(appointment_data) < 100:
            raise ValueError(f"The number of rows of DataFrame appointment_data "
                             f"should be at least 100 (at least one appointment "
                             f"per animal).")

        self.initial_weight_data = animal_data[
            ['id_animal', 'species', 'breed', 'gender']
        ]
        self.appointment_data = appointment_data[
            ['id_animal', 'id_appointment', 'appt_date']
        ].sort_values(['appt_date', 'id_animal']).reset_index(drop=[True, True])
        self.animal_weight_data = []

    #----------------------------------------------------------------    
    def generate_animal_weight_data_df(self):
        """
            Return the current state of the DataFrame animal_weight_data
        """
        logging.info("Return the Animal_weight relation's data")
        return self.animal_weight_data

    #----------------------------------------------------------------
    def assign_initial_weight_to_animals(self,
        cat_breed_weight_range: pd.DataFrame,
        dog_breed_weight_range: pd.DataFrame
        ) -> pd.DataFrame:
        """
            Creates a new column ('weight') to the DataFrame initial_weight_data
            and fill it with randomly selected weight value between weight
            range associated to each animal's breed and gender.
            Return the DataFrame initial_weight_data with newly added and
            filled column 'weight'.
            Note that the weight ranges correspond to adult weight ranges,
            and that the assignment of weight does not take into consideration
            the age of the animal, thus these synthetic values do not
            necessarily reflect observed trends in real-life.
        """
        logging.info(f"Adding initial weight value to all animals "
                     f"in relation Animal, corresponding to their weight, "
                     f"during their first visit.")

        initial_weight_data = self.initial_weight_data.copy()

        is_cat = initial_weight_data["species"] == "feline"
        is_dog = initial_weight_data["species"] == "canine"

        cats_df = initial_weight_data[is_cat].copy()
        dogs_df = initial_weight_data[is_dog].copy()

        cats_merged = cats_df.merge(
            cat_breed_weight_range,
            on="breed",
            how="left",
            suffixes=("", "_breed")
        )
        dogs_merged = dogs_df.merge(
            dog_breed_weight_range,
            on="breed",
            how="left",
            suffixes=("", "_breed")
        )

        if cats_merged["male_min_weight"].isna().any() or cats_merged["female_min_weight"].isna().any():
            missing = cats_merged[cats_merged["male_min_weight"].isna() & cats_merged["female_min_weight"].isna()]["breed"].unique()
            raise ValueError(f"No weight‐range data found for cat breed(s): {missing.tolist()}")

        if dogs_merged["male_min_weight"].isna().any() or dogs_merged["female_min_weight"].isna().any():
            missing = dogs_merged[dogs_merged["male_min_weight"].isna() & dogs_merged["female_min_weight"].isna()]["breed"].unique()
            raise ValueError(f"No weight‐range data found for dog breed(s): {missing.tolist()}")

        def pick_min_max(df: pd.DataFrame) -> pd.DataFrame:
            """
            Given a DataFrame that has:
            - 'gender' in {'M', 'F'}
            - 'male_min_weight', 'male_max_weight', 'female_min_weight', 'female_max_weight'
            Return two new Series:
            - min_w: the appropriate minimum weight for each row
            - max_w: the appropriate maximum weight for each row
            """
            male_mask = df["gender"] == "M"
            female_mask = df["gender"] == "F"

            min_w = np.empty(len(df), dtype=float)
            max_w = np.empty(len(df), dtype=float)

            min_w[male_mask] = df.loc[male_mask, "male_min_weight"].to_numpy(dtype=float)
            max_w[male_mask] = df.loc[male_mask, "male_max_weight"].to_numpy(dtype=float)
            min_w[female_mask] = df.loc[female_mask, "female_min_weight"].to_numpy(dtype=float)
            max_w[female_mask] = df.loc[female_mask, "female_max_weight"].to_numpy(dtype=float)

            if (~(male_mask | female_mask)).any():
                bad_idxs = df.index[~(male_mask | female_mask)].tolist()
                raise ValueError(f"Unexpected gender(s) at index(es): {bad_idxs}")

            return pd.Series(min_w, index=df.index), pd.Series(max_w, index=df.index)

        cat_min_w, cat_max_w = pick_min_max(cats_merged)
        dog_min_w, dog_max_w = pick_min_max(dogs_merged)

        cats_n = len(cats_merged)
        dogs_n = len(dogs_merged)

        rng = np.random.default_rng()

        cat_uniforms = rng.random(cats_n)
        dog_uniforms = rng.random(dogs_n)

        cats_merged["weight"] = np.round(
            cat_min_w + (cat_max_w - cat_min_w) * cat_uniforms, 
            2
        )
        dogs_merged["weight"] = np.round(
            dog_min_w + (dog_max_w - dog_min_w) * dog_uniforms, 
            2
        )

        result = pd.concat([cats_merged, dogs_merged], axis=0)

        result = result.sort_index()
        drop_cols = [
            "male_min_weight", "male_max_weight",
            "female_min_weight", "female_max_weight"
        ]
        initial_weight_data = result.drop(columns=[c for c in drop_cols if c in result.columns])

        logging.info(f"Initial Animal_Weight relation of size {len(initial_weight_data)}")
        self.initial_weight_data = initial_weight_data
        return initial_weight_data

    #----------------------------------------------------------------
    def assign_weight_per_appointment(self,
        initial_weight_data: pd.DataFrame = None
        ):
        """
            From DataFrame initial_weight_data which contains the initial
            weight of each animal (associated to the first appointment), 
            creates a new DataFrame including a weight value for each 
            animal and each appointment. Each weight is randimly selected
            as plus or minus 10% of the weight associated to previous
            appointment.
        """
        logging.info(f"Adding additional weight values to relation "
                     f"Animal_Weight, one value per appointment, "
                     f"to keep it within +/-10% of the weight associated "
                     f"to the previous appointment.")

        if initial_weight_data is None or len(initial_weight_data) == 0:
            initial_weight_data = self.initial_weight_data
        else:
            initial_weight_data = initial_weight_data.copy()

        df = initial_weight_data.merge(
            self.appointment_data,
            on="id_animal",
            how="right"
        )

        df = df.sort_values(["id_animal", "id_appointment"]).reset_index(drop=True)

        first_mask = (df.groupby("id_animal")["id_appointment"]
                        .cumcount() == 0)
        n = len(df)
        rng = np.random.default_rng()
        offsets = rng.uniform(-0.1, 0.1, size=n)
        offsets[first_mask] = 0.0

        df["multiplier"] = 1.0 + offsets

        df["initial_wt"] = df.groupby("id_animal")["weight"].transform("first")

        df["cum_multiplier"] = df.groupby("id_animal")["multiplier"].cumprod()
        df["weight"] = np.round(df["initial_wt"] * df["cum_multiplier"], 2)

        out = df.drop(columns=["multiplier", "initial_wt", "cum_multiplier"])

        logging.info(f"Final Animal_weight relation of size {len(out)}")
        self.animal_weight_data = out
        return out

    #----------------------------------------------------------------
    def assign_id_weight(self,
        animal_weight_data: pd.DataFrame = None,
        pk_column_name: str = 'id_weight',
        starting_id_value: int = 1
        ):
        """
            Creates a new column ('id_weight') to the DataFrame
            animal_weight_data to serve as main identifier.
            Returns the DataFrame with the newly added id.
        """
        logging.info("Adding PK id_weight to relation Animal_Weight")
        if animal_weight_data is None or len(animal_weight_data) == 0:
            animal_weight_data = self.animal_weight_data

        if 'id_appointment' not in animal_weight_data.columns:
            raise KeyError(f"Required column 'id_appointment' not found"
                           f"in DataFrame 'animal_weight_data'")

        animal_weight_data = animal_weight_data.sort_values(
            'id_appointment'
        ).reset_index(drop=True)

        animal_weight_data = add_primary_key_values(
            relation=animal_weight_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        self.animal_weight_data = animal_weight_data
        return animal_weight_data


