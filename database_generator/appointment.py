import pandas as pd
import numpy as np
import random as rd
import holidays
import warnings
from datetime import date, timedelta
from collections import defaultdict
from shared_functions import add_primary_key_values, get_country_holidays
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="- %(levelname)s - %(asctime)s - %(message)s"
)

class Appointment:
    """
        This class is used to create the relation appointment,
        and progressively generate the required data. 
        First, a denormalised DataFrame is created, which size
        is the number of animals and where all appointments associated
        to one animal are stored on one row. From this DataFrame is
        created the normalised DataFrame in which each row is associated
        to only one appointment. Its size is the total number of
        appointments.
        The self contained DataFrames appointment_data_denorm and 
        appointment_data are updated as data generation progresses.
    """
    def __init__(self,
                microchip_data: pd.DataFrame,
                clinic_start_year: int = 2015,
                last_operation_date: date = date.today(),
                life_expectancy: int = 15
                ):
        """
            Initialize the instance with the previously generated
            DataFrame microchip_data and the same values for attributes
            clinic_start_year and last_operation_date as specified
            when initializing class Animal, and the life expectancy
            of patients, to be used to assign a number of appointment
            to each patient.
        """
        logging.info("Instantiating object from class Appointment")

        if len(np.setdiff1d(
            ['id_tmp', 'dob', 'implant_date'], 
            microchip_data.columns)) > 0:
            raise KeyError(f"Required columns 'id_tmp', 'dob',"
                           f"'implant_date' not found in DataFrame"
                           f"'microchip_data'")

        self.appointment_data_denorm = microchip_data[['id_tmp', 'dob', 'implant_date']]
        logging.info(f"Denormalized Appointment relation of size "
                     f"{len(self.appointment_data_denorm)}.")
        self.appointment_data = []
        self.last_operation_date = last_operation_date
        self.clinic_start_year = clinic_start_year
        self.date_opening = date(self.clinic_start_year, 1, 1)
        self.life_expectancy = life_expectancy

    #---------------------------------------------------------------- 
    def generate_appointment_data_denorm_df(self):
        """
            Return the current state of the DataFrame appointment_data_denorm
        """
        logging.info(f"Return the data of the denormalized version "
                     f"relation Appointment")
        return self.appointment_data_denorm

    #---------------------------------------------------------------- 
    def generate_appointment_data_df(self):
        """
            Return the current state of the DataFrame appointment_data
        """
        logging.info(f"Return the data of the normalized version "
                     f"relation Appointment")
        return self.appointment_data

    #---------------------------------------------------------------- 
    def assign_nb_appointments(self,
            appointment_data_denorm: pd.DataFrame = None
        ):
        """
            Creates a new column ('nb_appointment') to the DataFrame
            appointment_data_denorm to indicate how many appointments
            will be added to the relation appointment for each patient.
            The number of appointment is assigned based on the patient's
            date of birth, the clinic's opening year and the last
            appointment date. Returns the DataFrame appointment_data_denorm
            will newly added attribute 'nb_appointment'.
        """
        logging.info(f"Start assigning a number of appointments to all "
                     f"animals, based on their date of birth, clinic's "
                     f"opening year and last appointment date.")

        if appointment_data_denorm is None:
            appointment_data_denorm = self.appointment_data_denorm

        def define_nb_appointment_animal(dob):
            """
                Assign a number of appointments based on the date of
                birth of the patient, while considering also the 
                opening year of the clinic and the date of the last
                appointment.
            """
            if pd.isnull(dob):
                raise ValueError(f"Date of birth '{dob}' is null")
            last_date = pd.Timestamp(self.last_operation_date)
            start_date = max(dob, pd.Timestamp(self.date_opening))
            end_date = min(dob + pd.DateOffset(years=self.life_expectancy), last_date)
            years = (end_date - start_date).days / 365.25
            app_range = int(years * 2)
            nb_app = rd.randrange(app_range) if app_range > 0 else 1
            return nb_app if nb_app > 0 else 1

        appointment_data_denorm['nb_appointment'] = appointment_data_denorm['dob'].apply(
            define_nb_appointment_animal
        )

        self.appointment_data_denorm = appointment_data_denorm

        return appointment_data_denorm

    #---------------------------------------------------------------- 
    @staticmethod
    def _assign_visit_reason(
        dataframe: pd.DataFrame,
        attribute: str,
        indices_list: list,
        prop_dict: dict
        ):
        """
            Fill values of a column (attribute) to the input DataFrame
            (dataframe) to rows associated to specified indices
            (indices_list). Values are selected from the keys of 
            dictionnary (prop_dict) and assigned following the distribution
            specified in the values of the dictionnary.
        """
## CONTROLER LE DICTIONNAIRE POUR VERIFIER LES CATEGORIES & PROP

        shuffled_indices = np.random.permutation(indices_list)
        total = len(shuffled_indices)
        counts = {k: int(v * total) for k, v in prop_dict.items()}

        remaining = total - sum(counts.values())
        if remaining > 0:
            max_cat = max(counts, key=counts.get)
            counts[max_cat] += remaining
        
        start = 0
        category_indices = {}
        for category, count in counts.items():
            category_indices[category] = shuffled_indices[start:start + count].tolist()
            start += count
        
        for category, idx_list in category_indices.items():
            dataframe.loc[idx_list, attribute] = category

        return dataframe

    #---------------------------------------------------------------- 
    def assign_first_appointment_reason(self,
        appointment_data_denorm: pd.DataFrame = None,
        distribution_reason_microchipped_before_opening: dict = None,
        distribution_reason_microchipped_after_opening: dict = None
        ):
        """
            Creates a new column ('appt_reason_1') to the DataFrame
            appointment_data_denorm and fill it with randomly
            selected appointment reasons following specified distributions
            in input dictionnaries. Distributions for patients
            microchipped before the opening of the clinic (i.e. those
            who are already patients in other clinics) and those
            who got microchipped after the opening (i.e. those who might 
            only be patient in Perfect Pet) are different.
        """
        logging.info(f"Start assigning the reason for the first "
                     f"appointment for every animal.")
        if appointment_data_denorm is None or len(appointment_data_denorm) == 0:
            appointment_data_denorm = self.appointment_data

        if not distribution_reason_microchipped_before_opening:
            distribution_reason_microchipped_before_opening = {
                'annual_visit':0.6,
                'sick_pet':0.2,
                'injured_pet':0.2
            }
        
        if not distribution_reason_microchipped_after_opening:
            distribution_reason_microchipped_after_opening = {
                'initial_visit':0.4,
                'annual_visit':0.15,
                'sick_pet':0.2,
                'injured_pet':0.15,
                'surgery':0.1,
            }

        indices_microchip_before_opening = appointment_data_denorm[
            appointment_data_denorm['implant_date']<pd.Timestamp(self.date_opening)
            ].index
        
        indices_microchip_after_opening = appointment_data_denorm[
            appointment_data_denorm['implant_date']>=pd.Timestamp(self.date_opening)
            ].index

        appointment_data_denorm['appt_reason_1'] = None

        appointment_data_denorm = self._assign_visit_reason(
            appointment_data_denorm,
            'appt_reason_1',
            indices_microchip_before_opening,
            distribution_reason_microchipped_before_opening
        )

        appointment_data_denorm = self._assign_visit_reason(
            appointment_data_denorm,
            'appt_reason_1',
            indices_microchip_after_opening,
            distribution_reason_microchipped_after_opening
        )

        self.appointment_data_denorm = appointment_data_denorm
        return appointment_data_denorm

    #---------------------------------------------------------------- 
    def assign_first_appointment_date(self,
        appointment_data_denorm: pd.DataFrame = None
        ):
        """
            Creates a new column ('appt_date_1') to the DataFrame
            appointment_data_denorm to indicate the date of the first
            appointment for every patient. The assignment of the date
            is based on the first appointment reason, the total number
            of appointment, and the time range (from the clinic's 
            opening date to the last appointment date).
            Returns the DataFrame with the newly added and filled column.

        """
        logging.info(f"Start assigning the first appointment date "
                     f"for every animal based on its reason.")

        if appointment_data_denorm is None or len(appointment_data_denorm) == 0:
            appointment_data_denorm = self.appointment_data

        if len(np.setdiff1d(
            ['nb_appointment', 'appt_reason_1'], 
            appointment_data_denorm.columns)) > 0:
            raise KeyError(f"Required columns 'nb_appointment', "
                           f"'appt_reason_1' not found in DataFrame"
                           f"'appointment_data_denorm'")

        appointment_data_denorm['appt_date_1'] = None

        def assign_first_visit_date(
            implant_date: pd.Timestamp,
            nb_visits: int,
            clinic_start_year: int,
            last_operation_date: date
            ) -> pd.Timestamp:
            """
            """
            start_date = date(clinic_start_year, 1, 1)
            if pd.isnull(implant_date):
                raise ValueError(f"Microchip implant date is null")
            if pd.isnull(nb_visits) or nb_visits <= 0:
                raise ValueError(f"Invalid number of visits: {nb_visits}")
            min_date = max(implant_date, pd.Timestamp(start_date))
            end_date = pd.Timestamp(last_operation_date)
            max_days = (end_date - min_date).days
            divider = max(1,nb_visits)
            interval = max(1, int(max_days / divider))
            offset_days = rd.randrange(interval)
            return min_date + pd.Timedelta(days=offset_days)

        non_initial_visit_tuples = appointment_data_denorm[
            appointment_data_denorm['appt_reason_1'] != 'initial_visit'
            ]
        appointment_data_denorm['appt_date_1'] = non_initial_visit_tuples.apply(
                lambda row: assign_first_visit_date(row['implant_date'], 
                                                    row['nb_appointment'],
                                                    self.clinic_start_year,
                                                    self.last_operation_date),
                axis=1
            )

        initial_visit_tuples = appointment_data_denorm[
            appointment_data_denorm['appt_reason_1'] == 'initial_visit'
            ]
        for row in initial_visit_tuples.itertuples():
            appointment_data_denorm.loc[row.Index, 'appt_date_1'] = row.implant_date

        self.appointment_data_denorm = appointment_data_denorm
        return appointment_data_denorm

    #---------------------------------------------------------------- 
    def assign_appointment_details(self,
        appointment_data_denorm: pd.DataFrame = None,
        prop_visit_non_followup: dict = None,
        perc_followup: float = 0.5
        ):
        """
            Assign the date and reason of all following appointments
            for every patient. Two columns are created for every xth
            appointment: one for the date and one for the reason.
            the total number of columns created is 
            max(appointment_data_denorm['nb_appointments'])*2.
            The columns will contain null values for animals with
            nb_appointments lower than x.
        """
        logging.info(f"Start assigning for every animal associated "
                     f"to more than one appointment, the reason and "
                     f"date for all subsequent appointments.")

        if appointment_data_denorm is None or len(appointment_data_denorm) == 0:
            appointment_data_denorm = self.appointment_data

        if prop_visit_non_followup is None:
            prop_visit_non_followup = {
                'annual_visit':0.5,
                'sick_pet':0.2,
                'injured_pet':0.2,
                'surgery':0.1
            }

        if len(np.setdiff1d(
            ['nb_appointment', 'appt_reason_1', 'appt_reason_1'], 
            appointment_data_denorm.columns)) > 0:
            raise KeyError(f"Required columns 'nb_appointment', 'appt_reason_1'"
                           f"'appt_reason_1' not found in DataFrame"
                           f"'appointment_data_denorm'")

        n=2
        max_n=max(appointment_data_denorm['nb_appointment'].tolist())
        perc_non_followup=1-perc_followup

        def assign_followup_visit_date(
            last_visit_date: pd.Timestamp,
            max_date: date,
            min_day: int,
            max_day: int) -> pd.Timestamp:
            """
                Assign a follow-up appointment date based on the date
                of the previous appointment
            """
            if pd.isnull(last_visit_date):
                raise ValueError(f"Last visit date is null")
            if pd.isnull(min_day) or min_day <= 0:
                raise ValueError(f"Invalid minimum day: {min_day}")
            if pd.isnull(max_day) or max_day <= 0:
                raise ValueError(f"Invalid maximum day: {max_day}")
            end_date = pd.Timestamp(max_date)
            revised_max_day=min(max_day, (end_date-last_visit_date).days)
            days_range=max(1,revised_max_day-min_day)
            offset_days = rd.randrange(days_range)
            return last_visit_date + pd.Timedelta(days=min_day + offset_days)

        def assign_random_visit_date(
            last_visit_date: pd.Timestamp,
            max_date: date,
            nb_visits_remaining: int
            ) -> pd.Timestamp:
            """
                Assign a non-follow-up appointment date based on the date
                of the previous appointment
            """
            if pd.isnull(last_visit_date):
                raise ValueError("Last visit date is null")
            if pd.isnull(nb_visits_remaining) or nb_visits_remaining < 0:
                raise ValueError(f"Invalid number of visits remaining: {nb_visits_remaining}")
            end_date = pd.Timestamp(max_date)
            max_days = (end_date - last_visit_date).days
            if max_days <= 0:
                raise ValueError(f"Last visit date {last_visit_date} is in the future of 31/12/2024")
            divider = max(nb_visits_remaining,1)
            interval = max(1, int(max_days / divider))
            offset_days = rd.randrange(interval)
            return last_visit_date + pd.Timedelta(days=offset_days)
        
        while n <= max_n:
            indices = appointment_data_denorm[appointment_data_denorm['nb_appointment']>=n].index
            if len(indices) > 0:
                reason_attribute = 'appt_reason_' + str(n)
                date_attribute = 'appt_date_' + str(n)
                appointment_data_denorm[reason_attribute] = None
                appointment_data_denorm[date_attribute] = None
                last_reason_attribute = 'appt_reason_' + str(n-1)
                last_date_attribute = 'appt_date_' + str(n-1)

                indices_last_visit_surgery = appointment_data_denorm.loc[indices].loc[
                    appointment_data_denorm[last_reason_attribute] == 'surgery'
                    ].index

                indices_last_visit_sickness_injury = appointment_data_denorm.loc[indices].loc[
                    appointment_data_denorm[last_reason_attribute].isin(['sick_pet','injured_pet'])
                    ].index

                indices_last_visit_sickness_injury_list = indices_last_visit_sickness_injury.tolist()
                selected_indices_last_visit_sickness_injury = rd.sample(
                    indices_last_visit_sickness_injury_list,
                    round(perc_followup * len(indices_last_visit_sickness_injury_list))
                )

                other_indices = list(
                    set(indices) -
                    set(indices_last_visit_surgery.tolist() + selected_indices_last_visit_sickness_injury)
                )
            
                ### assign visit reasons ###
                appointment_data_denorm.loc[indices_last_visit_surgery,reason_attribute] = 'follow_up_surgery'
                appointment_data_denorm.loc[selected_indices_last_visit_sickness_injury,reason_attribute] = 'follow_up'
                
                appointment_data_denorm=self._assign_visit_reason(
                    appointment_data_denorm,
                    reason_attribute,
                    other_indices,
                    prop_visit_non_followup
                )

                ### assign visit date ###
                # assign visit dates to appointments for follow-up surgery
                surgery_assigned_dates = appointment_data_denorm.loc[
                    indices_last_visit_surgery
                    ].apply(lambda row: assign_followup_visit_date(
                        last_visit_date=row[last_date_attribute],
                        max_date=self.last_operation_date,
                        min_day=3,
                        max_day=15
                    ),
                    axis=1
                )
                appointment_data_denorm.loc[
                    surgery_assigned_dates.index,
                    date_attribute
                ] = surgery_assigned_dates

                # assign visit dates to appointments for follow-up after injury of sickness
                sick_injured_assigned_dates = appointment_data_denorm.loc[
                    selected_indices_last_visit_sickness_injury
                    ].apply(lambda row: assign_followup_visit_date(
                        last_visit_date=row[last_date_attribute], 
                        max_date=self.last_operation_date,
                        min_day=7, 
                        max_day=28
                    ),
                    axis=1
                )
                appointment_data_denorm.loc[
                    sick_injured_assigned_dates.index,
                    date_attribute
                ] = sick_injured_assigned_dates
                
                # assign visit dates to other appointments
                other_assigned_dates = appointment_data_denorm.loc[other_indices
                    ].apply(lambda row: assign_random_visit_date(
                        last_visit_date=row[last_date_attribute],
                        max_date=self.last_operation_date,
                        nb_visits_remaining=row['nb_appointment']-n
                    ),
                    axis=1
                )
                appointment_data_denorm.loc[other_assigned_dates.index, date_attribute] = other_assigned_dates
                n=n+1

        self.appointment_data_denorm = appointment_data_denorm
        return appointment_data_denorm

    #---------------------------------------------------------------- 
    def reformatting_appointment_df(self,
        appointment_data_denorm: pd.DataFrame = None
        ):
        """
            Transform the format of appointment_data_denorm to
            normalize it by placing every appointment on a 
            single row in a new DataFrame with model:
            'id_tmp','appt_date','appt_reason'
            Returns DataFrame appointment_data.
        """
        logging.info(f"Transforming the Appointment relation's structure "
                     f"to normalise it so that it matches the clean "
                     f"database schema (instead of keeping one tuple "
                     f"per animal with all appointments lined up on this "
                     f"tuple, each appointment is moved to one unique "
                     f"tuple, and so one animal can be associated to "
                     f"multiple tuples).")

        if appointment_data_denorm is None or len(appointment_data_denorm) == 0:
            appointment_data_denorm = self.appointment_data

        appointment_col = ['id_tmp','appt_date','appt_reason']
        max_n=max(appointment_data_denorm['nb_appointment'].tolist())

        appointment_data = pd.DataFrame(
            columns=appointment_col
        )

        for i in range(1,max_n+1):
            reason_attribute = 'appt_reason_' + str(i)
            date_attribute = 'appt_date_' + str(i)
            appt_df = appointment_data_denorm[
                ~appointment_data_denorm[date_attribute].isna()
                ][['id_tmp',date_attribute,reason_attribute]]
            appt_df.columns = appointment_col
            
            appointment_data=pd.concat(
                [appointment_data, appt_df],
                ignore_index=True
            )

        logging.info(f"Reformated and normalized Appointment relation "
                     f"of size {len(appointment_data)}.")
        self.appointment_data = appointment_data
        return appointment_data

    #---------------------------------------------------------------- 
    #@staticmethod
    def correction_appt_date_daysoff(self,
        appointment_data: pd.DataFrame,
        country_code: str = 'JO',
        weekly_days_off: list = [4]
        ) -> pd.DataFrame:
        """
            Modify from DataFrame appointment_data the dates (appointments)
            associated to days off or public holidays in a specified
            country. By default, the day off is set to Friday (4) and 
            the country_code is set to 'JO' for Jordan.
        """
        logging.info(f"Adjusting appointment dates to comply with "
                     f"days off and public holidays in {country_code}.")
        years = appointment_data['appt_date'].apply(lambda x: x.year).unique()
        jordan_holidays = get_country_holidays(
            years = years,
            country_code = country_code
        )

        def adjust_date(
            date: date,
            country_holidays,
            weekly_days_off: list = 4
            ):
            """
                Modifies a date corresponding to a public holiday (from a specified
                list of public holidays) or a specified day off, and oush it to
                another valid holiday.
            """
            while date.weekday() in weekly_days_off or date in country_holidays:
                days_until_monday = (7 - date.weekday()) % 7
                start_of_next_week = date + timedelta(days=days_until_monday)
                working_days = [i for i in range(7) if i not in weekly_days_off]
                valid_days = [start_of_next_week + timedelta(days=i) for i in working_days]
                date = rd.choice(valid_days)
            return date

        appointment_data['appt_date'] = appointment_data.apply(
            lambda row: adjust_date(
                date = row['appt_date'],
                country_holidays = jordan_holidays,
                weekly_days_off = weekly_days_off
            ),
            axis = 1)

        self.appointment_data = appointment_data
        return appointment_data

    #---------------------------------------------------------------- 
    def assign_id_appointment(self,
        appointment_data: pd.DataFrame,
        pk_column_name: str = 'id_appointment',
        starting_id_value: int = 1,
        ):
        """
            Creates a new column ('id_appointment') to the DataFrame
            appointment_data to serve as main identifier. 
            Returns the DataFrame with the newly added id.
            The DataFrame appointment_data is sorted by appointment
            date and id_tmp before within this function.
        """
        logging.info("Adding PK id_appointment to relation Appointment")
        appointment_data = appointment_data.sort_values(
            ['appt_date', 'id_tmp']
            ).reset_index(drop=[True, True])

        appointment_data = add_primary_key_values(
            relation=appointment_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        self.appointment_data = appointment_data
        return appointment_data

    #---------------------------------------------------------------- 
    def assign_id_animal(self,
        appointment_data: pd.DataFrame,
        animal_data: pd.DataFrame
        ):
        """
            Add attribute 'id_animal' in DataFrame appointment_data
            to serve as the relation's foreign key towards relation
            animal.
        """
        logging.info("Adding FK id_animal to relation Appointment")
        if 'id_tmp' not in animal_data.columns:
            raise KeyError(f"Required column 'id_tmp' not found"
                           f"in DataFrame 'animal_data'")
                           
        appointment_data = appointment_data.merge(
            animal_data[['id_tmp', 'id_animal']],
            on='id_tmp',
            how='left'
        )

        self.appointment_data = appointment_data
        return appointment_data

    #---------------------------------------------------------------- 
    def generate_monthly_demand(self,
        appt_duration: dict,
        appointment_data: pd.DataFrame = None,
        ) -> pd.DataFrame:
        """
            Generate a summary of the month demand in terms of hours
            to be spent in appointments, divided between categories:
            surgery and other. Provide the duration of each category
            of appointment in a dictionnary (appt_duration).
        """
        logging.info(f"Start calculating the monthly appointment demand.")

        if appointment_data is None or len(appointment_data) == 0:
            appointment_data = self.appointment_data

        appointment_data_copy  = appointment_data.copy()
        appointment_data_copy['appt_date'] = pd.to_datetime(
            appointment_data_copy['appt_date']
        )
        appointment_data_copy['month'] = appointment_data_copy[
            'appt_date'
        ].dt.to_period('M').apply(lambda r: r.start_time)
        appointment_data_copy['hours'] = appointment_data_copy[
            'appt_reason'
        ].apply(lambda x: appt_duration.get(x, 1))

# HERE MODIFY THIS FUNCTION TO LET THE CHOICE OF GROUPING TO USER
        appointment_data_copy['appt_reason_grouped'] = appointment_data_copy[
            'appt_reason'
        ].apply(lambda x: 'surgery' if x == 'surgery' else 'other')

        monthly_demand = appointment_data_copy.groupby(
            ['month', 'appt_reason_grouped'],
            as_index=False
        )['hours'].sum()

        monthly_demand = monthly_demand.rename(
            columns={'appt_reason_grouped': 'appt_reason'}
        )

        return monthly_demand

    #---------------------------------------------------------------- 
    def assign_week_appointment(self,
        appointment_data: pd.DataFrame,
        slot_data: pd.DataFrame,
        start_day: int = 5 # has to be between 0 and 6
        ):
        """
            Create a new column ('week') to the DataFrame appointment_data.
            The weeks of the appointments are numbered to match the ones
            assigned to the slots (in DataFrame slot_data), the first
            week being assigned to the week of the first slot.
        """
        logging.info(f"Add the week number to all appointments, "
                     f"week 1 being the week of the first slot.")
        appointment_weeks = appointment_data.copy()
        min_date = slot_data['date'].min()
        w = min_date.weekday()
        offset = (w - start_day) % 7
        week1_start = min_date - pd.Timedelta(days=offset)
        days_since_anchor = (appointment_weeks['appt_date'] - week1_start).dt.days
        appointment_weeks['week'] = (days_since_anchor // 7) + 1

        return appointment_weeks

    #---------------------------------------------------------------- 
    def assign_owner_to_appt(self,
        animal_owner_data: pd.DataFrame,
        appointment_data: pd.DataFrame,
        animal_data: pd.DataFrame
        ):
        """
            Create a new column ('id_owner_tmp') to the DataFrame
            appointment_data to serve as the relation's foreign key 
            towards relation owner.
        """
        logging.info(f"Start matching appointments to owners.")

        appointment_data['id_owner_tmp'] = None
        list_id_animals = appointment_data['id_animal'].unique()
        animals_microchip_id = animal_data['id_microchip'].unique()

        unused_owners = set(animal_owner_data['id_owner_tmp'].unique())
        used_owners = set()

        for id in list_id_animals:
            id_microchip = int(animal_data[animal_data['id_animal'] == id]['id_microchip'].unique()[0])
            list_id_owners = animal_owner_data[animal_owner_data['id_microchip'] == id_microchip]['id_owner_tmp'].unique()

            appointments_index = appointment_data[appointment_data['id_animal'] == id].index.values.tolist()

            for idx in appointments_index:
                available = [owner for owner in list_id_owners if owner in unused_owners]
                
                if not available:
                    chosen_owner = int(rd.choice(list_id_owners))
                else:
                    chosen_owner = int(rd.choice(available))
                    unused_owners.discard(chosen_owner)
                    used_owners.add(chosen_owner)
                
                appointment_data.loc[idx, 'id_owner_tmp'] = chosen_owner

        self.appointment_data = appointment_data
        return appointment_data

#----------------------------------------------------------------
    def assign_appointment_id_owner(self,
        owner_data: pd.DataFrame,
        appointment_data: pd.DataFrame = None
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
        logging.info("Adding FK id_owner to relation Appointment")
        if appointment_data is None or len(appointment_data) == 0:
            appointment_data = self.appointment_data

        appointment_data = appointment_data.merge(
            owner_data[['id_owner_tmp', 'id_owner']],
            on='id_owner_tmp',
            how='left'
        )

        self.appointment_data = appointment_data
        return appointment_data

