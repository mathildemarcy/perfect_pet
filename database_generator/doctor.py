import pandas as pd
import numpy as np
import random as rd
import math
import string
import holidays
from dateutil import relativedelta
from datetime import date, datetime, timedelta
from pandas.tseries.offsets import MonthEnd, MonthBegin
from bisect import bisect_left
from faker import Faker
fake = Faker()
from shared_functions import (
    add_primary_key_values,
    generate_random_strings,
    select_random_date_in_range,
    generate_random_dates)
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="- %(levelname)s - %(asctime)s - %(message)s"
)

class Doctor:
    """
        This class is used to create the relations doctor and
        doctor_historization, and progressively generate the 
        required data. The self contained DataFrames doctor and
        doctor_historization are updated as data generation progresses.
    """
    def __init__(self,
        appointment_data: pd.DataFrame,
        yearly_turnover: float = 1/8,
        weekly_worked_days: int = 6,
        daily_max_worked_hours: int = 8,
        yearly_holiday_weeks: int = 5
        ):
        """
            Initialize the instance with the previously generated
            DataFrame appointment_data and other input parameters.
        """
        logging.info("Instantiating object from class Doctor")

        appointment_data_copy = appointment_data.copy()
        appointment_data_copy['appt_date'] = pd.to_datetime(appointment_data_copy['appt_date'])
        appointment_data_copy['year'] = appointment_data_copy['appt_date'].dt.year
        appointment_data_copy['month'] = appointment_data_copy['appt_date'].dt.month
        self.appointment_data_copy = appointment_data_copy
        
        self.first_appt_date = pd.to_datetime(min(appointment_data_copy['appt_date']))
        self.last_appt_date = pd.to_datetime(max(appointment_data_copy['appt_date']))
        activity_period = relativedelta.relativedelta(self.last_appt_date, self.first_appt_date)
        self.nb_activity_months = activity_period.years * 12 + activity_period.months

        self.max_monthly_hours_doctor = (daily_max_worked_hours * weekly_worked_days * (52-yearly_holiday_weeks))/12
        self.monthly_turnover = yearly_turnover/12


    #----------------------------------------------------------------
    def generate_appointment_data_copy_df(self):
        """
            Return the a copy of the DataFrame appointment_data
        """
        logging.info("Return the copy of Appointment relation's data")
        return self.appointment_data_copy

    #----------------------------------------------------------------
    def calculate_monthly_appt_nb(self,
        appointment_data: pd.DataFrame
        ):
        """
            Group the appointments from DataFrame appointment_data
            to calculate the monthly demand in terms of number
            of appointments. To calculate the monthly demand
            for surgeries only, the input DataFrame appointment_data
            should be filtered before being passed as parameter.
        """
        logging.info("Calculating the number of appointments per month.")
        monthly_appt = (
            appointment_data
            .groupby(['year', 'month'])['id_appointment']
            .nunique()
            .reset_index(name='appt_nb')
        )
        return monthly_appt

    #----------------------------------------------------------------
    def calculate_nb_doctor_total(self,
        monthly_demand: pd.DataFrame,
        appt_duration: int
        ):
        """
            Calculate the number of doctors to be available at all
            times based on the monthly demand, the duration of each
            appointment and the working conditions (inputs to the 
            class' instantiation). 
            To calculate the number of surgeons only, the input 
            DataFrame monthly_demand should be filtered before 
            being passed as parameter.
        """
        logging.info(f"Calculating the number of doctors that should be "
                     f"available at all times based on the number of "
                     f"appointments per month.")
        max_nb_doctor = math.ceil(
            max(monthly_demand['appt_nb'] * appt_duration / self.max_monthly_hours_doctor)
            )
        nb_doctors_total = max_nb_doctor + math.ceil(
            max_nb_doctor * self.nb_activity_months * self.monthly_turnover
            )
        return nb_doctors_total

    #----------------------------------------------------------------
    def calculate_xthperc(self,
        x: int,
        monthly_appt: pd.DataFrame,
        appt_duration: int
        ):
        """
            Calculate the xth poercentile of the peak number of doctors
            based on the monthly demand, the duration of each
            appointment and the working conditions (inputs to the 
            class' instantiation). 
            To calculate the number of surgeons only, the input 
            DataFrame monthly_demand should be filtered before 
            being passed as parameter.
            constraint:0<x<100
        """
        logging.info(f"Calculating the xth percentile of the peak "
                     f"number of appointments per month.")
        xthperc_value = math.ceil(
            np.percentile(monthly_appt['appt_nb']*appt_duration, x) / self.max_monthly_hours_doctor
            )
        return xthperc_value

    #----------------------------------------------------------------
    def generate_initial_doctor_data(self,
        nb_doctor: int
        ):
        """
            Create the DataFrame doctor_data to store the data of
            relation doctor. The attributes' values are mainly
            generated with the help of package Faker.
        """
        logging.info("Start generating the doctors' profiles.")

        doctors_first_names = []
        for _ in range(nb_doctor):
            doctors_first_names.append(
                fake.first_name()
            )
        doctors_last_names = []
        for _ in range(nb_doctor):
            doctors_last_names.append(
                fake.last_name()
            )

        license_numbers = generate_random_strings(nb_doctor)

        doctor_data = pd.DataFrame({
            'first_name': doctors_first_names,
            'last_name': doctors_last_names,
            'license_number': license_numbers
        })

        logging.info(f"Generated initial version of relation Doctor "
                     f"of size {len(doctor_data)}")
        self.doctor_data = doctor_data
        return doctor_data

    #----------------------------------------------------------------
    def generate_doctor_data_df(self):
        """
            Return the current state of the DataFrame doctor_data
        """
        logging.info("Return the copy of Doctor relation's data")
        return self.doctor_data

    #----------------------------------------------------------------
    def assign_doctor_specialty(self,
        specialy_dict: dict,
        doctor_data: pd.DataFrame
        ):
        """
            Create a new column: specialy (that can take as values
            the keys of dictionnary specialy_dict) in the DataFrame
            doctor_data. The assignment is random while respecting
            the distribution of doctor per specialty (obtained by
            calulating previously the number of doctors needed for
            each specialty which is passed as value in specialy_dict).
        """
        logging.info(f"Start assigning specialy to all doctors in "
                     f"relation Doctor")

        doctor_data['specialty'] = ''
        available_indices = doctor_data.index

        for key, value in specialy_dict.items():
            indices_category = np.random.choice(
                available_indices,
                size=value,
                replace=False
            )
            doctor_data.loc[indices_category, 'specialty'] = key

            available_indices = np.setdiff1d(
                available_indices, 
                indices_category
            )
        
        self.doctor_data = doctor_data
        return doctor_data


    #----------------------------------------------------------------
    def assign_working_periods(self,
        doctor_data: pd.DataFrame,
        nb_doctor_min: int,
        nb_doctor_max: int,
        first_appt_date: date = None,
        last_appt_date: date = None,
        min_contract: int = 365,
        max_overlap: int = 180
        ):
        """
            Create two new columns: start_date and end_date in the
            DataFrame doctor_data. Their values are semi-randomly
            assigned such that the number of doctor present at all
            time per specialty is met.
        """
        logging.info(f"Start assigning start and end working dates to "
                     f"all doctors in relation Doctor")

        if first_appt_date is None:
            first_appt_date = self.first_appt_date

        if last_appt_date is None:
            last_appt_date = self.last_appt_date

        doctor_data=doctor_data.reset_index(drop=True)
        doctor_data.loc[range(nb_doctor_min), 'start_date'] = first_appt_date
        
        max_index = doctor_data.index.max()
        indices_late = range(nb_doctor_min,max_index+1)
        late_start_min_date = first_appt_date + timedelta(days=min_contract)
        late_start_dates = generate_random_dates(late_start_min_date, last_appt_date, len(indices_late))
        late_start_dates.sort()

        doctor_data.loc[indices_late, 'start_date'] = late_start_dates

        i=0
        a=0
        min_end_date = first_appt_date
        for i in range(max_index):
            j = i + nb_doctor_min
            if j <= max_index:
                min_end_date = doctor_data.loc[j, 'start_date']
                if min_end_date <= last_appt_date:
                    max_end_date = min_end_date + timedelta(days=max_overlap)
                    random_date = select_random_date_in_range(min_end_date, max_end_date)
                    doctor_data.loc[i, 'end_date'] = random_date
                    i=i+1
                    a=a+1
            else:
                doctor_data.loc[i, 'end_date'] = None
                i=i+1

        left_end_indices = doctor_data[doctor_data['end_date'].isna()].index
        not_left_end_indices = np.setdiff1d(
            doctor_data.index, 
            left_end_indices
        )
        left_end_nb = len(left_end_indices)
        left_to_add = nb_doctor_min - left_end_nb
        if left_to_add < 0:
            remove_indices = np.random.choice(
                left_end_indices,
                size=(left_to_add*-1),
                replace=False
            )
            doctor_data.drop(index=remove_indices)
        elif left_to_add > 0:
            add_indices = np.random.choice(
                not_left_end_indices,
                size=left_to_add,
                replace=False
            )
            doctor_data.loc[add_indices, 'end_date'] = None

        self.doctor_data = doctor_data
        return doctor_data

    #----------------------------------------------------------------
    def assign_id_doctor(self,
        doctor_data: pd.DataFrame,
        pk_column_name: str = 'id_doctor',
        starting_id_value: int = 1,
        ):
        """
            Creates a new column ('id_doctor') to the DataFrame
            doctor_data to serve as main identifier.
            Returns the DataFrame with the newly added id.
            The DataFrame doctor_data is sorted by working start_date
            before within this function.
        """
        logging.info(f"Adding PK id_doctor to relation Doctor.")
        doctor_data = doctor_data.sort_values(
            'start_date',
        ).reset_index(drop=True)

        doctor_data = add_primary_key_values(
            relation=doctor_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        self.doctor_data = doctor_data
        return doctor_data

    #----------------------------------------------------------------
    def assign_monthly_workload_min_unmet(self,
        monthly_demand: pd.DataFrame,
        doctor_data: pd.DataFrame,
        weekly_max_working_hours: list = [200, 175, 150, 125, 100],
        max_weekly_working_hours: int = 200
        ):
        """
            Create the DataFrame doctor_historization_data by
            first copying DataFrame doctor_data and then duplicate
            each tuple by the number of months the associated doctor
            worked at the clinic, and assigne a maximal workload
            for each month (as number of worked hours).
        """
        logging.info(f"Creating relation Doctor_Historization by "
                     f"assigning monthly maximum workload to all "
                     f"doctors in relation Doctor for all working "
                     f"periods.")
        doctor_data_copy = doctor_data.copy()

        doctor_data_copy['start_date'] = pd.to_datetime(doctor_data_copy['start_date'])
        doctor_data_copy['end_date'] = pd.to_datetime(doctor_data_copy['end_date'])

        monthly_summary_rows = []
        employees_monthly_rows = []

        for _, demand_row in monthly_demand.iterrows():
            month = demand_row["month"]
            total_hours = demand_row["hours"]
            nxt_mnth = month.replace(day=28) + timedelta(days=4)
            last_day_month = nxt_mnth - timedelta(days=nxt_mnth.day)

            available_doctors = doctor_data_copy[
                (doctor_data_copy["start_date"] <= last_day_month) &
                ((doctor_data_copy["end_date"].isna())
                | (doctor_data_copy["end_date"] >= month))
            ].copy()

            if available_doctors.empty:
                monthly_summary_rows.append({"month": month, "unmet_hours": total_hours})
                continue
            
            available_doctors["max_monthly_hours"] = 0
            available_doctors["assigned_hours"]    = 0

            available_doctors["max_monthly_hours"] = np.random.choice(
                weekly_max_working_hours,
                size=len(available_doctors)
            )

            total_capacity = available_doctors["max_monthly_hours"].sum()

            # Assign hours to each doctor based on their capacity, with the goal of covering the total demand
            hours_remaining = total_hours
            for _, doc_row in available_doctors.iterrows():
                doctor_capacity = doc_row["max_monthly_hours"]
                doctor_share = doctor_capacity / total_capacity
                assigned_hours = int(doctor_share * total_hours)
                assigned_hours = min(assigned_hours, doctor_capacity)
                available_doctors.loc[doc_row.name, "assigned_hours"] = assigned_hours
                hours_remaining -= assigned_hours
                if hours_remaining <= 0:
                    break

            unmet_hours = hours_remaining if hours_remaining > 0 else 0

            if unmet_hours > 0:
                doctors_to_adjust = available_doctors[available_doctors["max_monthly_hours"] < 200]
                doctors_to_adjust = doctors_to_adjust.sort_values(by="assigned_hours")
                for _, doc_row in doctors_to_adjust.iterrows():
                    if unmet_hours <= 0:
                        break

                    available_increase = max_weekly_working_hours - doc_row["max_monthly_hours"]
                    increase = min(unmet_hours, available_increase)

                    available_doctors.loc[doc_row.name, "max_monthly_hours"] += increase
                    available_doctors.loc[doc_row.name, "assigned_hours"] += increase
                    unmet_hours -= increase

            for _, doc_row in available_doctors.iterrows():
                employees_monthly_rows.append({
                    "id_doctor": doc_row["id_doctor"],
                    "first_name": doc_row["first_name"],
                    "last_name": doc_row["last_name"],
                    "specialty": doc_row["specialty"],
                    "license_number": doc_row["license_number"],
                    "period_start_date": month,
                    "period_end_date":last_day_month,
                    "assigned_hours": int(doc_row["assigned_hours"]) if pd.notna(doc_row["assigned_hours"]) else 0,
                    "max_monthly_hours": int(doc_row["max_monthly_hours"])
                })

            monthly_summary_rows.append({
                "month": month,
                "unmet_hours": unmet_hours
            })

        monthly_unmet_hours = pd.DataFrame(monthly_summary_rows)
        doctor_historization_data = pd.DataFrame(employees_monthly_rows)

        self.doctor_historization_data = doctor_historization_data
        logging.info(f"Generated relation Doctor_Historization "
                     f"of size {len(doctor_historization_data)}")
        return monthly_unmet_hours, doctor_historization_data

    #----------------------------------------------------------------
    def assign_id_doctor_histo(self,
        doctor_historization_data: pd.DataFrame,
        pk_column_name: str = 'id_doctor_histo',
        starting_id_value: int = 1,
        ):
        """
            Creates a new column ('id_doctor_histo') to the DataFrame
            doctor_historization_data to serve as main identifier.
            Returns the DataFrame with the newly added id.
            Order of rows does not matter, no need to sort first.
        """
        logging.info(f"Adding PK id_doctor_histo to relation "
                     f"Doctor_Historization.")
        doctor_historization_data = add_primary_key_values(
            relation=doctor_historization_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        self.doctor_historization_data = doctor_historization_data
        return doctor_historization_data

    #----------------------------------------------------------------
    def add_current_workload_to_doctor_data(self,
        doctor_data_details: pd.DataFrame,
        doctor_historization_data: pd.DataFrame
        ):
        """
            Create three new columns: period_start_date, period_end_date
            and max_monthly_hours in the DataFrame doctor_data and fill
            them with the values of these attributes from DataFrame
            doctor_historization_data associated to the same doctor
            and to the last period documented in doctor_historization_data.  
        """
        logging.info(f"Adding most recent maximum workload (associated "
                     f"to the lastest period).")

        last_period_start_date = max(doctor_historization_data['period_start_date'])
        last_period_end_date = max(doctor_historization_data['period_end_date'])
        doctor_current_workload = doctor_historization_data[
            doctor_historization_data['period_end_date'] == last_period_end_date
            ][['id_doctor', 'max_monthly_hours']]

        doctor_data = doctor_data_details.merge(
            doctor_current_workload,
            on='id_doctor',
            how='left')

        doctor_data['period_start_date'] = last_period_start_date
        doctor_data['period_end_date'] = last_period_end_date

        doctor_data = doctor_data.sort_values(
            'id_doctor'
        ).reset_index(drop=True)

        self.doctor_data = doctor_data
        return doctor_data