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
    get_country_holidays)
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="- %(levelname)s - %(asctime)s - %(message)s"
)

class Slot():
    """
        This class is used to create the relation Slot, and 
        progressively generate the required data. The self 
        contained DataFrames slot_data is updated as data
        generation progresses.
    """
    def __init__(self):
        """
            No argument is required to initiate the instance
        """
        logging.info("Instantiating object from class Slot")
        pass

    #----------------------------------------------------------------
    def generate_slots(self,
        doctor_historization: pd.DataFrame,
        start_time: int,
        end_time: int,
        weekly_days_off: list = [4]
        ):
        """
            Generates the details on all available slots according to
            the doctors' availabilities during each week the work on,
            the first and last daily appointments' times.
            No slot can be proposed during the specified weekly day off.
        """

        doctor_historization_copy = doctor_historization.copy()
        doctor_historization_copy['period_start_date'] = pd.to_datetime(
            doctor_historization_copy['period_start_date']
        )
        doctor_historization_copy['period_end_date'] = pd.to_datetime(
            doctor_historization_copy['period_end_date']
        )

        start_date = doctor_historization_copy['period_start_date'].min()
        end_date = (doctor_historization_copy['period_end_date'].max() + MonthBegin(2)) - pd.Timedelta(days=1)

        # Generate all dates from start_date to end_date
        all_days = pd.date_range(start=start_date, end=end_date, freq='D')
        week_days = all_days.weekday
        correct_days = ~np.isin(week_days, weekly_days_off)
        valid_days = all_days[correct_days]

        hours = range(start_time, end_time)
        all_datetimes = pd.to_datetime([
            f"{day.date()} {hour:02d}:00:00"
            for day in valid_days
            for hour in hours
        ])

        datetime_df = pd.DataFrame({'date_time': all_datetimes})
        datetime_df['date'] = datetime_df['date_time'].dt.date
        datetime_df['time'] = datetime_df['date_time'].dt.time
        
        records = []
        for _, row in doctor_historization_copy.iterrows():
            doctor_id = row['id_doctor']
            start = row['period_start_date'].date()
            end = row['period_end_date'].date()
            
            mask = (datetime_df['date'] >= start) & (datetime_df['date'] <= end)
            matching_datetimes = datetime_df[mask]
            
            for _, dt_row in matching_datetimes.iterrows():
                records.append({
                    'id_doctor': doctor_id,
                    'date': dt_row['date'],
                    'time': dt_row['time']
                })

        slot_data = pd.DataFrame(records)
        slot_data = slot_data.sort_values(
            by=['id_doctor', 'date', 'time']
        ).reset_index(drop=True)

        slot_data['date'] = pd.to_datetime(slot_data['date'])

        logging.info(f"Generated initial version of relation Slot "
                     f"of size {len(slot_data)}")
        self.slot_data = slot_data
        return slot_data

    #---------------------------------------------------------------- 
    def generate_slot_data_df(self):
        """
            Return the current state of the DataFrame slot_data
        """
        logging.info("Return the Slot relation's data")
        return self.slot_data

    #----------------------------------------------------------------
    def adjust_slot_to_start_end_dates(self,
        slot_data: pd.DataFrame,
        doctor_data: pd.DataFrame
        ):
        """
            Remove the slots dates that are before the associated 
            doctor's working start date or after the working end date.
        """
        logging.info(f"Removing slots outside of the working period "
                     f"to the associated doctors.")
        slot_data_copy = slot_data.copy()
        slot_data_copy['date'] = pd.to_datetime(slot_data_copy['date']).dt.date

        id_doctor_list = doctor_data['id_doctor'].unique()
        for id in id_doctor_list:
            start_date = doctor_data[doctor_data['id_doctor'] ==  id]['start_date'].values[0]
            start_date = pd.to_datetime(start_date).date()
            end_date = doctor_data[doctor_data['id_doctor'] ==  id]['end_date'].values[0]
            end_date = pd.to_datetime(end_date).date()

            slot_data_copy = slot_data_copy.drop(
                slot_data_copy[
                    (slot_data_copy['id_doctor']==id)
                    & ((slot_data_copy['date'] < start_date)
                    | (slot_data_copy['date'] > end_date))
                    ].index
                )

        self.slot_data = slot_data_copy
        return slot_data_copy

    #----------------------------------------------------------------
    def adjust_slots_to_country_holidays(self,
        slot_data: pd.DataFrame,
        country_code: str = 'JO'
        ):
        """
            Remove the slots for which the date is a national holiday
            in the clinic's operating country.
        """
        logging.info(f"Adjusting slot dates to comply with public "
                     f"holidays in {country_code}.")
        years = slot_data['date'].apply(lambda x: x.year).unique()
        jordan_holidays = get_country_holidays(
            years = years,
            country_code = country_code
        )
        slot_data = slot_data[~slot_data['date'].isin(jordan_holidays)]

        self.slot_data = slot_data
        return slot_data

    #----------------------------------------------------------------
    def label_appointment_type(self,
        slot_data: pd.DataFrame,
        doctor_historization: pd.DataFrame,
        max_daily_working_hours: int = 8
        ):
        """
            Creates a new column ('type') to the DataFrame slot_data.
            Assign appointment type to each slot. The two categories
            are 'regular' and 'overtime'. The number of regular slots
            has to match the maximal monthly workload of the doctors
            and the maximal daily working hours. Every other slot is
            categorised 'overtime'.
        """
        logging.info(f"Start labelling slots (regular/overtime) based "
                     f"on the doctors' workload and daily working hours.")

        slot_data['type'] = ''
        slot_data['date'] = pd.to_datetime(slot_data['date'])
        doctor_historization['period_start_date'] = pd.to_datetime(doctor_historization['period_start_date'])
        doctor_historization['period_end_date'] = pd.to_datetime(doctor_historization['period_end_date'])

        slot_data['year_month'] = slot_data['date'].dt.to_period('M')
        doctor_historization['year_month'] = doctor_historization['period_start_date'].dt.to_period('M')

        merged = slot_data.merge(
            doctor_historization,
            on=['id_doctor', 'year_month'],
            how='left'
        )

        merged = merged[
            (merged['date'] >= merged['period_start_date']) &
            (merged['date'] <= merged['period_end_date'])
        ]

        merged = merged.sort_values(by=['id_doctor', 'year_month', 'date', 'time'])
        def assign_types(group):
            max_hours = group['max_monthly_hours'].iloc[0]
            assigned_regular = 0
            group['type'] = 'overtime'

            for date, day_group in group.groupby('date'):
                if assigned_regular >= max_hours:
                    break

                assignable_today = min(
                    max_daily_working_hours,
                    max_hours - assigned_regular
                )
                if assignable_today <= 0:
                    break

                idxs = day_group.index[:assignable_today]
                group.loc[idxs, 'type'] = 'regular'
                assigned_regular += assignable_today

            return group

        labeled = merged.groupby(['id_doctor', 'year_month'], group_keys=False).apply(assign_types)
        revised_slot_data = labeled[['id_doctor', 'specialty', 'date', 'time', 'type']]

        self.slot_data = revised_slot_data
        return revised_slot_data

    #----------------------------------------------------------------
    def assign_id_slot(self,
        slot_data: pd.DataFrame,
        pk_column_name: str = 'id_slot',
        starting_id_value: int = 1,
        ):
        """
            Creates a new column ('id_slot') to the DataFrame
            slot_data to serve as main identifier. 
            Returns the DataFrame with the newly added id.
            Order of rows does not matter, no need to sort first.
        """
        logging.info(f"Adding PK id_slot to relation Slot.")

        slot_data = slot_data.sort_values(
            ['date', 'time']
        ).reset_index(drop=[True, True])

        slot_data = add_primary_key_values(
            relation=slot_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )
        return slot_data

    #----------------------------------------------------------------
    def assign_week_slot(self,
        slot_data: pd.DataFrame,
        start_day: int = 5 # has to be between 0 and 6
        ):
        """
            Create a new column ('week') to the DataFrame slot_data.
            The weeks of the slots are numbered, the first week being
            assigned to the week of the first slot.
        """
        logging.info(f"Add the week number to all slots, week 1 being "
                     f"the week of the first slot.")

        slot_weeks = slot_data.copy()
        min_date = slot_weeks['date'].min()
        w = min_date.weekday()
        offset = (w - start_day) % 7
        week1_start = min_date - pd.Timedelta(days=offset)
        days_since_anchor = (slot_weeks['date'] - week1_start).dt.days
        slot_weeks['week'] = (days_since_anchor // 7) + 1

        self.slot_data = slot_weeks
        return slot_weeks

    #----------------------------------------------------------------
    def assign_appointments_to_slots(self,
        appointment_data: pd.DataFrame,
        slot_data: pd.DataFrame):
        """
            Assigns available slots to appointments, prioritizing surgery appointments.
            Rules:
            - Surgery appointments are assigned first, then follow-up surgeries, then the other ones.
            - Surgery appointments go only to surgeons, and require 3 consecutive hourly slots.
            - Other appointments can go to any doctor.
            - Prefer slots with type 'regular' before 'overtime'.
            - A slot can only be used once.
            Returns a DataFrame with columns: ['id_appointment', 'id_slot']
        """
        logging.info(f"Start creating relation Appointment_Slot by "
                     f"matching appointments to slots.")

        appointment_data_copy = appointment_data.copy()
        slot_data_copy = slot_data.copy()
        slot_data_copy['id_appointment'] = None

        appointment_data_copy['appt_date'] = pd.to_datetime(appointment_data_copy['appt_date'])
        slot_data_copy['date'] = pd.to_datetime(slot_data_copy['date'])
        weeks = slot_data_copy['week'].unique()

        def generate_surgery_planning(
            slot_data: pd.DataFrame
            ) -> pd.DataFrame:
            """
                Generate a surgery slots planning by grouping available
                slots associated to surgeons by three.
            """
            df = slot_data.copy()
            df['date'] = pd.to_datetime(df['date'], dayfirst=True).dt.date

            surgeons = df[df['specialty'] == 'surgeon'].copy()
            if surgeons.empty:
                return pd.DataFrame(
                    columns=["id_doctor", "date", "slot1_id", "slot2_id", "slot3_id", "time"]
                )

            def hour_index(t):
                return t.hour
            surgeons.sort_values(["id_doctor", "date", "time"], inplace=True)
            grouped = surgeons.groupby(["id_doctor", "date", "week", "type"])

            blocks = []
            k = 1
            for (doc_id, day, week, type), group in grouped:
                slot_ids = group["id_slot"].tolist()
                times   = group["time"].tolist()
                hours   = [hour_index(t) for t in times]
                n = len(hours)

                i = 0
                while i <= n - 3:
                    # Check if these three hours are consecutive
                    if hours[i + 1] == hours[i] + 1 and hours[i + 2] == hours[i] + 2:
                        slot1 = slot_ids[i]
                        slot2 = slot_ids[i + 1]
                        slot3 = slot_ids[i + 2]
                        time1 = times[i].strftime("%H:%M")
                        time2 = times[i + 1].strftime("%H:%M")
                        time3 = times[i + 2].strftime("%H:%M")

                        blocks.append({
                            "id_slot": k,
                            "id_doctor": doc_id,
                            "date": day,
                            "week": week,
                            "type": type,
                            "slot1_id": slot1,
                            "slot2_id": slot2,
                            "slot3_id": slot3,
                            "times": f"{time1} → {time2} → {time3}"
                        })
                        k = k+1
                        i += 3
                    else:
                        i += 1
            
            surgery_planning = pd.DataFrame(blocks)

            return surgery_planning


        def assign_appointment_type_to_slots(
            type_appointments: pd.DataFrame,
            type_planning: pd.DataFrame,
            weeks: list
            ):
            """
                Match the appointments of a specific type: surgery or
                regular (listed in DataFrame type_appointments) to
                the planning of available slots for the same specific
                type (listed in DataFrame type_planning).
                The match is made by week: all appointments planned
                for a week are assigned to slots in the same week.
                If there are not enough available slots in that week,
                the week of non-assigned appointments is modified to 
                the following one.
            """
            slots_columns = type_planning.columns
            assigned_slot = pd.DataFrame(columns = slots_columns)
            for week in weeks:
                week_appointments = type_appointments[type_appointments['week'] == week]
                week_planning = type_planning[type_planning['week'] == week]
                for _, appt in week_appointments.iterrows():
                    id_appt = appt['id_appointment']
                    avail_reg_slot = week_planning[
                        (week_planning['id_appointment'].isna())
                        & (week_planning['type'] == 'regular')]
                    avail_over_slot = week_planning[
                        (week_planning['id_appointment'].isna())
                        & (week_planning['type'] == 'regular')]

                    if len(avail_reg_slot) > 0:
                        id_planning = min(avail_reg_slot['id_slot'])
                        week_planning.loc[
                            week_planning['id_slot']==id_planning,
                        'id_appointment'] = id_appt
                    elif len(avail_over_slot) > 0:
                        id_planning = min(avail_over_slot['id_slot'])
                        week_planning.loc[
                            week_planning['id_slot']==id_planning,
                        'id_appointment'] = id_appt
                    else:
                        type_appointments.loc[
                            type_appointments['id_appointment'] == id_appt,
                        'week'] = week+1
                assigned_slot = pd.concat(
                    [assigned_slot, week_planning], 
                    ignore_index = True)

            return assigned_slot

        def expand_planning_to_slots(
            planning_df: pd.DataFrame
            ) -> pd.DataFrame:
            """
                Reformat the assigned grouped slots to surgical
                appointments to get the individual match: 1 slot -
                1 appointment. In the new format, each surgical
                appointment will be assigned to three slots
                (compared to one group of slot in the input DataFrame).
            """

            df = planning_df.copy()
            df = df.rename(columns={'id_slot': 'id_slot_tmp'})
            keep_cols = [col for col in df.columns
                        if col not in ("slot1_id", "slot2_id", "slot3_id")]
            melted = df.melt(
                id_vars=keep_cols,
                value_vars=["slot1_id", "slot2_id", "slot3_id"],
                var_name="slot_position",
                value_name="id_slot"
            )

            melted = melted.dropna(subset=["id_slot"])
            melted = melted.drop(columns=["slot_position"])
            cols_order = ["id_doctor", "date", "id_slot", "id_appointment"]
            if "id_appt" in keep_cols:
                cols_order.append("id_appointment")

            for c in keep_cols:
                if c not in ("id_doctor", "date", "id_appointment", "id_appointment"):
                    cols_order.append(c)
            
            reformated_planning = melted[cols_order]

            return reformated_planning

        # ASSIGN SURGERIES FIRST
        logging.info(f"start assigning surgical appointments to "
                     f"slots accomodating surgeries.")
        surgery_appointments = appointment_data[
            appointment_data['appt_reason'] == 'surgery'
        ]
        surgery_planning = generate_surgery_planning(slot_data_copy)
        surgery_planning['id_appointment'] = None

        assigned_surgeries_grouped = assign_appointment_type_to_slots(
            type_appointments = surgery_appointments,
            type_planning = surgery_planning,
            weeks = weeks
        )
        assigned_surgeries = expand_planning_to_slots(
            planning_df = assigned_surgeries_grouped
        )

        # ADD THE id_appointment TO slot_data_copy
        surg_slot_to_appt = assigned_surgeries.set_index('id_slot')['id_appointment'].to_dict()
        slot_data_copy['id_appointment'] = slot_data_copy['id_appointment'].fillna(
            slot_data_copy["id_slot"].map(surg_slot_to_appt)
        )

        # ASSIGN FOLLOW-UP SURGERY
        logging.info(f"start assigning surgery follow-up "
                     f"appointments to available slots.")
        fu_surgery_appointments = appointment_data_copy[
            appointment_data_copy['appt_reason'] == 'follow-up surgery'
        ]
        fu_surgery_planning = slot_data_copy[
            (slot_data_copy['specialty'] == 'surgeon')
            & (slot_data_copy['id_appointment'].isna())
        ]
        assigned_fu_surgeries = assign_appointment_type_to_slots(
            type_appointments = fu_surgery_appointments,
            type_planning = fu_surgery_planning,
            weeks = weeks
        )
        # ADD THE id_appointment TO slot_data_copy
        ufs_slot_to_appt = assigned_fu_surgeries.set_index('id_slot')['id_appointment'].to_dict()
        slot_data_copy['id_appointment'] = slot_data_copy['id_appointment'].fillna(
            slot_data_copy["id_slot"].map(ufs_slot_to_appt)
        )

        # ASSIGN OTHER APPOINTMENTS
        logging.info(f"start assigning remaining appointments "
                     f"to available slots.")
        other_appointments = appointment_data_copy[
            ~appointment_data_copy['appt_reason'].isin(
                ['follow-up surgery', 'surgery']
            )
        ]
        other_planning = slot_data_copy[
            slot_data_copy['id_appointment'].isna()
        ]
        assigned_other_appt = assign_appointment_type_to_slots(
            type_appointments = other_appointments,
            type_planning = other_planning,
            weeks = weeks
        )
        # ADD THE id_appointment TO slot_data_copy
        other_slot_to_appt = assigned_other_appt.set_index('id_slot')['id_appointment'].to_dict()
        slot_data_copy['id_appointment'] = slot_data_copy['id_appointment'].fillna(
            slot_data_copy["id_slot"].map(other_slot_to_appt)
        )

        appointment_slot_data = slot_data_copy[
            ~slot_data_copy['id_appointment'].isna()
        ][['id_appointment', 'id_slot']]

        appointment_slot_data['id_appointment'] = appointment_slot_data['id_appointment'].astype(int)

        logging.info(f"Generated relation Appointment_Slot "
                     f"of size {len(appointment_slot_data)}")
        self.appointment_slot_data = appointment_slot_data
        return appointment_slot_data

    #----------------------------------------------------------------
    def assign_id_appointment_slot(self,
        appointment_slot_data: pd.DataFrame,
        pk_column_name: str = 'id_appointment_slot',
        starting_id_value: int = 1,
        ):
        """
            Creates a new column ('id_appointment_slot') to the
            DataFrame appointment_slot_data to serve as main identifier. 
            Returns the DataFrame with the newly added id.
            Sort values by id_slot and id_appointment first.
        """
        logging.info(f"Adding PK id_appointment_slot to relation "
                     f"Appointment_Slot.")

        appointment_slot_data = appointment_slot_data.sort_values(
            ['id_slot', 'id_appointment']
            ).reset_index(drop=[True, True])

        appointment_slot_data = add_primary_key_values(
            relation=appointment_slot_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        self.appointment_slot_data = appointment_slot_data
        return appointment_slot_data