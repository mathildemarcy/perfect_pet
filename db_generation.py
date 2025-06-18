import pandas as pd
import numpy as np
import random as rd
import holidays
import math
from datetime import date, timedelta
from database_generator.animal import Animal, AnimalWeigth
from database_generator.microchip import MicrochipCode, Microchip
from database_generator.appointment import Appointment
from database_generator.service import Service
from database_generator.doctor import Doctor
from database_generator.slot import Slot
from database_generator.owner import Owner
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="- %(levelname)s - %(asctime)s - %(message)s"
)

# import external data
logging.info(f"Importing base datasets from csv files.")
animal_list = pd.read_csv('base_data/animal_list.csv')
appt_reason_service_list = pd.read_csv('base_data/appt_reason_service_list.csv')
cat_breed_weight_range = pd.read_csv('base_data/cat_breed_weight_range.csv')
dog_breed_weight_range = pd.read_csv('base_data/dog_breed_weight_range.csv')
microchip_codes_data = pd.read_csv('base_data/microchip_codes_data.csv')
service_list = pd.read_csv('base_data/service_list.csv')
surgery_types_distribution = pd.read_csv('base_data/surgery_types_distribution.csv')

logging.info(f"Setting values of input parameters.")
# set the number of unique animals to represent in the database
nb_animals = 250000
logging.info(f"The number of unique animals to include in the database "
             f"is {nb_animals}.")

# set the opening year of the clinic, the opening date by default will be the first day of the year
clinic_start_year = 2015
logging.info(f"The clinic's opening year was set to {clinic_start_year}.")

# set the last operation date to be represented in the database (appointment date)
last_operation_date = date.today()
logging.info(f"The last appointment date was set to {last_operation_date}.")

# Set the proportion of animal patients born before the clinic's opening
prop_born_before_opening = 0.3
logging.info(f"The proportion of animal patients born before the clinic's opening was set to {prop_born_before_opening}.")

# Set the maximum age of animal patients at the time of the clinic opening
max_animal_age_at_opening = 10
logging.info(f"The maximum age of animal patients at the time of the clinic opening was set to {max_animal_age_at_opening}.")

# Define the minimum age (in days) of an animal to be microchipped
dob_microchip_gap = 100
logging.info(f"The minimum age of an animal to be microchipped was set to {dob_microchip_gap} days.")

# Set the distribution of animals per microchip implant location
locations_dict = {
    'between_shoulders':0.9, 
    'midline_cervicals':0.01, 
    'left_lateral_neck':0.08, 
    'right_lateral_neck':0.01
}
logging.info(f"The distribution of animals per microchip implant location was set to {locations_dict}.")

# Define the country in which the clinic is operating
country_code = 'JO'
logging.info(f"The operation country of the clinic was set to {country_code}.")

# Specify the pets' life expectancy
life_expectancy = 15
logging.info(f"The pets' life expectancy was set to {life_expectancy}.")

# Set the distribution of first appointment reason for pets microchipped before
# the clinic's opening by reason for appointment
distribution_reason_microchipped_before_opening = {
    'annual_visit':0.6,
    'sick_pet':0.2,
    'injured_pet':0.2
}
logging.info(f"The distribution of first appointment reason "
             f"for pets microchipped before the clinic's opening "
             f"by reason for appointment was set to "
             f"{distribution_reason_microchipped_before_opening}.")

# Set the distribution of first appointment reason for pets microchipped after
# the clinic's opening by reason for appointment
distribution_reason_microchipped_after_opening = {
    'initial_visit':0.4,
    'annual_visit':0.15,
    'sick_pet':0.2,
    'injured_pet':0.15,
    'surgery':0.1,
}
logging.info(f"The distribution of first appointment reason "
             f"for pets microchipped after the clinic's opening "
             f"by reason for appointment was set to "
             f"{distribution_reason_microchipped_after_opening}.")

# Set the proportion of appointments following an appointment for injury
# or sickness that corresponds to a follow-up
perc_followup = 0.5
logging.info(f"The proportion of follow-up appointments was set to {perc_followup}.")

# Set the distribution of non-follow-up appointments by reason for appointment
prop_visit_non_followup = {
    'annual_visit':0.5,
    'sick_pet':0.2,
    'injured_pet':0.2,
    'surgery':0.1
}
logging.info(f"The distribution of non-follow-up appointments by  "
             f"reason for appointment was set to "
             f"{prop_visit_non_followup}.")

# Set the average yearly turnover of employed veterinarian
yearly_turnover = 1/8
logging.info(f"The average yearly turnover of employed veterinarian is set to {yearly_turnover}.")

# Specify the number of days worked per week per veterinarian in the clinic
weekly_worked_days = 6
logging.info(f"The number of days worked per week was set to {weekly_worked_days}.")

# Specify the maximum number of hours worked per day per veterinarian outside of overtime
daily_max_worked_hours = 8
logging.info(f"The maximum number of hours worked per day was set to {daily_max_worked_hours}.")

# Specify the maximum number of hours worked per week per veterinarian
max_weekly_working_hours = 200
logging.info(f"The maximum number of hours worked per week was set to {max_weekly_working_hours}.")

# Specify the average number of weeks of holidays per year per veterinarian at the clinic
yearly_holiday_weeks = 5
logging.info(f"The average number of weeks of holidays per year was set to {yearly_holiday_weeks}.")

# Specify duration of a regular appointment (all except surgeries)
regular_appt_duration = 1
logging.info(f"The duration of regular appointments was set to {regular_appt_duration} hour(s).")

# Specify duration of a surgical appointment
surgery_appt_duration = 3
logging.info(f"The duration of surgical appointments was set to {surgery_appt_duration} hour(s).")

# Specify the minimum number of days of a working contract
min_contract = 365
logging.info(f"The minimum duration of a veterinarian's work contract was set to {min_contract} days.")

# Specify the maximum number of days for two doctors overlaping while one replace the other
max_overlap = 180
logging.info(f"The maximum overlap between a doctor and the one they replace was set to {max_overlap} days.")

# Specify the working start time for veterinarian at the clinic
start_time = 8
logging.info(f"The daily first appointment time was set to {start_time}:00.")

# Specify the last appointment start time for veterinarian at the clinic
end_time = 20
logging.info(f"The daily last appointment time was set to {end_time}:00.")

# Specify the day of the week considered as the first day of the week in the country the clinic operates in
week_start_day = 5
logging.info(f"The first day of the week was set to {end_time}:00.")

# Specify the list of weekly days off according to the country in which the clinic operates
weekly_days_off = [4]
logging.info(f"The list of weekly days off was set to {weekly_days_off}.")

# Specify the distribution of households per number of pets
prop_nb_animal_household = {
    4:0.05,
    3:0.1,
    2:0.25,
    1:0.60
} 
logging.info(f"The istribution of households per number of pets was set to {prop_nb_animal_household}.")

# Set the minimum number of appointments an animal should have been to to
# be associated to more than one owner
min_nb_appt = 3
logging.info(f"The minimum number of appointments to assign an animal to more "
             f"than one owner was set to {min_nb_appt}.")

# Define the number of cities in which owners live
nb_cities = 10 
logging.info(f"The number of cities in which owners live was set to {nb_cities}.")

# Set the average number of streets covered by city
ratio_city_streets = 25
logging.info(f"The average number of streets covered by city was set to {ratio_city_streets}.")

# Set the proportion of households in which more than one person is registered as a pet owner
prop_household_several_owner = 0.3
logging.info(f"The proportion of households in which more than one person "
             f"is registered as a pet owner was set to {prop_household_several_owner}.")

#----------------------------------------------------------------------------
# Create the Animal class to create and modify the animal_data DataFrame
logging.info(f"Instantiating object from Animal class.")
Animal = Animal(
    base_animal_data = animal_list,
    nb_animals = nb_animals,
    clinic_start_year = clinic_start_year,
    last_operation_date = last_operation_date,
    initial_augmentation_seed = 56
)

# Create the initial animal_data DataFrame
animal_data = Animal.generate_animal_data_df()
logging.info(f"Initial version of Animal relation created of size {len(animal_data)}.")

# Add the dob attribute to animal_data
logging.info(f"Assigning dates of birth to animals in Animal relation.")
animal_data = Animal.assign_date_of_birth(
    animal_data = animal_data,
    prop_born_before_opening = prop_born_before_opening,
    max_animal_age_at_opening = max_animal_age_at_opening
)

# Add the hash_id
logging.info(f"Assigning unique hash values (has_id) to animals in Animal relation.")
animal_data = Animal.assign_hash_id(
    animal_data = animal_data
)

# Add a tmp id to animal_data
logging.info(f"Assigning a temporary id to animals in Animal relation.")
animal_data = Animal.assign_tmp_id(
    animal_data = animal_data
)

#----------------------------------------------------------------------------
# Create the MicrochipCode class
logging.info(f"Instantiating object from Microchip_Code class.")
MicrochipCode = MicrochipCode(
    microchip_code_data = microchip_codes_data
)

# Create the initial microchip_code_data DataFrame
microchip_code_data = MicrochipCode.generate_microchip_code_data_df()
logging.info(f"Microchip_Code relation created of size {len(microchip_code_data)}.")

# Add the id_code PK to microchip_code_data
logging.info(f"Assigning unique id values (id_code) to tuples in Microchip relation.")
microchip_code_data = MicrochipCode.assign_id_code(
    microchip_code_data = microchip_code_data,
    pk_column_name = 'id_code',
    starting_id_value = 3,
)

#----------------------------------------------------------------------------
# Create the Microchip class
logging.info(f"Instantiating object from Microchip class.")
Microchip = Microchip(
    animal_data = animal_data,
    microchip_code_data = microchip_code_data
)

# Create the initial microchip_data DataFrame
microchip_data = Microchip.assign_implant_date_based_on_dob(
    dob_microchip_gap = 100
)
logging.info(f"Initial version of Microchip relation created of size "
             f"{len(microchip_data)}, including attribute 'implant_date'.")

# Assign random microchip numbers
logging.info(f"Assigning number values to tuples in Microchip relation.")
microchip_data = Microchip.assign_random_microchip_number(microchip_data)

# Assign FK code_id poitning to microchip_code 
logging.info(f"Assigning randomly FK values to microchip codes in Microchip relation.")
microchip_data = Microchip.assign_fk_microchip_code()

# Assign implant location
logging.info(f"Assigning implant location values to tuples in Microchip relation.")
microchip_data = Microchip.assign_implant_location(microchip_data)

#----------------------------------------------------------------------------
# Create the Appointment class
logging.info(f"Instantiating object from Appointment class.")
Appointment = Appointment(
    microchip_data = microchip_data,
    clinic_start_year = clinic_start_year,
    last_operation_date = last_operation_date,
    life_expectancy = 15
)

# Create the initial appointment_data_denorm DataFrame
logging.info(f"Assigning number values of appointments to each animal included "
             f"in relation Animal.")
appointment_data_denorm = Appointment.assign_nb_appointments()

# Assign reason of first appointment
logging.info(f"Assigning a reason for the first appointment of each animal "
             f"included in relation Animal.")
appointment_data_denorm = Appointment.assign_first_appointment_reason(
    appointment_data_denorm = appointment_data_denorm
)
        
# Assign the date of the first appointment
logging.info(f"Assigning a date for the first appointment of each animal "
             f"included in relation Animal.")
appointment_data_denorm = Appointment.assign_first_appointment_date(
    appointment_data_denorm = appointment_data_denorm
)

# Assign details of following appointments
logging.info(f"Assigning reasons and dates for all appointments following "
             f"the first one for each animal included in relation Animal.")
appointment_data_denorm = Appointment.assign_appointment_details (
    appointment_data_denorm = appointment_data_denorm
)

# Reformat appointments df to denormalize it
logging.info(f"Reformating the Appintment relation to match the database "
             f"schema, such that every tuple represents one appointment only.")
appointment_data = Appointment.reformatting_appointment_df(
    appointment_data_denorm = appointment_data_denorm
)

# Correct the appointments assignement to match the calendar of the country
logging.info(f"Modifying the appointment dates to avoid scheduling any appointment "
             f"on a public holiday in {country_code}")
appointment_data = Appointment.correction_appt_date_daysoff(
    appointment_data = appointment_data,
    country_code = country_code,
    weekly_days_off = weekly_days_off
)

# Assign id_appointment
logging.info(f"Assigning unique id values (id_appointment) to tuples in "
             f"Appointment relation.")
appointment_data = Appointment.assign_id_appointment(
    appointment_data = appointment_data,
    pk_column_name = 'id_appointment',
    starting_id_value = 23,
)

#----------------------------------------------------------------------------
# Modify dataframe animal_data to add id_animal
# First, sort values by appointment date
logging.info(f"Sorting tuples in relation Animal by first appointment date.")
animal_data = Animal.sort_animal_by_appt_date(
    appointment_data = appointment_data,
    animal_data = animal_data
)
# Then assign id_animal
logging.info(f"Assigning unique id values (id_animal) to tuples in "
             f"Animal relation.")
animal_data = Animal.assign_id_animal(
    animal_data = animal_data,
    pk_column_name = 'id_animal',
    starting_id_value = 47,
)

#----------------------------------------------------------------------------
# Modify dataframe microchip_data to add id_microchip
# First, sort values by appointment date
logging.info(f"Sorting tuples in relation Microchip by first appointment date.")
microchip_data = Microchip.sort_microchip_by_appt_date(
    appointment_data = appointment_data,
    microchip_data = microchip_data
)
# Then assign id_microchip
logging.info(f"Assigning unique id values (id_microchip) to tuples in "
             f"Microchip relation.")
microchip_data = Microchip.assign_id_microchip(
    microchip_data = microchip_data,
    pk_column_name = 'id_microchip',
    starting_id_value = 34
)

#----------------------------------------------------------------------------
# Modify dataframe animal to add id_microchip
logging.info(f"Assigning values to FK id_microchip in Animal relation.")
animal_data = Animal.assign_id_microchip(
    microchip_data = microchip_data,
    animal_data = animal_data
)
#----------------------------------------------------------------------------
# Modify dataframe appointment_data to add id_animal
logging.info(f"Assigning values to FK id_animal in Appointment relation.")
appointment_data = Appointment.assign_id_animal(
    appointment_data = appointment_data,
    animal_data = animal_data
)

#----------------------------------------------------------------------------
# Create the AnimalWeight class to create and modify the animal_weight_data DataFrame
logging.info(f"Instantiating object from AnimalWeight class.")
AnimalWeigth = AnimalWeigth(
    animal_data = animal_data,
    appointment_data = appointment_data,
    cat_breed_weight_range = cat_breed_weight_range,
    dog_breed_weight_range = dog_breed_weight_range
)  

# Create the dataframe containing the initial weight of each animal
initial_weight_data = AnimalWeigth.assign_initial_weight_to_animals(
    cat_breed_weight_range = cat_breed_weight_range,
    dog_breed_weight_range = dog_breed_weight_range
)
logging.info(f"Initial version of AnimalWeight relation created of size "
             f"{len(initial_weight_data)}, by assigning the initial weight "
             f"value to every animal included in relation Animal.")

# Compute the other weight values
logging.info(f"Assigning weight values for other appointments for all animals "
             f"included in relation Animal.")
animal_weight_data = AnimalWeigth.assign_weight_per_appointment(
    initial_weight_data = initial_weight_data
)

# Assign id_weight
logging.info(f"Assigning unique id values (id_weight) to tuples in "
             f"AnimalWeight relation.")
animal_weight_data = AnimalWeigth.assign_id_weight(
    animal_weight_data = animal_weight_data,
    pk_column_name = 'id_weight',
    starting_id_value = 1
)
#----------------------------------------------------------------------------
# Create the Service class to create and modify the service and appointment_service DataFrames
logging.info(f"Instantiating object from Service class.")
Service = Service(
    service_data = service_list
)

# Create the dataframe service_data
service_data = Service.generate_service_data_df()
logging.info(f"Service relation created of size {len(service_data)}.")

# Add to service_data the id id_service
logging.info(f"Assigning unique id values (id_service) to tuples in "
             f"Service relation.")
service_data = Service.assign_id_service(
    service_data = service_data,
    pk_column_name = 'id_service',
    starting_id_value = 1
)

# Creation of appointment_services_data dataframe to map appointments with services
logging.info(f"Matching appointments with services.")
appointment_services_data = Service.map_appointment_services(
    service_data = service_data,
    appointment_data = appointment_data,
    appt_reason_service_list = appt_reason_service_list,
    surgery_types_distribution = surgery_types_distribution
)
logging.info(f"Appointment_Service relation created of size "
             f"{len(appointment_services_data)}.")

# Add id_appointment_service to dataframe appointment_services_data
logging.info(f"Assigning unique id values (id_appointment_service) to tuples in "
             f"Appointment_Service relation.")
appointment_services_data = Service.assign_id_appointment_service(
    appointment_services_data = appointment_services_data,
    pk_column_name = 'id_appointment_service',
    starting_id_value = 1
)

#----------------------------------------------------------------------------
# Create the Doctor class and relations doctor_data and doctor_historization_data
logging.info(f"Instantiating object from Doctor class.")
Doctor = Doctor(
    appointment_data = appointment_data,
    yearly_turnover = yearly_turnover,
    weekly_worked_days = weekly_worked_days,
    daily_max_worked_hours = daily_max_worked_hours,
    yearly_holiday_weeks = yearly_holiday_weeks
)

appointment_data_copy = Doctor.generate_appointment_data_copy_df()

# Get the number of surgeries and regular appointments scheduled per month
logging.info(f"Calculating the number of surgery and regular appointments "
             f"per month.")
monthly_appt_regular = Doctor.calculate_monthly_appt_nb(
    appointment_data_copy[appointment_data_copy['appt_reason']!='surgery']
)
monthly_appt_surgery = Doctor.calculate_monthly_appt_nb(
    appointment_data_copy[appointment_data_copy['appt_reason']=='surgery']
)

# Get the required number of generalist and surgeons based on scheduled 
# appointments and working conditions
logging.info(f"Calculating the number of surgeons and generalists to be "
             f"working at all time.")
nb_regular_total = Doctor.calculate_nb_doctor_total(
    monthly_demand = monthly_appt_regular,
    appt_duration = regular_appt_duration
)
nb_surgeons_total = Doctor.calculate_nb_doctor_total(
    monthly_demand = monthly_appt_surgery,
    appt_duration = surgery_appt_duration
)
nb_doctors_total = nb_regular_total + nb_surgeons_total

# Get the 95th percentile of number of doctors needed (based on peak demand)
logging.info(f"Calculating the 95th percentile of the peak demand for "
             f"surgeons and generalists throughou the entire working period.")
nb_regular_95thperc = Doctor.calculate_xthperc(
    x = 95,
    monthly_appt = monthly_appt_regular,
    appt_duration = regular_appt_duration
)
nb_surgeon_95thperc = Doctor.calculate_xthperc(
    x = 95,
    monthly_appt = monthly_appt_surgery,
    appt_duration = surgery_appt_duration
)
nb_doctors_95thperc = nb_regular_95thperc + nb_surgeon_95thperc


# Generate the initial data for doctors
initial_doctor_data = Doctor.generate_initial_doctor_data(
    nb_doctor = nb_doctors_total
)
logging.info(f"Initial version of Doctor relation created of size "
             f"{len(initial_doctor_data)}.")

# Randomly assign specialty to doctors
logging.info(f"Assigning specialty to doctors included in Doctor relation.")
specialy_dict = {
    'surgeon': nb_surgeons_total,
    'generalist': nb_regular_total
}
initial_doctor_data = Doctor.assign_doctor_specialty(
    specialy_dict = specialy_dict,
    doctor_data = initial_doctor_data
)

# Assign working periods to doctors (start and end work dates)
# while still ensuring that the demand is met
logging.info(f"Assigning start and end working date to doctors included in Doctor relation.")
first_appt_date = pd.to_datetime(min(appointment_data['appt_date']))
last_appt_date = pd.to_datetime(max(appointment_data['appt_date']))
generalist_data_workload = Doctor.assign_working_periods(
    doctor_data = initial_doctor_data[initial_doctor_data['specialty'] != 'surgeon'],
    nb_doctor_min = nb_regular_95thperc,
    nb_doctor_max = nb_regular_95thperc + 1,
    first_appt_date = first_appt_date,
    last_appt_date = last_appt_date,
    min_contract = min_contract,
    max_overlap = max_overlap
)
surgeon_data_workload = Doctor.assign_working_periods(
    doctor_data = initial_doctor_data[initial_doctor_data['specialty'] == 'surgeon'],
    nb_doctor_min = nb_surgeon_95thperc,
    nb_doctor_max = nb_surgeon_95thperc + 1,
    first_appt_date = first_appt_date,
    last_appt_date = last_appt_date,
    min_contract = min_contract,
    max_overlap = max_overlap
)
doctor_data_details = pd.concat(
    [generalist_data_workload, surgeon_data_workload],
    axis=0).reset_index(drop=True)

logging.info(f"Assigning unique id values (id_doctor) to tuples in "
             f"Doctor relation.")
doctor_data_details = Doctor.assign_id_doctor(
    doctor_data = doctor_data_details,
    pk_column_name = 'id_doctor',
    starting_id_value = 1,
)

# Compute monthly demand
appt_duration = {
    'surgery': surgery_appt_duration # All others assumed to be 1 hour
}

monthly_demand = Appointment.generate_monthly_demand(
    appt_duration = appt_duration,
    appointment_data = appointment_data
)

logging.info(f"Create relation Doctor_Historization by assigning evolving "
             f"working condition details to doctors included in Doctor relation.")
# Assign max monthly working hours to each doctor for each month he/she is working
generalist_doctor_histo = Doctor.assign_monthly_workload_min_unmet(
    monthly_demand = monthly_demand[monthly_demand['appt_reason']=='other'],
    doctor_data = doctor_data_details[doctor_data_details['specialty'] == 'generalist'],
    max_weekly_working_hours = max_weekly_working_hours
)[1]
surgeon_doctor_histo = Doctor.assign_monthly_workload_min_unmet(
    monthly_demand = monthly_demand[monthly_demand['appt_reason']=='surgery'],
    doctor_data = doctor_data_details[doctor_data_details['specialty'] == 'surgeon']
)[1]
# Create the doctor_historization_data dataframe
doctor_historization_data = pd.concat(
    [generalist_doctor_histo, surgeon_doctor_histo],
    axis=0).reset_index(drop=True)

logging.info(f"Sorting tuples in relation Doctor_Historization by period and id_doctor")
doctor_historization_data = doctor_historization_data.sort_values(
    ['period_start_date', 'id_doctor'],
    ascending=[True, True]
)
# Add id_doctor_histo to doctor_historization_data
logging.info(f"Assigning unique id values (id_doctor_histo) to tuples in "
             f"Doctor_Historization relation.")
doctor_historization_data = Doctor.assign_id_doctor_histo(
    doctor_historization_data = doctor_historization_data,
    pk_column_name = 'id_doctor_histo',
    starting_id_value = 1,
)

# Update relation doctor_data
logging.info(f"Updating relation Doctor to add the latest working conditions")
doctor_data = Doctor.add_current_workload_to_doctor_data(
    doctor_data_details = doctor_data_details,
    doctor_historization_data = doctor_historization_data
)

#----------------------------------------------------------------------------
# Create the Slot class and relation slot_data
logging.info(f"Instantiating object from Slot class.")
SlotData = Slot()

# Create initial relation slot_data
slot_data = SlotData.generate_slots(
    doctor_historization = doctor_historization_data,
    start_time = start_time,
    end_time = end_time,
    weekly_days_off = weekly_days_off
)
logging.info(f"Initial version of Slot relation created of size {len(slot_data)}.")

# Adjust it to the start and end working dates of each doctor
logging.info(f"Adjusting the slots days to comply with each doctor's start "
             f"and end working dates.")
slot_data = SlotData.adjust_slot_to_start_end_dates(
    slot_data = slot_data,
    doctor_data = doctor_data
)

# Adjust the slots to working days and national holidays in Jordan
logging.info(f"Modifying the slots' dates to avoid scheduling any appointment "
             f"on a public holiday in {country_code}")
slot_data = SlotData.adjust_slots_to_country_holidays(
slot_data = slot_data,
country_code = country_code
)

# Add appointment_type to each slot (regular or overtime) in order to
# respect the max working hours of each doctor for each month
logging.info(f"Assigning appointment type (regular/overtime) to respect "
             f"the max workload of each doctor for each period.")
revised_slot_data = SlotData.label_appointment_type(
    slot_data = slot_data,
    doctor_historization = doctor_historization_data,
    max_daily_working_hours = daily_max_worked_hours
)

logging.info(f"Assigning unique id values (id_slot) to tuples in "
             f"Slot relation.")
# Add id_slot to slot_data
revised_slot_data = SlotData.assign_id_slot(
    slot_data = revised_slot_data,
        pk_column_name = 'id_slot',
        starting_id_value = 1
)

# Add a week number to slots in order to facilitate the match between
# appointments and slots and reschedule appointments when needed
logging.info(f"Assigning week numbers to slots.")
revised_slot_data = SlotData.assign_week_slot(
    slot_data = revised_slot_data,
    start_day = week_start_day
)

#----------------------------------------------------------------------------
# Add a week number to appointments in order to facilitate the match between
# appointments and slots and reschedule appointments when needed
logging.info(f"Assigning week numbers to appointments.")
appointment_data = Appointment.assign_week_appointment(
    appointment_data = appointment_data,
    slot_data = revised_slot_data,
    start_day = week_start_day
)

#----------------------------------------------------------------------------
# Create the relation appointment_slot

# Assign appointments to slots
logging.info(f"Matching appointments with slots.")
appointment_slot_data = SlotData.assign_appointments_to_slots(
    appointment_data = appointment_data,
    slot_data = revised_slot_data
)
logging.info(f"Relation Appointment_Slot created of size {len(appointment_slot_data)}.")

# Add id_appointment_slot to appointment_slot_data
logging.info(f"Assigning unique id values (id_appointment_slot) to tuples in "
             f"Appointment_Slot relation.")
appointment_slot_data = SlotData.assign_id_appointment_slot(
    appointment_slot_data = appointment_slot_data,
    pk_column_name = 'id_appointment_slot',
    starting_id_value = 17
)

#----------------------------------------------------------------------------
# Instantiate class Owner and create the relation owner
logging.info(f"Instantiating object from Owner class.")
OwnerData = Owner(nb_animals = nb_animals)

# Compute number of owners
logging.info(f"Computing the total number of owners to be included in the database.")
nb_owners = OwnerData.compute_nb_owners()

# Generate initial owners' profiles
owner_data = OwnerData.generate_owner_profile(nb_owners=nb_owners)
logging.info(f"Initial version of Owner relation created of size {len(owner_data)}.")

# Add id_owner_tmp to owner_data
logging.info(f"Assigning a temporary id to owners in Owner relation.")
owner_data = OwnerData.assign_id_owner_tmp(
    owner_data = owner_data,
    pk_column_name = 'id_owner_tmp',
    starting_id_value = 1,
)

# Assign animals to owners
# this is done in two to three steps:
# step 1 - assign animals to one owner each
logging.info(f"Assigning animals to households.")
animal_owner_1 = OwnerData.assign_animal_to_household(
    owner_data = owner_data,
    microchip_data = microchip_data
)

# Step 2 - assign some animals to more than one owner
logging.info(f"Assigning some animals to additional owners.")
additional_owners = OwnerData.get_additional_owners_id(
    owner_data = owner_data,
    animal_owner_data = animal_owner_1
)
household_several_appt = OwnerData.get_list_animals_several_appt(
    appointment_data = appointment_data,
    animal_data = animal_data,
    animal_owner_data = animal_owner_1,
    min_nb_appt = 3 
)
animal_additional_owner = OwnerData.assign_animal_to_additional_owner(
    animal_owner_data = animal_owner_1,
    additional_owners = additional_owners,
    household_several_appt = household_several_appt
)
# concatenate the two sets of matches between animals and owners
animal_owner = pd.concat(
    [animal_owner_1, animal_additional_owner]
).reset_index(drop=True)

# Step 3 - Check if some animals were not assign to any owner
# and if there are some, assign them randomly to owners
logging.info(f"Assigning remaining animals to owners, if any.")
left_animal_owner = OwnerData.assign_left_animals(
    animal_data = animal_data,
    animal_owner_data = animal_owner
)

# concatenate the two sets of matches between animals and owners
animal_owner_data = pd.concat(
    [animal_owner, left_animal_owner]
).reset_index(drop=True)


# Assign owners to appointments
logging.info(f"Assigning owners to appointments.")
appointment_data = Appointment.assign_owner_to_appt(
    animal_owner_data = animal_owner_data,
    appointment_data = appointment_data,
    animal_data = animal_data
)

# Sort owner_data by appt_date
logging.info(f"Sorting tuples in relation Owner by first appointment date.")
owner_data = OwnerData.sort_owner_by_appt_date(
    appointment_data = appointment_data,
    owner_data = owner_data
)
# Add id_owner to owner_data
logging.info(f"Assigning unique id values (id_owner) to tuples in "
             f"Owner relation.")
owner_data = OwnerData.assign_id_owner(
    owner_data = owner_data,
    pk_column_name = 'id_owner',
    starting_id_value = 85
)

# Assign id_owner to animal_owner_data
logging.info(f"Assigning FK values id_owner to Animal relation.")
animal_owner_data = OwnerData.assign_animal_owner_id_owner(
    animal_owner_data = animal_owner_data,
    owner_data = owner_data
)

# Add id_animal_owner to animal_owner_data
logging.info(f"Assigning FK values id_animal to Owner relation.")
animal_owner_data = OwnerData.assign_id_animal_owner(
    animal_owner_data = animal_owner_data,
    pk_column_name = 'id_animal_owner',
    starting_id_value = 1
)

logging.info(f"Assigning FK values id_owner to Appointment relation.")
appointment_data = Appointment.assign_appointment_id_owner(
    owner_data = owner_data,
    appointment_data = appointment_data
)


#================================================================
# Finalize relations
logging.info(f"Adjusting the struture of all relations to match "
             f"the clean database schema.")

# Relation animal
animals_columns = ['id_animal', 'species', 'breed', 'name', 'id_microchip', 'gender', 'dob', 'hash_id']
animal_rel = animal_data[animals_columns]

# Relation animal_weight
animals_weight_columns = ['id_weight', 'id_animal', 'id_appointment', 'weight']
animal_weight_rel = animal_weight_data[animals_weight_columns]

# Relation microchip_code
microchip_code_columns = ['id_code', 'code', 'brand', 'provider', 'country']
microchip_code_rel = microchip_code_data[microchip_code_columns]

# Relation microchip
microchip_data = microchip_data.rename(columns={'microchip_number': 'number'})
microchip_columns = ['id_microchip', 'id_code', 'number', 'implant_date', 'location']
microchip_rel = microchip_data[microchip_columns]

# Relation owner
owner_columns = ['id_owner', 'first_name', 'last_name', 'address', 'city', 'postal_code', 'phone_number']
owner_rel = owner_data[owner_columns]

# Relation animal_owner
animal_owner_columns = ['id_animal_owner', 'id_microchip', 'id_owner']
animal_owner_rel = animal_owner_data[animal_owner_columns]

# Relation service
service_columns = ['id_service', 'service_name']
service_rel = service_data[service_columns]

# Relation appointment
appointment_columns = ['id_appointment', 'id_animal', 'appt_reason', 'id_owner']
appointment_rel = appointment_data[appointment_columns]

# Relation appointment_service
appointment_services_columns = ['id_appointment_service', 'id_appointment', 'id_service']
appointment_services_rel = appointment_services_data[appointment_services_columns]

# Relation slot
slot_columns = ['id_slot', 'id_doctor', 'date', 'time', 'type']
slot_rel = revised_slot_data[slot_columns]

# Relation appointment_slot
appointment_slot_columns = ['id_appointment_slot', 'id_appointment', 'id_slot']
appointment_slot_rel = appointment_slot_data[appointment_slot_columns]

# Relation doctor
doctor_columns = ['id_doctor', 'first_name', 'last_name', 'license_number', 'specialty', 
'start_date', 'end_date', 'max_monthly_hours', 'period_start_date', 'period_end_date']
doctor_rel = doctor_data[doctor_columns]

# Relation doctor_historization
doctor_histo_columns = ['id_doctor_histo', 'id_doctor', 'first_name', 'last_name', 'specialty',
'license_number', 'period_start_date', 'period_end_date', 'max_monthly_hours']
doctor_historization_rel = doctor_historization_data[doctor_histo_columns]


#================================================================
# Save relations data in csv files
logging.info(f"Saving the data of the clean relations into new csv files.")

microchip_code_rel.to_csv('working_data/microchip_code_rel.csv')
microchip_rel.to_csv('working_data/microchip_rel.csv')
animal_rel.to_csv('working_data/animal_rel.csv')
animal_weight_rel.to_csv('working_data/animal_weight_rel.csv')
service_rel.to_csv('working_data/service_rel.csv')
appointment_rel.to_csv('working_data/appointment_rel.csv')
appointment_services_rel.to_csv('working_data/appointment_services_rel.csv')
slot_rel.to_csv('working_data/slot_rel.csv')
appointment_slot_rel.to_csv('working_data/appointment_slot_rel.csv')
owner_rel.to_csv('working_data/owner_rel.csv')
animal_owner_rel.to_csv('working_data/animal_owner_rel.csv')
doctor_rel.to_csv('working_data/doctor_rel.csv')
doctor_historization_rel.to_csv('working_data/doctor_historization_rel.csv')