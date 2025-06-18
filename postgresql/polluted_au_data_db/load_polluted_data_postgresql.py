# Code to load the generated and polluted data in the schema 'polluted_db'
import pandas as pd
import getpass
from sqlalchemy import create_engine, text

#================================================================
# Save relations data in csv files
## Relation microchip_code
microchip_code = pd.read_csv('working_data/microchip_code_au_dirty.csv')
## Relation microchip
microchip = pd.read_csv('working_data/microchip_au_dirty.csv')
## Relation owner
owner = pd.read_csv('working_data/owner_au_dirty.csv')
## Relation animal
animal = pd.read_csv('working_data/animal_au_dirty.csv')
## Relation doctor
doctor = pd.read_csv('working_data/doctor_au_dirty.csv')
## Relation service
service = pd.read_csv('working_data/service_au_dirty.csv')
## Relation appointment
appointment = pd.read_csv('working_data/appointment_au.csv')
## Relation slot
slot = pd.read_csv('working_data/slot_au.csv')
## Relation appointment_slot
appointment_slot = pd.read_csv('working_data/appointment_slot_au.csv')

#================================================================
# Connect to DB
# Get connection parameters from user
# Prompt the user for connection details
db_user = input("Enter PostgreSQL username: ")
db_password = getpass.getpass("Enter PostgreSQL password: ")
db_host = input("Enter host (e.g., localhost): ")
db_port = input("Enter port (e.g., 5432): ")
db_name = input("Enter database name: ")

connection_url = (
    f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)
engine = create_engine(connection_url)

schema_name='polluted_db'
#================================================================
# Load data

#----------------------------------------------------------------------------
# Relation microchip_code
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE polluted_db.microchip_code"))
microchip_code_columns = ['id_code', 'code', 'brand', 'provider', 'country']
microchip_code[microchip_code_columns].to_sql(
    'microchip_code', engine, schema=schema_name, if_exists='append', index=False
)

#----------------------------------------------------------------------------
# Relation microchip
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE polluted_db.microchip"))
microchip_columns = ['id_microchip', 'id_code', 'number', 'implant_date',
                     'location', 'id_owner']
microchip[microchip_columns].to_sql(
    'microchip', engine, schema=schema_name, if_exists='append', index=False
)

#----------------------------------------------------------------------------
# Relation animal
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE polluted_db.animal"))
animal_columns = ['id_animal', 'species', 'breed', 'name', 'id_microchip', 
                  'gender', 'dob', 'weight', 'hash_id', 'id_owner']
animal[animal_columns].to_sql(
    'animal', engine, schema=schema_name, if_exists='append', index=False
)

#----------------------------------------------------------------------------
# Relation owner
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE polluted_db.owner"))
owner_columns = ['id_owner', 'first_name', 'last_name', 'address', 'city',
                 'postal_code', 'phone_number', 'id_animal']
owner[owner_columns].to_sql(
    'owner', engine, schema=schema_name, if_exists='append', index=False
)

#----------------------------------------------------------------------------
# Relation doctor
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE polluted_db.doctor"))
doctor_columns = ['id_doctor', 'first_name', 'last_name', 'specialty',
                  'license_number', 'start_date', 'end_date',
                  'period_start_date', 'period_end_date', 'max_monthly_hours']
doctor[doctor_columns].to_sql(
    'doctor', engine, schema=schema_name, if_exists='append', index=False
)

#----------------------------------------------------------------------------
# Relation service
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE polluted_db.service"))
service_columns = ['id_service', 'service_name'] 
service[service_columns].to_sql(
    'service', engine, schema=schema_name, if_exists='append', index=False
)

#----------------------------------------------------------------------------
# Relation appointment
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE polluted_db.appointment"))
appointment_columns = ['id_appointment', 'id_animal', 'appt_reason',
                       'id_service', 'id_owner']
appointment[appointment_columns].to_sql(
    'appointment', engine, schema=schema_name, if_exists='append', index=False
)

#----------------------------------------------------------------------------
# Relation slot
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE polluted_db.slot"))
slot_columns = ['id_slot', 'id_doctor', 'date', 'time', 'type']
slot[slot_columns].to_sql(
    'slot', engine, schema=schema_name, if_exists='append', index=False
)

#----------------------------------------------------------------------------
# Relation appointment_slot
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE polluted_db.appointment_slot"))
appointment_slot_columns = ['id_appointment_slot', 'id_appointment', 'id_slot']
appointment_slot[appointment_slot_columns].to_sql(
    'appointment_slot', engine, schema=schema_name, if_exists='append', index=False
)