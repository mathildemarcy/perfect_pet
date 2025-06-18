import pandas as pd
import numpy as np
from au_pollutor.au_insertion import au_transformator
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="- %(levelname)s - %(asctime)s - %(message)s"
)

#================================================================
# Import data from clean relations (not suffering from artificial unicity)
logging.info(f"Importing csv files containing the data of the relations "
             f"to be polluted with artificila unicity.")

animal_rel = pd.read_csv('working_data/animal_rel.csv')
animal_weight_rel = pd.read_csv('working_data/animal_weight_rel.csv')
microchip_code_rel = pd.read_csv('working_data/microchip_code_rel.csv')
microchip_rel = pd.read_csv('working_data/microchip_rel.csv')
owner_rel = pd.read_csv('working_data/owner_rel.csv')
animal_owner_rel = pd.read_csv('working_data/animal_owner_rel.csv')
appointment_rel = pd.read_csv('working_data/appointment_rel.csv')
service_rel = pd.read_csv('working_data/service_rel.csv')
appointment_services_rel = pd.read_csv('working_data/appointment_services_rel.csv')
doctor_rel = pd.read_csv('working_data/doctor_rel.csv')
doctor_historization_rel = pd.read_csv('working_data/doctor_historization_rel.csv')
slot_rel = pd.read_csv('working_data/slot_rel.csv')
appointment_slot_rel = pd.read_csv('working_data/appointment_slot_rel.csv')


microchip_code_augmentation_rate = 0.5
logging.info(f"The augmentation rate for relation Microchip_Code "
             f"was set to {microchip_code_augmentation_rate}.")

service_augmentation_rate = 0.5
logging.info(f"The augmentation rate for relation Service "
             f"was set to {service_augmentation_rate}.")

animal_augmentation_rate = 0.75
logging.info(f"The augmentation rate for relation Animal "
             f"was set to {animal_augmentation_rate}.")

#================================================================
# Instantiate object au_transformator
Transf = au_transformator(
    microchip_code_rel = microchip_code_rel,
    microchip_rel = microchip_rel,
    animal_rel = animal_rel,
    animal_weight_rel = animal_weight_rel,
    owner_rel = owner_rel,
    animal_owner_rel = animal_owner_rel,
    service_rel = service_rel,
    appointment_rel = appointment_rel,
    appointment_service_rel = appointment_services_rel,
    slot_rel = slot_rel,
    appointment_slot_rel = appointment_slot_rel,
    doctor_rel = doctor_rel,
    doctor_historization_rel = doctor_historization_rel
)

#================================================================
# Pollute relations

## Relation microchip_code
logging.info(f"Introducing artificial unicity in relation Microchip_Code.")

microchip_code_au = Transf.transform_microchip_code(
    augmentation_rate = microchip_code_augmentation_rate,
    starting_id_value = 3
)
## Relation service
logging.info(f"Introducing artificial unicity in relation Service.")

service_au = Transf.transform_service(
    augmentation_rate = service_augmentation_rate,
    starting_id_value = 1
)
## Relation animal
logging.info(f"Introducing artificial unicity in relation Animal.")

animal_au = Transf.transform_animal(
    augmentation_rate = animal_augmentation_rate,
    starting_id_value = 47
)
## Relation microchip
logging.info(f"Introducing artificial unicity in relation Microchip.")

microchip_au = Transf.transform_microchip(
    microchip_code_au = microchip_code_au,
    starting_id_value = 34
)
## Relation appointment
logging.info(f"Introducing artificial unicity in relation Appointment.")

appointment_au = Transf.transform_appointment(
    animal_au = animal_au
)
## Relation appointment_slot
logging.info(f"Introducing artificial unicity in relation Slot.")

appointment_slot_au = Transf.transform_appointment_slot(
    appointment_au = appointment_au,
    starting_id_value = 17
)
## Relation doctor
logging.info(f"Introducing artificial unicity in relation Doctor.")

doctor_au = Transf.transform_doctor()
## Relation slot
slot_au = Transf.transform_slot(
    doctor_au = doctor_au
)
## Relation owner
logging.info(f"Introducing artificial unicity in relation Owner.")

owner_au = Transf.transform_owner(
    appointment_au = appointment_au,
    animal_au = animal_au,
    starting_id_value = 85
)

#================================================================
# Update some foreign and surrogate keys of the polluted relations

## Update FK id_microchip in relation animal
logging.info(f"Updating values of FK id_microchip in relation Animal.")
animal_au = Transf.update_animal_id_microchip(
    microchip_au = microchip_au,
    animal_au = animal_au
)

## Update FK id_owner in relation animal
logging.info(f"Updating values of FK id_owner in relation Animal.")

animal_au = Transf.update_animal_id_owner(
    animal_au = animal_au,
    owner_au = owner_au
)

## Assign id_owner in relation animal when missing
logging.info(f"XXX.")
animal_au = Transf.assign_missing_owner_to_animal(
    animal_au = animal_au,
    animal_owner_rel = animal_owner_rel,
    owner_au = owner_au
)

## Update FK id_owner in relation microchip
logging.info(f"Updating values of FK id_owner in relation Microchip.")

microchip_au = Transf.update_microchip_id_owner(
    microchip_au = microchip_au,
    animal_au = animal_au
)
## Update SK hash_id in relation animal
logging.info(f"Updating values of SK hash_id in relation Animal.")

animal_au = Transf.update_animal_hash_id(
    animal_au = animal_au
)

## Update FK id_owner in relation appointent
logging.info(f"Updating values of FK id_owner in relation Appointment.")

appointment_au = Transf.update_appointment_id_owner(
    appointment_au = appointment_au,
    owner_au = owner_au
)
#----------------------------------------------------------
# Finalize the relations by adjusting their schema
logging.info(f"Adjusting the struture of all relations to match the database schema.")

## Relation microchip_code
microchip_code_au = Transf.finalize_microchip_code(
    microchip_code_au = microchip_code_au
)
## Relation microchip
microchip_au = Transf.finalize_microchip(
    microchip_au = microchip_au
)
## Relation owner
owner_au = Transf.finalize_owner(
    owner_au = owner_au
)
## Relation animal
animal_au = Transf.finalize_animal(
    animal_au = animal_au
)
## Relation service
service_au = Transf.finalize_service(
    service_au = service_au
)
## Relation appointment
appointment_au = Transf.finalize_appointment(
    appointment_au = appointment_au
)
## Relation appointment_slot
appointment_slot_au = Transf.finalize_appointment_slot(
    appointment_slot_au = appointment_slot_au
)
## Relation slot
slot_au = Transf.finalize_slot(
    slot_au = slot_au
)
## Relation doctor
doctor_au = Transf.finalize_doctor(
    doctor_au = doctor_au
)

#================================================================
# Save relations data in csv files
logging.info(f"Saving the data of relations now suffering from "
             f"artificial unicity into new csv files.")

## Relation microchip_code
microchip_code_au.to_csv('working_data/microchip_code_au.csv')
## Relation microchip
microchip_au.to_csv('working_data/microchip_au.csv')
## Relation owner
owner_au.to_csv('working_data/owner_au.csv')
## Relation animal
animal_au.to_csv('working_data/animal_au.csv')
## Relation service
service_au.to_csv('working_data/service_au.csv')
## Relation appointment
appointment_au.to_csv('working_data/appointment_au.csv')
## Relation appointment_slot
appointment_slot_au.to_csv('working_data/appointment_slot_au.csv')
## Relation slot
slot_au.to_csv('working_data/slot_au.csv')
## Relation doctor
doctor_au.to_csv('working_data/doctor_au.csv')