This file describes the generation of the clean database instance including 5,000 tuples in relation Animal (referred to 5000_0), and its polluted version: 5000_100, in which artificial unicity was injected using a factor of 100%.

# Initial set-up

From the terminal, go to the repository folder.
Make sur that all the packages listed in file requirements.txt are installed. 
Replace "perfect_pet_x" by "perfect_pet_5000" (or any database name you would like to use) in the file postgresql/create_database_perfect_pet.sql, then run it to create the database.


# Generate clean database instance

Set paramater "nb_animals" to 5,000 in file "db_generation.py" line 33. Then run command:

```bash
python db_generation.py
```
The clean relation's data are extracted in folder "working_data". 13 files were generated (animal_rel.csv, animal_weight_rel.csv, animal_owner_rel.csv, appointment_rel.csv, appointment_services_rel.csv, appointment_slot_rel.csv, doctor_rel.csv, doctor_hostorization_rel.csv, microchip_code_rel.csv, microchip_rel.csv, owner_rel.csv, service_rel.csv, slot_rel.csv)


## Create and fill the clean_db schema

### Create schema
From pgAdmin or any other GUI tool of your choice compatible with PostgreSQL, run the script in postgresql/clean_db/perfectpet_clean_data_model.sql to create the schema dedicated to store the clean database instance. 

### Load the data
Then load the generated data to the schema by running in the terminal:

```bash
python postgresql/clean_db/load_clean_data_postgresql.py
```

### Add the foreign key constraints
Once the data is uploaded, run in pgAdmin or any other GUI the script in postgresql/clean_db/add_fk_constraints_clean_db.sql to add the foreign key constraints to the clean_db schema.


# Pollute clean data with artificial unicity

Verify that the following parameters are the set to the values used to generate 5000_100:
microchip_code_augmentation_rate = 0.5 (line 31)
service_augmentation_rate = 0.5 (line 35)
animal_augmentation_rate = 1 (line 39)

Then run command:

```bash
python db_pollution_au.py
```

The polluted with artificial unicity relation's data are extracted in folder "working_data". 9 files were generated (animal_au.csv, appointment_au.csv, appointment_slot_au.csv, doctor_au.csv, microchip_code_au.csv, microchip_au.csv, owner_au.csv, service_au.csv, slot_au.csv).

## Create and fill the polluted_db_au schema

### Create schema
From pgAdmin or any other GUI tool of your choice, run the script in postgresql/polluted_au_db/perfectpet_polluted_au_model.sql to create the schema dedicated to store the database instance polluted with 100% factor artificial unicity.

You can modify the schema's name from "polluted_db_au" to "polluted_db_au_100" to indicate the artificial unicity factor.

### Load the data
Then load the generated data to the schema by running in the terminal:

```bash
python postgresql/polluted_au_db/load_polluted_au_postgresql.py
```

### Add the foreign key constraints
Once the data is uploaded, run in pgAdmin or any other GUI the script in postgresql/polluted_au_db/add_fk_constraints_polluted_au_db.sql to add the foreign key constraints to the clean_db schema.

Note that if you changed to schema's name, you also need to modify it in the files "load_polluted_au_postgresql.py" and "add_fk_constraints_polluted_au_db.sql".
