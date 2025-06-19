Perfect Pet is an open-source software to generate and pollute relational database instances of the fictional Perfect Pet veterinary clinic. 
It was developed to address the lack of databases containing artificial unicity among publicly available ones used to test and compare data quality methods. 

Artificial unicity is a pernicious form of redundancy found in operational databases due to the heavy reliance on surrogate keys, which hinders one’s capacity to efficiently and thoroughly clean them for analytical purposes.

# How to use the software?

First, you generate a clean database instance with the following schema:
![perfect_pet_data_model_clean](https://github.com/user-attachments/assets/27d897e4-c420-4b86-9f6a-055b8f742cf4)


This instance can be used as “ground truth” and be used for comparison with the results of  polluted datasets cleaning.

Second, you pollute the instance’s data by inserting artificial unicity. Copies of the clean relations’ data are created and modified to transform the database schema to the following:
![perfect_pet_data_model](https://github.com/user-attachments/assets/b16169ab-5f2b-4f88-9bb1-602c64f237fe)


Tuples are then duplicated, and values of surrogate keys are modified to remain unique.

Lastly, you can pollute the data values of the instance suffering from artificial unicity by adding basic data quality issues such as insertion of random characters, swapping characters, transforming all characters in upper cases, etc.


## Generate a clean database instance’s data

First, generate a clean instance of the Perfect Pet database by running the file db_generation.py:

```bash
python db_generation.py
```

Note: you can specify the value of several parameters to control the data generation (l.31 to 195 of file db_generation.py).
The most important parameter is the number of animals (l.33) to be represented in relation Animal (i.e. the number of tuples). It will dictate the size of most other relations (excluding Microchip_Code and Service since their sizes are fixed). 

The relations’ data will be saved in csv files in the folder “working_data”.

### Create a PostgreSQL database and clean schema

Files located in folders postgresql and postgresql/clean_db can be used to create the database and schema and upload the data.

After creating the database, run the content of the file perfectpet_clean_data_model.sql to create the schema model. You can use pgAdmin or any GUI compatible with PostgreSQL to do so.
Then run the file load_clean_data_postgresql.py:

```bash
python load_clean_data_postgresql.py
```

The code loads the csv data files from the folder “working_data”, so ensure that they are not moved after their generation. 

Note: if you changed the name of the schema in file perfectpet_clean_data_model.sql, you need to reflect it in file load_clean_data_postgresql.py.


## Pollute the database instance with artificial unicity

You can control the levels of artificial unicity injected in relations Animal, Microchip_Code, and Service. The level of artificial unicity in Animal will dictate those in relations Microchip and Owner. Levels are fixed for other relations. A 25%
Level means that tuples in Animal were duplicated up to 25% of the number of associated non-first appointments.

First, set the artificial levels you wish to use for relations Animal, Microchip_Code, and Service in file db_pollution_au.py (l. 31 to 41), then run the file:

```bash
python db_pollution_au.py
```

The polluted relations’ data will be saved in the folder “working_data”.

### Create the artificial unicity polluted schema

Files located in the folder postgresql/polluted_au_db can be used to create the schema and upload the data (in the database created earlier).

After creating the database, run the content of the file perfectpet_polluted_au_model.sql to create the schema model. You can use pgAdmin or any GUI compatible with PostgreSQL to do so.
Then run the file load_polluted_au_postgresql.py:

```bash
python load_polluted_au_postgresql.py
```

The code loads the csv data files from folder “working_data”, so ensure that they are not moved after their generation. 

Note: if you changed the name of the schema in file perfectpet_polluted_au_model.sql, you need to reflect it in file load_polluted_au_postgresql.py.


## Further pollute the data

All data pollution instructions are specified in the file db_pollution_data.py. You can modify it to adjust the levels of pollution or the attributes to pollute. Then, run the file:

```bash
python db_pollution_data.py
```
The polluted relations’ data will be saved in the folder “working_data”.


### Create the data-polluted schema

Files located in the folder postgresql/polluted_au_data_db can be used to create the schema and upload the data (in the database created earlier).

After creating the database, run the content of the file perfectpet_polluted_data_model.sql to create the schema model. You can use pgAdmin or any GUI compatible with PostgreSQL to do so.
Then run the file load_polluted_data_postgresql.py:

```bash
python load_polluted_data_postgresql.py
```

The code loads the csv data files from folder “working_data”, so ensure that they are not moved after their generation. 

Note: if you changed the name of the schema in file perfectpet_polluted_data_model.sql, you need to reflect it in file load_polluted_data_postgresql.py.



You now have three instances of the Perfect Pet database, one clean, one only polluted with artificial unicity and one polluted with both artificial unicity and data quality pollution. You can test your data quality methods on your polluted instances and compare the results with the clean instance that serves as “ground truth”.

For sensitivity analyses, you can pollute a clean instance with various levels of pollution (artificial unicity and data pollution), and compare the results of your methods on these different instances.
