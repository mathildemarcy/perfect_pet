-- Replace 'perfect_pet_x' with the database name that you selected
-- We suggest to only replace 'x' by the value of 'nb_animals' used
-- to generate the clean database

-- Database: perfect_pet_x
CREATE DATABASE IF NOT EXISTS perfect_pet_x
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'C'
    LC_CTYPE = 'C'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    TEMPLATE = template1
    IS_TEMPLATE = False;