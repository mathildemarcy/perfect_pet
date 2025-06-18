-- Create the schema to store the polluted version of the database
CREATE SCHEMA IF NOT EXISTS polluted_db_au;

--###############################################################################################
-- Table: polluted_db_au.animal
CREATE TYPE polluted_db_au.animal_species AS ENUM
    ('canine', 'feline');
CREATE TYPE polluted_db_au.animal_gender AS ENUM
    ('F', 'M');

-- DROP TABLE IF EXISTS polluted_db_au.animal;
CREATE TABLE IF NOT EXISTS polluted_db_au.animal
(
    id_animal integer NOT NULL,
    species polluted_db_au.animal_species NOT NULL,
    breed character varying(50) COLLATE pg_catalog."default" NOT NULL,
    name character varying(50) COLLATE pg_catalog."default" NOT NULL,
    id_microchip integer NOT NULL,
    gender polluted_db_au.animal_gender NOT NULL,
    dob date,
    weight double precision,
    hash_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    id_owner integer NOT NULL,
    CONSTRAINT animal_pkey PRIMARY KEY (id_animal)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS polluted_db_au.animal
    OWNER to postgres;
-- Index: idx_animal_id_owner
-- DROP INDEX IF EXISTS polluted_db_au.idx_animal_id_owner;
CREATE INDEX IF NOT EXISTS idx_animal_id_owner
    ON polluted_db_au.animal USING btree
    (id_owner ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_animal_id_microchip
-- DROP INDEX IF EXISTS polluted_db_au.idx_animal_id_microchip;
CREATE INDEX IF NOT EXISTS idx_animal_id_microchip
    ON polluted_db_au.animal USING btree
    (id_microchip ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: polluted_db_au.microchip_code
-- DROP TABLE IF EXISTS polluted_db_au.microchip_code;
CREATE TABLE IF NOT EXISTS polluted_db_au.microchip_code
(
    id_code integer NOT NULL,
    code character varying(15) COLLATE pg_catalog."default" NOT NULL,
    brand character varying(50) COLLATE pg_catalog."default" NOT NULL,
    provider character varying(50) COLLATE pg_catalog."default",
    country character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT code_pkey PRIMARY KEY (id_code)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS polluted_db_au.microchip_code
    OWNER to postgres;

-- Index: idx_microchip_microchip_code
-- DROP INDEX IF EXISTS polluted_db_au.idx_microchip_microchip_code;
CREATE INDEX IF NOT EXISTS idx_microchip_cidx_microchip_microchip_codeode
    ON polluted_db_au.microchip_code USING btree
    (code ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_microchip_code_brand
-- DROP INDEX IF EXISTS polluted_db_au.idx_microchip_code_brand;
CREATE INDEX IF NOT EXISTS idx_microchip_code_brand
    ON polluted_db_au.microchip_code USING btree
    (brand ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: polluted_db_au.microchip

CREATE TYPE polluted_db_au.microchip_location AS ENUM
    ('left_lateral_neck', 'between_shoulders', 'midline_cervicals', 'right_lateral_neck');

-- DROP TABLE IF EXISTS polluted_db_au.microchip;
CREATE TABLE IF NOT EXISTS polluted_db_au.microchip
(
    id_microchip integer NOT NULL,
    id_code integer NOT NULL,
    "number" character varying(50) COLLATE pg_catalog."default" NOT NULL,
    implant_date date,
    location polluted_db_au.microchip_location NOT NULL,
    id_owner integer NOT NULL,
    CONSTRAINT microchip_pkey PRIMARY KEY (id_microchip)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS polluted_db_au.microchip
    OWNER to postgres;

-- Index: idx_microchip_id_code
-- DROP INDEX IF EXISTS polluted_db_au.idx_microchip_id_code;
CREATE INDEX IF NOT EXISTS idx_microchip_id_code
    ON polluted_db_au.microchip USING btree
    (id_code ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: polluted_db_au.owner

-- DROP TABLE IF EXISTS polluted_db_au.owner;
CREATE TABLE IF NOT EXISTS polluted_db_au.owner
(
    id_owner integer NOT NULL,
    first_name character varying(150) COLLATE pg_catalog."default" NOT NULL,
    last_name character varying(150) COLLATE pg_catalog."default" NOT NULL,
    address character varying(150) COLLATE pg_catalog."default",
    city character varying(50) COLLATE pg_catalog."default" NOT NULL,
    postal_code character varying(20) COLLATE pg_catalog."default",
    phone_number character varying(50) COLLATE pg_catalog."default",
    id_animal integer,
    CONSTRAINT owner_pkey PRIMARY KEY (id_owner)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS polluted_db_au.owner
    OWNER to postgres;

-- Index: idx_owner_id_animal
-- DROP INDEX IF EXISTS polluted_db_au.idx_owner_id_animal;
CREATE INDEX IF NOT EXISTS idx_owner_id_animal
    ON polluted_db_au.owner USING btree
    (id_animal ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_owner_first_name
-- DROP INDEX IF EXISTS polluted_db_au.idx_owner_first_name;
CREATE INDEX IF NOT EXISTS idx_owner_first_name
    ON polluted_db_au.owner USING btree
    (first_name ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: polluted_db_au.doctor

CREATE TYPE polluted_db_au.medical_specialty AS ENUM
    ('surgeon', 'generalist');

-- DROP TABLE IF EXISTS polluted_db_au.doctor;
CREATE TABLE IF NOT EXISTS polluted_db_au.doctor
(
    id_doctor integer NOT NULL,
    first_name character varying(150) COLLATE pg_catalog."default" NOT NULL,
    last_name character varying(150) COLLATE pg_catalog."default" NOT NULL,
    specialty polluted_db_au.medical_specialty NOT NULL,
    license_number character varying(25) COLLATE pg_catalog."default" NOT NULL,
    start_date date NOT NULL,
    end_date date,
    period_start_date date NOT NULL,
    period_end_date date NOT NULL,
    max_monthly_hours double precision DEFAULT 0 NOT NULL,
    CONSTRAINT doctor_pkey PRIMARY KEY (id_doctor)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS polluted_db_au.doctor
    OWNER to postgres;

-- Index: idx_doctor_specialy
-- DROP INDEX IF EXISTS polluted_db_au.idx_doctor_specialy;
CREATE INDEX IF NOT EXISTS idx_doctor_specialy
    ON polluted_db_au.doctor USING btree
    (specialty ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: polluted_db_au.service

-- DROP TABLE IF EXISTS polluted_db_au.service;
CREATE TABLE IF NOT EXISTS polluted_db_au.service
(
    id_service integer NOT NULL,
    service_name character varying(50) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT service_pkey PRIMARY KEY (id_service)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS polluted_db_au.service
    OWNER to postgres;

-- Index: idx_service_name
-- DROP INDEX IF EXISTS polluted_db_au.idx_service_name;
CREATE INDEX IF NOT EXISTS idx_service_name
    ON polluted_db_au.service USING btree
    (service_name ASC NULLS LAST)
    TABLESPACE pg_default;


--###############################################################################################
-- Table: polluted_db_au.appointment

CREATE TYPE polluted_db_au.appointment_reason AS ENUM
    ('annual_visit', 'sick_pet', 'injured_pet', 'initial_visit', 'follow_up', 'surgery', 'follow_up_surgery');

-- DROP TABLE IF EXISTS polluted_db_au.appointment;
CREATE TABLE IF NOT EXISTS polluted_db_au.appointment
(
    id_appointment integer NOT NULL,
    id_animal integer NOT NULL,
    appt_reason polluted_db_au.appointment_reason NOT NULL,
    id_service integer NOT NULL,
    id_owner integer NOT NULL,
    CONSTRAINT appointment_pkey PRIMARY KEY (id_appointment)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS polluted_db_au.appointment
    OWNER to postgres;

-- Index: idx_appointment_id_animal
-- DROP INDEX IF EXISTS polluted_db_au.idx_appointment_id_animal;
CREATE INDEX IF NOT EXISTS idx_appointment_id_animal
    ON polluted_db_au.appointment USING btree
    (id_animal ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_appointment_id_service
-- DROP INDEX IF EXISTS polluted_db_au.idx_appointment_id_service;
CREATE INDEX IF NOT EXISTS idx_appointment_id_service
    ON polluted_db_au.service USING btree
    (id_service ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: polluted_db_au.slot

CREATE TYPE polluted_db_au.slot_type AS ENUM
    ('regular', 'overtime');

-- DROP TABLE IF EXISTS polluted_db_au.slot;
CREATE TABLE IF NOT EXISTS polluted_db_au.slot
(
    id_slot integer NOT NULL,
    id_doctor integer NOT NULL,
    date date NOT NULL,
    "time" time NOT NULL,
    type polluted_db_au.slot_type NOT NULL,
    CONSTRAINT slot_pkey PRIMARY KEY (id_slot)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS polluted_db_au.slot
    OWNER to postgres;

-- Index: idx_slot_doctor
-- DROP INDEX IF EXISTS polluted_db_au.idx_slot_doctor;
CREATE INDEX IF NOT EXISTS idx_slot_doctor
    ON polluted_db_au.slot USING btree
    (id_doctor ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_slot_date
-- DROP INDEX IF EXISTS polluted_db_au.idx_slot_date;
CREATE INDEX IF NOT EXISTS idx_slot_date
    ON polluted_db_au.slot USING btree
    (date ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: polluted_db_au.appointment_slot

-- DROP TABLE IF EXISTS polluted_db_au.appointment_slot;
CREATE TABLE IF NOT EXISTS polluted_db_au.appointment_slot
(
    id_appointment_slot integer NOT NULL,
    id_appointment integer NOT NULL,
    id_slot integer NOT NULL,
    CONSTRAINT appointment_slot_pkey PRIMARY KEY (id_appointment_slot)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS polluted_db_au.appointment_slot
    OWNER to postgres;

-- Index: idx_appointment_slot_appointment
-- DROP INDEX IF EXISTS polluted_db_au.idx_appointment_slot_appointment;
CREATE INDEX IF NOT EXISTS idx_appointment_slot_appointment
    ON polluted_db_au.appointment_slot USING btree
    (id_appointment ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_appointment_slot_slot
-- DROP INDEX IF EXISTS polluted_db_au.idx_appointment_slot_slot;
CREATE INDEX IF NOT EXISTS idx_appointment_slot_slot
    ON polluted_db_au.appointment_slot USING btree
    (id_slot ASC NULLS LAST)
    TABLESPACE pg_default;