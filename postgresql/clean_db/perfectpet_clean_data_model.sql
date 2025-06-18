-- Create the schema to store the clean version of the database
CREATE SCHEMA IF NOT EXISTS clean_db;

--###############################################################################################
-- Table: clean_db.animal
CREATE TYPE clean_db.animal_species AS ENUM
    ('canine', 'feline');
CREATE TYPE clean_db.animal_gender AS ENUM
    ('F', 'M');

-- DROP TABLE IF EXISTS clean_db.animal;
CREATE TABLE IF NOT EXISTS clean_db.animal
(
    id_animal integer NOT NULL,
    species clean_db.animal_species NOT NULL,
    breed character varying(50) COLLATE pg_catalog."default" NOT NULL,
    name character varying(50) COLLATE pg_catalog."default" NOT NULL,
    id_microchip integer NOT NULL,
    gender clean_db.animal_gender NOT NULL,
    dob date,
    hash_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT animal_pkey PRIMARY KEY (id_animal)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS clean_db.animal
    OWNER to postgres;

-- Index: idx_animal_id_microchip
-- DROP INDEX IF EXISTS clean_db.idx_animal_id_microchip;
CREATE INDEX IF NOT EXISTS idx_animal_id_microchip
    ON clean_db.animal USING btree
    (id_microchip ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: clean_db.animal_weight

-- DROP TABLE IF EXISTS clean_db.animal_weight;
CREATE TABLE IF NOT EXISTS clean_db.animal_weight
(
    id_weight integer NOT NULL,
    id_animal integer NOT NULL,
    id_appointment integer NOT NULL,
    weight double precision,
    CONSTRAINT animal_weight_pkey PRIMARY KEY (id_weight)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS clean_db.animal_weight
    OWNER to postgres;

-- Index: idx_animal_weight_id_animal
-- DROP INDEX IF EXISTS clean_db.idx_animal_id_animal;
CREATE INDEX IF NOT EXISTS idx_animal_weight_id_animal
    ON clean_db.animal_weight USING btree
    (id_animal ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: clean_db.microchip_code
-- DROP TABLE IF EXISTS clean_db.microchip_code;
CREATE TABLE IF NOT EXISTS clean_db.microchip_code
(
    id_code integer NOT NULL,
    code character varying(10) COLLATE pg_catalog."default" NOT NULL,
    brand character varying(50) COLLATE pg_catalog."default" NOT NULL,
    provider character varying(50) COLLATE pg_catalog."default",
    country character varying(20) COLLATE pg_catalog."default",
    CONSTRAINT code_pkey PRIMARY KEY (id_code)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS clean_db.microchip_code
    OWNER to postgres;

-- Index: idx_microchip_microchip_code
-- DROP INDEX IF EXISTS clean_db.idx_microchip_microchip_code;
CREATE INDEX IF NOT EXISTS idx_microchip_cidx_microchip_microchip_codeode
    ON clean_db.microchip_code USING btree
    (code ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_microchip_code_brand
-- DROP INDEX IF EXISTS clean_db.idx_microchip_code_brand;
CREATE INDEX IF NOT EXISTS idx_microchip_code_brand
    ON clean_db.microchip_code USING btree
    (brand ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: clean_db.microchip

CREATE TYPE clean_db.microchip_location AS ENUM
    ('left_lateral_neck', 'between_shoulders', 'midline_cervicals', 'right_lateral_neck');

-- DROP TABLE IF EXISTS clean_db.microchip;
CREATE TABLE IF NOT EXISTS clean_db.microchip
(
    id_microchip integer NOT NULL,
    id_code integer NOT NULL,
    "number" character varying(50) COLLATE pg_catalog."default" NOT NULL,
    implant_date date,
    location clean_db.microchip_location NOT NULL,
    CONSTRAINT microchip_pkey PRIMARY KEY (id_microchip)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS clean_db.microchip
    OWNER to postgres;

-- Index: idx_microchip_id_code
-- DROP INDEX IF EXISTS clean_db.idx_microchip_id_code;
CREATE INDEX IF NOT EXISTS idx_microchip_id_code
    ON clean_db.microchip USING btree
    (id_code ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: clean_db.owner

-- DROP TABLE IF EXISTS clean_db.owner;
CREATE TABLE IF NOT EXISTS clean_db.owner
(
    id_owner integer NOT NULL,
    first_name character varying(150) COLLATE pg_catalog."default" NOT NULL,
    last_name character varying(150) COLLATE pg_catalog."default" NOT NULL,
    address character varying(150) COLLATE pg_catalog."default",
    city character varying(50) COLLATE pg_catalog."default" NOT NULL,
    postal_code character varying(20) COLLATE pg_catalog."default",
    phone_number character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT owner_pkey PRIMARY KEY (id_owner)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS clean_db.owner
    OWNER to postgres;

-- Index: idx_owner_first_name
-- DROP INDEX IF EXISTS clean_db.idx_owner_first_name;
CREATE INDEX IF NOT EXISTS idx_owner_first_name
    ON clean_db.owner USING btree
    (first_name ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: clean_db.animal_owner

-- DROP TABLE IF EXISTS clean_db.animal_owner;
CREATE TABLE IF NOT EXISTS clean_db.animal_owner
(
    id_animal_owner integer NOT NULL,
    id_microchip integer NOT NULL,
    id_owner integer NOT NULL,
    CONSTRAINT animal_owner_pkey PRIMARY KEY (id_animal_owner)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS clean_db.animal_owner
    OWNER to postgres;

--###############################################################################################
-- Table: clean_db.doctor

CREATE TYPE clean_db.medical_specialty AS ENUM
    ('surgeon', 'generalist');

-- DROP TABLE IF EXISTS clean_db.doctor;
CREATE TABLE IF NOT EXISTS clean_db.doctor
(
    id_doctor integer NOT NULL,
    first_name character varying(150) COLLATE pg_catalog."default" NOT NULL,
    last_name character varying(150) COLLATE pg_catalog."default" NOT NULL,
    specialty clean_db.medical_specialty NOT NULL,
    license_number character varying(25) COLLATE pg_catalog."default" NOT NULL,
    start_date date NOT NULL,
    end_date date,
    period_start_date date NOT NULL,
    period_end_date date,
    max_monthly_hours double precision DEFAULT 0,
    CONSTRAINT doctor_pkey PRIMARY KEY (id_doctor)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS clean_db.doctor
    OWNER to postgres;

-- Index: idx_doctor_specialy
-- DROP INDEX IF EXISTS clean_db.idx_doctor_specialy;
CREATE INDEX IF NOT EXISTS idx_doctor_specialy
    ON clean_db.doctor USING btree
    (specialty ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: clean_db.doctor_historization

-- DROP TABLE IF EXISTS clean_db.doctor_historization;
CREATE TABLE IF NOT EXISTS clean_db.doctor_historization
(
    id_doctor_histo integer NOT NULL,
    id_doctor integer NOT NULL,
    first_name character varying(150) COLLATE pg_catalog."default" NOT NULL,
    last_name character varying(150) COLLATE pg_catalog."default" NOT NULL,
    specialty clean_db.medical_specialty NOT NULL,
    license_number character varying(25) COLLATE pg_catalog."default" NOT NULL,
    period_start_date date NOT NULL,
    period_end_date date,
    max_monthly_hours double precision DEFAULT 0 NOT NULL,
    CONSTRAINT doctor_histo_pkey PRIMARY KEY (id_doctor_histo)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS clean_db.doctor_historization
    OWNER to postgres;

-- Index: idx_doctor_historization_specialy
-- DROP INDEX IF EXISTS clean_db.idx_doctor_historization_specialy;
CREATE INDEX IF NOT EXISTS idx_doctor_historization_specialy
    ON clean_db.doctor_historization USING btree
    (specialty ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: clean_db.service

-- DROP TABLE IF EXISTS clean_db.service;
CREATE TABLE IF NOT EXISTS clean_db.service
(
    id_service integer NOT NULL,
    service_name character varying(50) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT service_pkey PRIMARY KEY (id_service)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS clean_db.service
    OWNER to postgres;

-- Index: idx_service_name
-- DROP INDEX IF EXISTS clean_db.idx_service_name;
CREATE INDEX IF NOT EXISTS idx_service_name
    ON clean_db.service USING btree
    (service_name ASC NULLS LAST)
    TABLESPACE pg_default;


--###############################################################################################
-- Table: clean_db.appointment

CREATE TYPE clean_db.appointment_reason AS ENUM
    ('annual_visit', 'sick_pet', 'injured_pet', 'initial_visit', 'follow_up', 'surgery', 'follow_up_surgery');

-- DROP TABLE IF EXISTS clean_db.appointment;
CREATE TABLE IF NOT EXISTS clean_db.appointment
(
    id_appointment integer NOT NULL,
    id_animal integer NOT NULL,
    appt_reason clean_db.appointment_reason NOT NULL,
    id_owner integer NOT NULL,
    CONSTRAINT appointment_pkey PRIMARY KEY (id_appointment)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS clean_db.appointment
    OWNER to postgres;

-- Index: idx_appointment_id_animal
-- DROP INDEX IF EXISTS clean_db.idx_appointment_id_animal;
CREATE INDEX IF NOT EXISTS idx_appointment_id_animal
    ON clean_db.appointment USING btree
    (id_animal ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: clean_db.appointment_service

-- DROP TABLE IF EXISTS clean_db.appointment_service;
CREATE TABLE IF NOT EXISTS clean_db.appointment_service
(
    id_appointment_service integer NOT NULL,
    id_appointment integer NOT NULL,
    id_service integer NOT NULL,
    CONSTRAINT appointment_service_pkey PRIMARY KEY (id_appointment_service)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS clean_db.appointment_service
    OWNER to postgres;

-- Index: idx_appointment_service_id_appointment
-- DROP INDEX IF EXISTS clean_db.idx_appointment_service_id_appointment;
CREATE INDEX IF NOT EXISTS idx_appointment_service_id_appointment
    ON clean_db.appointment_service USING btree
    (id_appointment ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_appointment_service_id_service
-- DROP INDEX IF EXISTS clean_db.idx_appointment_service_id_service;
CREATE INDEX IF NOT EXISTS idx_appointment_service_id_service
    ON clean_db.appointment_service USING btree
    (id_service ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: clean_db.slot

CREATE TYPE clean_db.slot_type AS ENUM
    ('regular', 'overtime');

-- DROP TABLE IF EXISTS clean_db.slot;
CREATE TABLE IF NOT EXISTS clean_db.slot
(
    id_slot integer NOT NULL,
    id_doctor integer NOT NULL,
    date date NOT NULL,
    "time" time NOT NULL,
    type clean_db.slot_type NOT NULL,
    CONSTRAINT slot_pkey PRIMARY KEY (id_slot)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS clean_db.slot
    OWNER to postgres;

-- Index: idx_slot_doctor
-- DROP INDEX IF EXISTS clean_db.idx_slot_doctor;
CREATE INDEX IF NOT EXISTS idx_slot_doctor
    ON clean_db.slot USING btree
    (id_doctor ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_slot_date
-- DROP INDEX IF EXISTS clean_db.idx_slot_date;
CREATE INDEX IF NOT EXISTS idx_slot_date
    ON clean_db.slot USING btree
    (date ASC NULLS LAST)
    TABLESPACE pg_default;

--###############################################################################################
-- Table: clean_db.appointment_slot

-- DROP TABLE IF EXISTS clean_db.appointment_slot;
CREATE TABLE IF NOT EXISTS clean_db.appointment_slot
(
    id_appointment_slot integer NOT NULL,
    id_appointment integer NOT NULL,
    id_slot integer NOT NULL,
    CONSTRAINT appointment_slot_pkey PRIMARY KEY (id_appointment_slot)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS clean_db.appointment_slot
    OWNER to postgres;

-- Index: idx_appointment_slot_appointment
-- DROP INDEX IF EXISTS clean_db.idx_appointment_slot_appointment;
CREATE INDEX IF NOT EXISTS idx_appointment_slot_appointment
    ON clean_db.appointment_slot USING btree
    (id_appointment ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: idx_appointment_slot_slot
-- DROP INDEX IF EXISTS clean_db.idx_appointment_slot_slot;
CREATE INDEX IF NOT EXISTS idx_appointment_slot_slot
    ON clean_db.appointment_slot USING btree
    (id_slot ASC NULLS LAST)
    TABLESPACE pg_default;