
-- ADD FOREIGN KEY CONSTRAINTS ON POLLUTED DATABASE
-- TO EXECUTE AFTER LOADING THE DATA

ALTER TABLE polluted_db_au.animal
ADD CONSTRAINT fk_animal_owner FOREIGN KEY (id_owner)
    REFERENCES polluted_db_au.owner (id_owner) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE polluted_db_au.animal
ADD CONSTRAINT fk_animal_microchip FOREIGN KEY (id_microchip)
    REFERENCES polluted_db_au.microchip (id_microchip) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE polluted_db_au.microchip
ADD CONSTRAINT fk_microchip_code FOREIGN KEY (id_code)
    REFERENCES polluted_db_au.microchip_code (id_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
ALTER TABLE polluted_db_au.microchip
ADD CONSTRAINT fk_microchip_owner FOREIGN KEY (id_owner)
    REFERENCES polluted_db_au.owner (id_owner) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE polluted_db_au.owner
ADD CONSTRAINT fk_owner_animal FOREIGN KEY (id_animal)
    REFERENCES polluted_db_au.animal (id_animal) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE polluted_db_au.appointment
ADD CONSTRAINT fk_appointment_animal FOREIGN KEY (id_animal)
    REFERENCES polluted_db_au.animal (id_animal) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
ALTER TABLE polluted_db_au.appointment
ADD CONSTRAINT fk_appointment_service FOREIGN KEY (id_service)
    REFERENCES polluted_db_au.service (id_service) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE polluted_db_au.slot
ADD CONSTRAINT fk_slot_doctor FOREIGN KEY (id_doctor)
    REFERENCES polluted_db_au.doctor (id_doctor) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE polluted_db_au.appointment_slot
ADD CONSTRAINT fk_appointment_slot_appointment FOREIGN KEY (id_appointment)
    REFERENCES polluted_db_au.appointment (id_appointment) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
ALTER TABLE polluted_db_au.appointment_slot
ADD CONSTRAINT fk_appointment_slot_slot FOREIGN KEY (id_slot)
    REFERENCES polluted_db_au.slot (id_slot) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;