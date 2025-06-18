
-- ADD FOREIGN KEY CONSTRAINTS ON POLLUTED DATABASE
-- TO EXECUTE AFTER LOADING THE DATA

ALTER TABLE polluted_db.animal
ADD CONSTRAINT fk_animal_owner FOREIGN KEY (id_owner)
    REFERENCES polluted_db.owner (id_owner) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE polluted_db.animal
ADD CONSTRAINT fk_animal_microchip FOREIGN KEY (id_microchip)
    REFERENCES polluted_db.microchip (id_microchip) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE polluted_db.microchip
ADD CONSTRAINT fk_microchip_code FOREIGN KEY (id_code)
    REFERENCES polluted_db.microchip_code (id_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
ALTER TABLE polluted_db.microchip
ADD CONSTRAINT fk_microchip_owner FOREIGN KEY (id_owner)
    REFERENCES polluted_db.owner (id_owner) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE polluted_db.owner
ADD CONSTRAINT fk_owner_animal FOREIGN KEY (id_animal)
    REFERENCES polluted_db.animal (id_animal) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE polluted_db.appointment
ADD CONSTRAINT fk_appointment_animal FOREIGN KEY (id_animal)
    REFERENCES polluted_db.animal (id_animal) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
ALTER TABLE polluted_db.appointment
ADD CONSTRAINT fk_appointment_service FOREIGN KEY (id_service)
    REFERENCES polluted_db.service (id_service) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE polluted_db.slot
ADD CONSTRAINT fk_slot_doctor FOREIGN KEY (id_doctor)
    REFERENCES polluted_db.doctor (id_doctor) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE polluted_db.appointment_slot
ADD CONSTRAINT fk_appointment_slot_appointment FOREIGN KEY (id_appointment)
    REFERENCES polluted_db.appointment (id_appointment) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
ALTER TABLE polluted_db.appointment_slot
ADD CONSTRAINT fk_appointment_slot_slot FOREIGN KEY (id_slot)
    REFERENCES polluted_db.slot (id_slot) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;