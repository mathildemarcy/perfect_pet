
-- ADD FOREIGN KEY CONSTRAINTS ON CLEAN DATABASE
-- TO EXECUTE AFTER LOADING THE DATA

ALTER TABLE clean_db.animal
ADD CONSTRAINT fk_animal_microchip FOREIGN KEY (id_microchip)
    REFERENCES clean_db.microchip (id_microchip) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE clean_db.animal_weight
ADD CONSTRAINT fk_animal_weight_animal FOREIGN KEY (id_animal)
    REFERENCES clean_db.animal (id_animal) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
ALTER TABLE clean_db.animal_weight
ADD CONSTRAINT fk_animal_weight_appointment FOREIGN KEY (id_appointment)
    REFERENCES clean_db.appointment (id_appointment) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE clean_db.microchip
ADD CONSTRAINT fk_microchip_code FOREIGN KEY (id_code)
    REFERENCES clean_db.microchip_code (id_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE clean_db.animal_owner
ADD CONSTRAINT fk_animal_owner_microchip FOREIGN KEY (id_microchip)
    REFERENCES clean_db.microchip (id_microchip) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
ALTER TABLE clean_db.animal_owner
ADD CONSTRAINT fk_animal_owner_owner FOREIGN KEY (id_owner)
    REFERENCES clean_db.owner (id_owner) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE clean_db.appointment
ADD CONSTRAINT fk_appointment_animal FOREIGN KEY (id_animal)
    REFERENCES clean_db.animal (id_animal) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
ALTER TABLE clean_db.appointment
ADD CONSTRAINT fk_appointment_owner FOREIGN KEY (id_owner)
    REFERENCES clean_db.owner (id_owner) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE clean_db.appointment_service
ADD CONSTRAINT fk_appointment_service_service FOREIGN KEY (id_service)
    REFERENCES clean_db.service (id_service) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
ALTER TABLE clean_db.appointment_service
ADD CONSTRAINT fk_appointment_service_appointment FOREIGN KEY (id_appointment)
    REFERENCES clean_db.appointment (id_appointment) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE clean_db.slot
ADD CONSTRAINT fk_slot_doctor FOREIGN KEY (id_doctor)
    REFERENCES clean_db.doctor (id_doctor) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE clean_db.appointment_slot
ADD CONSTRAINT fk_appointment_slot_appointment FOREIGN KEY (id_appointment)
    REFERENCES clean_db.appointment (id_appointment) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
ALTER TABLE clean_db.appointment_slot
ADD CONSTRAINT fk_appointment_slot_slot FOREIGN KEY (id_slot)
    REFERENCES clean_db.slot (id_slot) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE clean_db.doctor_historization
ADD CONSTRAINT fk_doctor_historization_doctor FOREIGN KEY (id_doctor)
    REFERENCES clean_db.doctor (id_doctor) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;