import pandas as pd
import numpy as np
import random as rd
from faker import Faker
fake = Faker()
from shared_functions import add_primary_key_values
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="- %(levelname)s - %(asctime)s - %(message)s"
)

def randomly_select_rows(
    dataframe: pd.DataFrame,
    augmentation_rate: float,
    replace: bool = False
    ):
    """
    """
    n_rows = len(dataframe) 
    n_to_select = int(n_rows * augmentation_rate)
    indices_to_select = np.random.choice(
        dataframe.index,
        n_to_select, replace=replace
    )
    selected_rows = dataframe.loc[indices_to_select].copy()
    selected_rows.reset_index(drop=True, inplace=True)

    return selected_rows


def randomly_duplicate_rows(
    dataframe: pd.DataFrame,
    augmentation_rate: float
    ):
    """
        Returns an augmented version of the input DataFrame after
        duplicating a randomly selected portion of its rows
    """

    duplicated_rows = randomly_select_rows(
        dataframe = dataframe,
        augmentation_rate = augmentation_rate,
        replace = True
    )
    dataframe_duplicated = pd.concat([dataframe, duplicated_rows])
    dataframe_duplicated.reset_index(drop=True, inplace=True)

    return dataframe_duplicated

class au_transformator():
    """
        Transform the "clean" relations of Perfect Pet into
        relations suffering from artificial unicity and/or
        denormalization.
    """
    def __init__(self,
        microchip_code_rel: pd.DataFrame,
        microchip_rel: pd.DataFrame,
        animal_rel: pd.DataFrame,
        animal_weight_rel: pd.DataFrame,
        owner_rel: pd.DataFrame,
        animal_owner_rel: pd.DataFrame,
        service_rel: pd.DataFrame,
        appointment_rel: pd.DataFrame,
        appointment_service_rel: pd.DataFrame,
        slot_rel: pd.DataFrame,
        appointment_slot_rel: pd.DataFrame,
        doctor_rel: pd.DataFrame,
        doctor_historization_rel: pd.DataFrame
        ):
        """
            Initialize the class with the DataFrames corresponding to the 
            relations of the "clean" version of Perfect Pet database
        """
        logging.info("Instantiating object from class au_transformator")

        self.microchip_code_rel = microchip_code_rel
        self.microchip_rel = microchip_rel
        self.animal_rel = animal_rel
        self.animal_weight_rel = animal_weight_rel
        self.owner_rel = owner_rel
        self.animal_owner_rel = animal_owner_rel
        self.service_rel = service_rel
        self.appointment_rel = appointment_rel
        self.appointment_service_rel = appointment_service_rel
        self.slot_rel = slot_rel
        self.appointment_slot_rel = appointment_slot_rel
        self.doctor_rel = doctor_rel
        self.doctor_historization_rel = doctor_historization_rel

        self.microchip_code_au = []
        self.service_au = []
        self.microchip_au = []
        self.animal_au = []
        self.appointment_au = []
        self.owner_au = []
        self.slot_au = []
        self.appointment_slot_au = []
        self.doctor_au = []

    #---------------------------------------------------------------- 
    def transform_microchip_code(self,
        augmentation_rate: float = 0.45,
        starting_id_value: int = 1
        ) -> pd.DataFrame:
        """
            Redundancy will be introduced in relation microchip_code
        """
        logging.info("Start the transformation of relation Microchip_Code")

        microchip_code_au = randomly_duplicate_rows(
            dataframe = self.microchip_code_rel,
            augmentation_rate = augmentation_rate
        )

        microchip_code_au = add_primary_key_values(
            relation = microchip_code_au,
            pk_column_name = 'id_code_au',
            starting_id_value = starting_id_value
        )

        logging.info(f"Transformed relation Microchip_Code is of size {len(microchip_code_au)}")
        self.microchip_code_au = microchip_code_au
        return microchip_code_au

    #---------------------------------------------------------------- 
    def transform_service(self,
        augmentation_rate: float = 0,
        starting_id_value: int = 1
        ) -> pd.DataFrame:
        """
            Redundancy will be introduced in relation microchip_code
        """
        logging.info("Start the transformation of relation Service")

        service_au = randomly_duplicate_rows(
            dataframe = self.service_rel,
            augmentation_rate = augmentation_rate
        )

        service_au = add_primary_key_values(
            relation = service_au,
            pk_column_name = 'id_service_au',
            starting_id_value = starting_id_value
        )

        logging.info(f"Transformed relation Service is of size {len(service_au)}")
        self.service_au = service_au
        return service_au

    #---------------------------------------------------------------- 
    def transform_animal(self,
        augmentation_rate: float = 1,
        starting_id_value: int = 1
        ) -> pd.DataFrame:
        """
            Relation animal will be denormalized: 
            it will be merged with relation animal_weight to include
            one tuple/row for each appointment and add the unstable
            attribute weight
        """
        logging.info("Start the transformation of relation Animal")

        animal_appt = self.animal_rel.merge(
            self.animal_weight_rel,
            on='id_animal',
            how='left'
        )

        initial_appt = animal_appt.groupby(
            'id_animal'
        )['id_appointment'].transform('min')

        initial_appt_list = initial_appt.unique()

        animal_additional = animal_appt[
            ~animal_appt['id_appointment'].isin(initial_appt_list)
        ]

        initial_animals = animal_appt[
            animal_appt['id_appointment'].isin(initial_appt_list)
        ]

        duplicated_rows = randomly_select_rows(
            dataframe = animal_additional,
            augmentation_rate = augmentation_rate
        )

        animal_au = pd.concat([initial_animals, duplicated_rows])

        animal_au = animal_au.sort_values(
            ['id_appointment']
        ).reset_index(drop=True)

        animal_au = animal_au.drop_duplicates()

        animal_au = add_primary_key_values(
            relation = animal_au,
            pk_column_name = 'id_animal_au',
            starting_id_value = starting_id_value
        )

        logging.info(f"Transformed relation Animal is of size {len(animal_au)}")
        self.animal_au = animal_au
        return animal_au

    #---------------------------------------------------------------- 
    def transform_microchip(self,
        microchip_code_au: pd.DataFrame = None,
        starting_id_value: int = 1
        ) -> pd.DataFrame:
        """
            Relation microchip will be denormalized: the new one will
            contain one tuple/row for each animal, i.e. each appointment.
            Has to be executed after functions transform_animal and
            augment_microchip_code.
        """
        logging.info("Start the transformation of relation Microchip")

        if microchip_code_au is None:
            microchip_code_au = self.microchip_code_au

        microchip_au = self.microchip_rel.merge(
            self.animal_au[['id_microchip', 'id_animal_au']],
            on='id_microchip',
            how='left'
        )
        microchip_au = microchip_au.sort_values(
            ['id_animal_au']
        ).reset_index(drop=True)

        microchip_au = add_primary_key_values(
            relation = microchip_au,
            pk_column_name = 'id_microchip_au',
            starting_id_value = starting_id_value
        )
        id_code_groups = microchip_code_au.groupby('id_code')['id_code_au'].apply(list)
        def get_random_id_mod(
            id_code: int
            ):
            possible_id_codes = id_code_groups.get(id_code, [])
            return np.random.choice(possible_id_codes) if possible_id_codes else None
        
        microchip_au['id_code_au'] = microchip_au['id_code']
        microchip_au['id_code_au'] = microchip_au['id_code_au'].apply(get_random_id_mod)

        logging.info(f"Transformed relation Microchip is of size {len(microchip_au)}")
        self.microchip_au = microchip_au
        return microchip_au

    #---------------------------------------------------------------- 
    def transform_appointment(self,
        animal_au: pd.DataFrame = None,
        service_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Relation appointment will be denormalized: 
            it will be merged with relation appointment_service to
            include one tuple/row for each service performed
            during an appointment. Has to be performed after function
            trasnform_animal
        """
        logging.info("Start the transformation of relation Appointment")

        if animal_au is None:
            animal_au = self.animal_au
        if service_au is None:
            service_au = self.service_au

        appointment_data = self.appointment_service_rel.merge(
            self.appointment_rel,
            on='id_appointment',
            how='left'
        )

        appointment_data = appointment_data.merge(
            animal_au[['id_appointment', 'id_animal_au']],
            on='id_appointment',
            how='left'
        )

        # if id_animal_au is null because the appointment was not kept in animal_au
        def find_latest_animal_tuple(
            id_animal: str,
            id_appointment: str
            ):
            """
                Function to find the latest assigned id_animal for a specific pet
                to be used for appointments for which no new animal tuple
                was added to animal_au
            """
            if pd.isna(id_animal) or pd.isna(id_appointment):
                return None
            return animal_au[
                (animal_au['id_animal'] == id_animal) &
                (animal_au['id_appointment'] < id_appointment)
            ]['id_animal_au'].max()

        assigned_appointments = appointment_data[~appointment_data['id_animal_au'].isna()]
        unassigned_appointments = appointment_data[appointment_data['id_animal_au'].isna()]
        unassigned_appointments['id_animal_au'] = unassigned_appointments.apply(
            lambda row: find_latest_animal_tuple(row['id_animal'], row['id_appointment']),
            axis=1
        )
        
        appointment_au = pd.concat([assigned_appointments, unassigned_appointments])
        appointment_au.reset_index(drop=True, inplace=True)

        id_service_groups = service_au.groupby('id_service')['id_service_au'].apply(list)
        def get_random_id_mod(
            id_service: int
            ):
            possible_id_services = id_service_groups.get(id_service, [])
            return np.random.choice(possible_id_services) if possible_id_services else None
        
        appointment_au['id_service_au'] = appointment_au['id_service']
        appointment_au['id_service_au'] = appointment_au['id_service_au'].apply(get_random_id_mod)

        logging.info(f"Transformed relation Appointment is of size {len(appointment_au)}")
        self.appointment_au = appointment_au
        return appointment_au

    #---------------------------------------------------------------- 
    def transform_appointment_slot(self,
        appointment_au: pd.DataFrame = None,
        starting_id_value: int = 1
        ) -> pd.DataFrame:
        """
            Association appointment_slot will be denormalized to
            assign a slot to every denormalized appointment tuple/row
            (i.e. one slot is assigned to every service performed)
            Has to be executed after function transform_appointment.
        """
        logging.info("Start the transformation of relation Appointment_Slot")

        if appointment_au is None:
            appointment_au = self.appointment_au

        appointment_slot_au = self.appointment_slot_rel.merge(
            appointment_au[['id_appointment', 'id_appointment_service']],
            on='id_appointment',
            how='left'
        )
        appointment_slot_au = appointment_slot_au[
            ~appointment_slot_au['id_appointment_service'].isna()
        ]
        appointment_slot_au = appointment_slot_au[['id_appointment_service', 'id_slot']]
        appointment_slot_au = appointment_slot_au.sort_values(
            ['id_slot', 'id_appointment_service']
        ).reset_index(drop=[True, True])

        appointment_slot_au = add_primary_key_values(
            relation = appointment_slot_au,
            pk_column_name = 'id_appointment_slot_au',
            starting_id_value = starting_id_value
        )

        logging.info(f"Transformed relation Appointment_Slot is of size {len(appointment_slot_au)}")
        self.appointment_slot_au = appointment_slot_au
        return appointment_slot_au

    #---------------------------------------------------------------- 
    def transform_doctor(self) -> pd.DataFrame:
        """
            Relation doctor will be denormalized: 
            it will be replaced by relation doctor_historization to
            which we add the attributes start_date and end_date from
            original relation doctor
        """
        logging.info("Start the transformation of relation Doctor")

        doctor_au = self.doctor_historization_rel.merge(
            self.doctor_rel[['id_doctor', 'start_date', 'end_date']],
            on = 'id_doctor',
            how = 'left'
        )

        logging.info(f"Transformed relation Doctor is of size {len(doctor_au)}")
        self.doctor_au = doctor_au
        return doctor_au

    #---------------------------------------------------------------- 
    def transform_slot(self,
        doctor_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Add attributes 'id_doctor_histo' to DataFrame slot
            to associate it to the denormalized version of relation
            doctor. Has to be executed after function transform_doctor
        """
        logging.info("Start the transformation of relation Slot")

        if doctor_au is None:
            doctor_au = self.doctor_au

        slot_au = self.slot_rel
        slot_au['id_doctor_histo'] = None
        for _, row in doctor_au.iterrows():
            start_date = row['period_start_date']
            end_date = row['period_end_date']
            id_doctor = row['id_doctor']
            id_doctor_histo = row['id_doctor_histo']
            index_slot_replace = slot_au[
                (slot_au['id_doctor']==id_doctor)
                & (slot_au['date'] >= start_date)
                & (slot_au['date'] <= end_date)
            ].index.values.tolist()
            slot_au.loc[index_slot_replace, 'id_doctor_histo'] = id_doctor_histo

        logging.info(f"Transformed relation Slot is of size {len(slot_au)}")
        self.slot_au = slot_au
        return slot_au

    #---------------------------------------------------------------- 
    def transform_owner(self,
        appointment_au: pd.DataFrame = None,
        animal_au: pd.DataFrame = None,
        starting_id_value: int = 1
        ) -> pd.DataFrame:
        """
            Relation owner will be denormalized to include one tuple/row
            per animal and appointment, and will be associated to both.
            Has to be executed after functions transform_appointment
            and transform_animal
        """
        logging.info("Start the transformation of relation Owner")

        if appointment_au is None:
            appointment_au = self.appointment_au
        if animal_au is None:
            animal_au = self.animal_au

        owner_appt = self.owner_rel.merge(
            appointment_au[['id_owner', 'id_appointment']],
            on='id_owner',
            how='left'
        )
        owner_appt = owner_appt.drop_duplicates()
        owner_au = owner_appt.merge(
            animal_au[['id_appointment', 'id_animal_au']],
            on='id_appointment',
            how='left'
        )
        owner_au = owner_au[
            ~owner_au['id_animal_au'].isna()
        ]
        owner_au = owner_au.sort_values(
            ['id_animal_au']
        ).reset_index(drop=True)

        owner_au = add_primary_key_values(
            relation = owner_au,
            pk_column_name = 'id_owner_au',
            starting_id_value = starting_id_value
        )

        logging.info(f"Transformed relation Owner is of size {len(owner_au)}")
        self.owner_au = owner_au
        return owner_au

    #---------------------------------------------------------------- 
    def update_appointment_id_owner(self,
        appointment_au: pd.DataFrame = None,
        owner_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Add attribute 'id_owner_au' to DataFrame appointment_au
            to associate it to the polluted version of relation
            owner. Has to be executed after functions
            transform_appointment and transform_owner
        """
        logging.info("Adding FK id_owner to relation Appointment")

        if appointment_au is None:
            appointment_au = self.appointment_au
        if owner_au is None:
            owner_au = self.owner_au

        appointment_au = appointment_au.merge(
            owner_au[['id_animal_au', 'id_owner_au']],
            on='id_animal_au',
            how='left'
        )

        self.appointment_au = appointment_au
        return appointment_au
    
    #---------------------------------------------------------------- 
    def update_animal_id_microchip(self,
        microchip_au: pd.DataFrame = None,
        animal_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Add attribute 'id_microchip_au' to DataFrame animal_au
            to associate it to the polluted version of relation
            microchip. Has to be executed after functions
            transform_animal and transform_microchip
        """
        logging.info("Adding FK id_microchip to relation Animal")

        if microchip_au is None:
            microchip_au = self.microchip_au
        if animal_au is None:
            animal_au = self.animal_au

        animal_au = animal_au.merge(
            microchip_au[['id_animal_au', 'id_microchip_au']],
            on='id_animal_au',
            how='left'
        )

        self.animal_au = animal_au
        return animal_au

    #---------------------------------------------------------------- 
    def update_animal_id_owner(self,
        animal_au: pd.DataFrame = None,
        owner_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Add attribute 'id_owner_au' to DataFrame animal_au
            to associate it to the polluted version of relation
            owner. Has to be executed after functions
            transform_animal and transform_owner
        """
        logging.info("Adding FK id_owner to relation Animal")

        if animal_au is None:
            animal_au = self.animal_au
        if owner_au is None:
            owner_au = self.owner_au

        animal_au = animal_au.merge(
            owner_au[['id_animal_au', 'id_owner_au']],
            on='id_animal_au',
            how='left'
        )
        animal_au = animal_au.drop_duplicates()

        self.animal_au = animal_au
        return animal_au

    #---------------------------------------------------------------- 
    def assign_missing_owner_to_animal(self,
        animal_au: pd.DataFrame = None,
        animal_owner_rel: pd.DataFrame = None,
        owner_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            XXX
        """
        logging.info("XXX")

        if animal_au is None:
            animal_au = self.animal_au
        if animal_owner_rel is None:
            animal_owner_rel = self.animal_owner_rel
        if owner_au is None:
            owner_au = self.owner_au

        animal_au_copy = animal_au.copy()

        missing_owner = animal_au_copy[animal_au_copy['id_owner_au'].isna()]
        if len(missing_owner) > 0:
            animal_list = missing_owner['id_animal_au'].unique()
            microchip_list = missing_owner['id_microchip'].unique()

            match = animal_owner_rel[
                animal_owner_rel['id_microchip'].isin(microchip_list)
            ]

            if len(match) > 0:
                for id in animal_list:
                    id_microchip = rd.choice(missing_owner[
                        missing_owner['id_animal_au'] == id
                    ]['id_microchip'].unique())

                    id_owner = rd.choice(match[
                        match['id_microchip'] == id_microchip
                    ]['id_owner'].unique())

                    id_owner_au = rd.choice(
                        owner_au[owner_au['id_owner'] == id_owner
                    ]['id_owner_au'].unique())

                    animal_au_copy.loc[animal_au_copy['id_animal_au'] == id, 'id_owner_au'] = id_owner_au

        self.animal_au = animal_au_copy

        return self.animal_au

    #---------------------------------------------------------------- 
    def update_microchip_id_owner(self,
        microchip_au: pd.DataFrame = None,
        animal_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Add attribute 'id_owner_au' to DataFrame microchip_au
            to associate it to the polluted version of relation
            owner. Has to be executed after functions
            transform_microchip and transform_owner
        """
        logging.info("Adding FK id_owner to relation Microchip")

        if microchip_au is None:
            microchip_au = self.microchip_au
        if animal_au is None:
            animal_au = self.animal_au

        microchip_au = microchip_au.merge(
            animal_au[['id_animal_au', 'id_owner_au']],
            on='id_animal_au',
            how='left'
        )

        self.microchip_au = microchip_au
        return microchip_au

    #---------------------------------------------------------------- 
    def update_animal_hash_id(self,
        animal_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """ 
            Update the values of hash_id in relation animal to have
            them all distinct, only the one from the tuple associated
            to the first appointment for each animal is kept
        """
        logging.info("Modify values of SK hash_id in relation Animal to make them unique")

        if animal_au is None:
            animal_au = self.animal_au

        # find the minimal appointment id for each animal
        animal_au['min_appt'] = animal_au.groupby(
            'id_animal'
        )['id_appointment'].transform('min')
        
        mask = animal_au['id_appointment'] != animal_au['min_appt']
        new_hashes = [fake.uuid4() for _ in range(mask.sum())]
        animal_au.loc[mask, 'hash_id'] = new_hashes
        animal_au = animal_au.drop(columns='min_appt')
        
        self.animal_au = animal_au
        return animal_au

    #----------------------------------------------------------------
    def finalize_microchip_code(self,
        microchip_code_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Transform old id_code into id_code_v1 for reference and
            to get the accurate reference classes, and new id_code_au
            into the main identifier id_code
        """
        logging.info("Finalizing relation Microchip_Code (suggering from AU)")

        if microchip_code_au is None:
            microchip_code_au = self.microchip_code_au
        
        microchip_code_au.rename(
            columns={
                'id_code': 'id_code_v1',
                'id_code_au': 'id_code'
            },
            inplace=True)

        self.microchip_code_au = microchip_code_au
        return microchip_code_au

    #----------------------------------------------------------------
    def finalize_microchip(self,
        microchip_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Transform old id_microchip into id_microchip_v1 for reference and
            to get the accurate reference classes, and new id_microchip_au
            into the main identifier id_microchip, and keep only required
            attributes
        """
        logging.info("Finalizing relation Microchip (suggering from AU)")

        if microchip_au is None:
            microchip_au = self.microchip_au

        microchip_au = self.microchip_au[
            ['id_microchip_au', 'id_code_au', 'number', 'implant_date',
            'location', 'id_owner_au', 'id_microchip', 'id_code']
        ].copy()
        microchip_au.rename(
            columns={
                'id_microchip': 'id_microchip_v1',
                'id_microchip_au': 'id_microchip',
                'id_owner_au': 'id_owner',
                'id_code': 'id_code_v1',
                'id_code_au': 'id_code'
            },
            inplace=True)

        self.microchip_au = microchip_au
        return microchip_au

    #----------------------------------------------------------------
    def finalize_owner(self,
        owner_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Transform old id_owner into id_owner_v1 for reference and
            to get the accurate reference classes, and new id_owner_au
            into the main identifier id_owner, and keep only required
            attributes
        """
        logging.info("Finalizing relation Owner (suggering from AU)")

        if owner_au is None:
            owner_au = self.owner_au

        owner_au = owner_au[
            ['id_owner_au', 'first_name', 'last_name', 'address', 'city',
            'postal_code', 'phone_number', 'id_animal_au', 'id_owner']
        ].copy()
        owner_au.rename(
            columns={
                'id_owner': 'id_owner_v1',
                'id_owner_au': 'id_owner',
                'id_animal_au': 'id_animal'
            },
            inplace=True)

        self.owner_au = owner_au
        return owner_au

    #----------------------------------------------------------------
    def finalize_animal(self,
        animal_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Transform old id_animal into id_animal_v1 for reference and
            to get the accurate reference classes, and new id_animal_au
            into the main identifier id_animal, and keep only required
            attributes
        """
        logging.info("Finalizing relation animal (suggering from AU)")

        if animal_au is None:
            animal_au = self.animal_au

        animal_au = animal_au[
            ['id_animal_au', 'species', 'breed', 'name', 'id_microchip_au',
            'gender', 'dob', 'weight', 'hash_id', 'id_owner_au', 'id_animal']
        ].copy()
        animal_au.rename(
            columns={
                'id_animal': 'id_animal_v1',
                'id_animal_au': 'id_animal', 
                'id_owner_au': 'id_owner',
                'id_microchip_au': 'id_microchip'
            },
            inplace=True)

        self.animal_au = animal_au
        return animal_au

    #----------------------------------------------------------------
    def finalize_service(self,
        service_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Transform old id_service into id_service_v1 for reference and
            to get the accurate reference classes, and new id_service_au
            into the main identifier id_service
        """
        logging.info("Finalizing relation Service (suggering from AU)")

        if service_au is None:
            service_au = self.service_au
        
        service_au.rename(
            columns={
                'id_service': 'id_service_v1',
                'id_service_au': 'id_service'
            },
            inplace=True)

        self.service_au = service_au
        return service_au
        
    #----------------------------------------------------------------
    def finalize_appointment(self,
        appointment_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Transform old id_appointment into id_appointment_v1 for reference and
            to get the accurate reference classes, and new id_appointment_au
            into the main identifier id_appointment, and keep only required
            attributes
        """
        logging.info("Finalizing relation Appointment (suggering from AU)")

        if appointment_au is None:
            appointment_au = self.appointment_au

        appointment_au = appointment_au[
            ['id_appointment_service', 'id_animal_au', 'appt_reason',
            'id_service', 'id_owner_au', 'id_appointment', 'id_animal',
            'id_owner']
        ].copy()
        appointment_au.rename(
            columns={
                'id_appointment': 'id_appointment_v1',
                'id_appointment_service': 'id_appointment',
                'id_animal': 'id_animal_v1',
                'id_animal_au': 'id_animal',
                'id_owner': 'id_owner_v1',
                'id_owner_au': 'id_owner'
            },
            inplace=True)

        self.appointment_au = appointment_au
        return appointment_au

    #----------------------------------------------------------------
    def finalize_appointment_slot(self,
        appointment_slot_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Transform old id_appointment into id_appointment_v1 for reference and
            to get the accurate reference classes, and new id_appointment_au
            into the main identifier id_appointment, and keep only required
            attributes
        """
        logging.info("Finalizing relation Appointment_Slot (suggering from AU)")

        if appointment_slot_au is None:
            appointment_slot_au = self.appointment_slot_au

        appointment_slot_au.rename(
            columns={
                'id_appointment_slot_au': 'id_appointment_slot',
                'id_appointment_service': 'id_appointment'
            },
            inplace=True)

        self.appointment_slot_au = appointment_slot_au
        return appointment_slot_au

    #----------------------------------------------------------------
    def finalize_slot(self,
        slot_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Transform old id_slot into id_slot_v1 for reference and
            to get the accurate reference classes, and new id_slot_au
            into the main identifier id_slot, and keep only required
            attributes
        """
        logging.info("Finalizing relation Slot (suggering from AU)")

        if slot_au is None:
            slot_au = self.slot_au

        slot_au = slot_au[
            ['id_slot', 'id_doctor_histo', 'date', 'time', 'type',
            'id_doctor']
        ].copy()
        slot_au.rename(
            columns={
                'id_doctor': 'id_doctor_v1',
                'id_doctor_histo': 'id_doctor'
            },
            inplace=True)

        self.slot_au = slot_au
        return slot_au

    #----------------------------------------------------------------
    def finalize_doctor(self,
        doctor_au: pd.DataFrame = None
        ) -> pd.DataFrame:
        """
            Transform old id_doctor into id_doctor_v1 for reference and
            to get the accurate reference classes, and new id_doctor_au
            into the main identifier id_doctor, and keep only required
            attributes
        """
        logging.info("Finalizing relation Doctor (suggering from AU)")
        
        if doctor_au is None:
            doctor_au = self.doctor_au

        doctor_au = doctor_au[
            ['id_doctor_histo', 'first_name', 'last_name', 'specialty',
            'license_number', 'start_date', 'end_date', 'period_start_date',
            'period_end_date', 'max_monthly_hours', 'id_doctor']
        ].copy()
        doctor_au.rename(
            columns={
                'id_doctor': 'id_doctor_v1',
                'id_doctor_histo': 'id_doctor'
            },
            inplace=True)

        self.doctor_au = doctor_au
        return doctor_au

