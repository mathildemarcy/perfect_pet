import pandas as pd
import numpy as np
import random as rd
from datetime import date, timedelta
from shared_functions import add_primary_key_values
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="- %(levelname)s - %(asctime)s - %(message)s"
)


class Service():
    """
        This class is used to create the relations service and
        appointment_service, and progressively generate the 
        required data. The self contained DataFrames service_data 
        and appointment_services_data are updated as data
        generation progresses.
    """
    def __init__(self,
        service_data: pd.DataFrame
        ):
        """
            Initialize the instance with the data contained in
            csv file service_list.csv included in the package.
        """
        logging.info("Instantiating object from class Service")

        self.service_data = service_data
        self.appointment_services_data = []
        logging.info(f"Generated Service relation of size "
                     f"{len(self.service_data)}.")

    #---------------------------------------------------------------- 
    def generate_service_data_df(self):
        """
            Return the current state of the DataFrame service_data
        """
        logging.info("Return the Service relation's data")
        return self.service_data

    #---------------------------------------------------------------- 
    def assign_id_service(self,
        service_data: pd.DataFrame = None,
        pk_column_name: str = 'id_service',
        starting_id_value: int = 1
        ):
        """
            Creates a new column ('id_service') to the DataFrame
            service_data to serve as main identifier. 
            Returns the DataFrame with the newly added id.
            Order of rows does not matter, no need to sort first.
        """
        logging.info("Adding PK id_service to relation Service")

        if service_data is None or len(service_data) == 0:
            service_data = self.service_data

        service_data = add_primary_key_values(
            relation=service_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        self.service_data = service_data
        return service_data

    #---------------------------------------------------------------- 
    def map_appointment_services(self,
        appointment_data: pd.DataFrame,
        appt_reason_service_list: pd.DataFrame,
        surgery_types_distribution: pd.DataFrame,
        service_data: pd.DataFrame = None
        ):
        """
            Create the DataFrame appointment_services_data to associate
            appointments with services, from previously created DataFrame
            appointment_data, and external dataset included in the package:
            appt_reason_service_list and surgery_types_distribution.
            The data from these files can be modified, but the correspondance
            between their values should be maintained.

        """
        logging.info(f"Start creating relation Appointment_Service "
                     f"containing the mapping of services to each appointment.")

        if service_data is None or len(service_data) == 0:
            service_data = self.service_data

        if 'id_service' not in service_data.columns:
            raise KeyError(f"Required column 'id_service' not found"
                           f"in DataFrame 'service_data'")

        rows = []

        reason_services_map = {
            reason: group.reset_index(drop=True)
            for reason, group in appt_reason_service_list.groupby('reason')
        }

        for row in appointment_data.itertuples(index=False):
            id_appointment = row.id_appointment
            reason = row.appt_reason

            if reason == 'surgery':
                surgery_prop = rd.uniform(0, 1)
                matched = surgery_types_distribution[
                    (surgery_types_distribution['min_prop'] < surgery_prop) &
                    (surgery_types_distribution['max_prop'] >= surgery_prop)
                ]
                if not matched.empty:
                    service_type = matched['type'].values[0]
                    id_service_type = service_data[
                        service_data['service_name'] == service_type
                        ]['id_service'].values[0]
                    rows.append({
                        'id_appointment': id_appointment,
                        'id_service': id_service_type
                    })

            services = reason_services_map.get(reason)
            if services is not None:
                n = len(services)
                props = [rd.uniform(0, 1) for _ in range(n)]
                for i in range(n):
                    if 1 - services.loc[i, 'probability'] > props[i]:
                        service_type = services.loc[i, 'service']
                        id_service = service_data[
                            service_data['service_name'] == service_type
                            ]['id_service'].values[0]
                        rows.append({
                            'id_appointment': id_appointment,
                            'id_service': id_service
                        })

        appointment_services_data = pd.DataFrame(
            rows,
            columns=['id_appointment', 'id_service'])

        logging.info(f"Generated Appointment_Service relation of size "
                     f"{len(appointment_services_data)}.")
        self.appointment_services_data = appointment_services_data
        return appointment_services_data

    #----------------------------------------------------------------
    def assign_id_appointment_service(self,
        appointment_services_data: pd.DataFrame,
        pk_column_name: str = 'id_appointment_service',
        starting_id_value: int = 1
        ):
        """
            Creates a new column ('id_appointment_service') to the DataFrame
            appointment_services_data to serve as main identifier.
            Returns the DataFrame with the newly added id.
            The DataFrame appointment_services_data is sorted by appointment
            and service before within this function.
        """
        logging.info(f"Adding PK id_appointment_service to relation "
                     f"Appointment_Service.")

        appointment_services_data = appointment_services_data.sort_values(
            ['id_appointment', 'id_service']
            ).reset_index(drop=[True, True])

        appointment_services_data = add_primary_key_values(
            relation=appointment_services_data,
            pk_column_name=pk_column_name,
            starting_id_value=starting_id_value
        )

        self.appointment_services_data = appointment_services_data

        return appointment_services_data