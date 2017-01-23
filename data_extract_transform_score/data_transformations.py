import csv
from db_classes import PipelineJobDataTranformationStep, DataTransformationStep

class DataTransformation(object):
    """Base class for representing a data transformation89jhuuu*/"""

    def run(self):
        pass

    def set_connection_and_meta_data(self, connection, meta_data):
        """This method will be called by the JobRunner"""
        self.connection = connection
        self.meta_data = meta_data

    def set_pipeline_job_data_transformation_id(self, pipeline_job_data_transformation_id):
        """This method will be called by the JobRunner"""
        self.pipeline_job_data_transformation_id = pipeline_job_data_transformation_id
        self.pipeline_job_data_transformation_obj = PipelineJobDataTranformationStep(self.connection, self.meta_data)
        self.pipeline_job_data_trans_row = self.pipeline_job_data_transformation_obj.find_by_id(self.pipeline_job_data_transformation_id)
        self.data_transformation_step_id = self.pipeline_job_data_trans_row.data_transformation_step_id

        self.data_transformation_obj = DataTransformationStep(self.connection, self.meta_data)
        self.data_transformation_row = self.data_transformation_obj.find_by_id(self.data_transformation_step_id)


class ClientServerDataTransformation(DataTransformation):
    """Represents where the client reads into the DB server, e.g., reading a flat file"""
    pass


class ServerClientDataTransformation(DataTransformation):
    """Represents where the server read out to the client e.g., writing a flat file"""
    pass


class ServerClientServerDataTransformation(DataTransformation):
    """Represents where the client reads from the server, does something, and writes back to the server"""
    pass


class ServerServerDataTransformation(DataTransformation):
    """Represents where the transformation happens on the server and results are stored on the server"""
    pass


class ReadFileIntoDB(ClientServerDataTransformation):
    def __init__(self, file_name, file_type, common_id_field_name, delimiter=","):
        pass


