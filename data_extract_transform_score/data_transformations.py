import csv


class DataTransformation(object):
    def run(self):
        pass

    def _set_connection_and_meta_data(self, connection, meta_data):
        """This method will be called by the JobRunner"""
        self.connection = connection
        self.meta_data = meta_data

    def _set_pipeline_job_id(self, pipeline_job_id):
        """This method will be called by the JobRunner"""

        self.pipeline_job_id = pipeline_job_id


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