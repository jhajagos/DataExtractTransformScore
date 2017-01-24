import csv
from db_classes import PipelineJobDataTranformationStep, DataTransformationStep, DataTransformationDB

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
        self.pipeline_job_id = self.pipeline_job_data_trans_row.pipeline_job_id
        self.data_transformation_step_id = self.pipeline_job_data_trans_row.data_transformation_step_id

        self.data_transformation_obj = DataTransformationDB(self.connection, self.meta_data)
        #self.data_transformation_step_row = self.data_transformation_obj.find_by_id(self.data_transformation_step_id)


class ClientServerDataTransformation(DataTransformation):
    """Represents where the client reads into the DB server, e.g., reading a flat file"""
    def _write_data(self, data, common_id, meta=None):
        dict_to_write = {"data": data, "common_id": common_id, "meta": meta}
        dict_to_write["pipeline_job_data_transformation_id"] = self.pipeline_job_data_transformation_id
        self.data_transformation_obj.insert_struct(dict_to_write)


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
        self.file_name = file_name
        self.common_id_field_name = common_id_field_name
        self.file_type = file_type
        self.delimiter = delimiter

    def run(self):
        if self.file_type == "csv":
            with open(self.file_name) as f:
                csv_dict_reader = csv.DictReader(f)
                i = 0
                for row_dict in csv_dict_reader:
                    common_id = row_dict[self.common_id_field_name]
                    data = row_dict
                    meta = {"row": i}
                    self._write_data(data, common_id, meta=meta)
                    i += 1





