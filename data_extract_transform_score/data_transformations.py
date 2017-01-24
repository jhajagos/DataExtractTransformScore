import csv
import datetime
from db_classes import PipelineJobDataTranformationStep, DataTransformationStep, DataTransformationDB
from sqlalchemy import text

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
        self.pipeline_job_data_transformation_step_id = pipeline_job_data_transformation_id
        self.pipeline_job_data_transformation_obj = PipelineJobDataTranformationStep(self.connection, self.meta_data)
        self.pipeline_job_data_trans_row = self.pipeline_job_data_transformation_obj.find_by_id(self.pipeline_job_data_transformation_step_id)
        self.pipeline_job_id = self.pipeline_job_data_trans_row.pipeline_job_id
        self.data_transformation_step_id = self.pipeline_job_data_trans_row.data_transformation_step_id

        self.data_transformation_obj = DataTransformationDB(self.connection, self.meta_data)
        #self.data_transformation_step_row = self.data_transformation_obj.find_by_id(self.data_transformation_step_id)

    def _sql_statement_execute(self, sql_statement, parameter_dict=None):
        if parameter_dict is None:
            self.connection.execute(sql_statement)
        else:
            self.connection.execute(text(sql_statement), **parameter_dict)



class ClientServerDataTransformation(DataTransformation):
    """Represents where the client reads into the DB server, e.g., reading a flat file"""
    def _write_data(self, data, common_id, meta=None):
        dict_to_write = {"data": data, "common_id": common_id, "meta": meta}
        dict_to_write["pipeline_job_data_transformation_step_id"] = self.pipeline_job_data_transformation_step_id
        dict_to_write["created_at"] = datetime.datetime.utcnow()
        self.data_transformation_obj.insert_struct(dict_to_write)


class ServerClientDataTransformation(DataTransformation):
    """Represents where the server read out to the client e.g., writing a flat file"""


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


class CoalesceData(ServerServerDataTransformation):
    """Aggregate JSON in data by the common id"""

    def __init__(self, step_number):
        self.step_number= step_number

    def run(self):

        schema = self.meta_data.schema
        if schema is None:
            schema_text = ""
        else:
            schema_text = schema + "."

        sql_statement = """
insert into %sdata_transformations (common_id, data, meta, created_at, pipeline_job_data_transformation_step_id)
select common_id, jsonb_agg(dt.data order by dt.id) as data, jsonb_agg(dt.meta order by dt.id) as meta,
  cast(now() as timestamp) at time zone 'utc', :pipeline_job_data_transformation_step_id
    from %sdata_transformations dt
    join %spipeline_jobs_data_transformation_steps pjdts on dt.pipeline_job_data_transformation_step_id = pjdts.id
    join %sdata_transformation_steps dts on pjdts.data_transformation_step_id = dts.id
    where step_number = :step_number and pjdts.pipeline_job_id = :pipeline_job_id
    group by dt.common_id order by common_id""" % (schema_text, schema_text, schema_text, schema_text)


        self._sql_statement_execute(sql_statement, {"step_number": self.step_number,
                                                    "pipeline_job_id": self.pipeline_job_id,
                                                    "pipeline_job_data_transformation_step_id": self.pipeline_job_data_transformation_step_id
                                                    })

        """
        select
        jsonb_insert('{}'::jsonb, '{dx}', data) from testing.data_transformations dt
        where
        dt.pipeline_job_data_transformation_step_id = 3;
        """

