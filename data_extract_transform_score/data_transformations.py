import csv
import datetime
from db_classes import PipelineJobDataTranformationStep, DataTransformationStep, DataTransformationDB
from sqlalchemy import text
import models
import json


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
            result_proxy = self.connection.execute(sql_statement)
        else:
            result_proxy = self.connection.execute(text(sql_statement), **parameter_dict)

        return result_proxy

    def _schema_name(self):
        schema = self.meta_data.schema
        if schema is None:
            schema_text = ""
        else:
            schema_text = schema + "."
        return schema_text

    def _write_data(self, data, common_id, meta=None):
        dict_to_write = {"data": data, "common_id": common_id, "meta": meta}
        dict_to_write["pipeline_job_data_transformation_step_id"] = self.pipeline_job_data_transformation_step_id
        dict_to_write["created_at"] = datetime.datetime.utcnow()
        self.data_transformation_obj.insert_struct(dict_to_write)


    def _get_data_transformation_step_proxy(self, step_number):

        schema = self._schema_name()

        sql_expression = """
select dt.* from %sdata_transformations dt
    join %spipeline_jobs_data_transformation_steps pjdts
        on dt.pipeline_job_data_transformation_step_id = pjdts.id and pjdts.pipeline_job_id = :pipeline_job_id
    join %sdata_transformation_steps dts on pjdts.data_transformation_step_id = dts.id
    where dts.step_number = :step_number""" % (schema, schema, schema)

        result_proxy = self._sql_statement_execute(sql_expression, {"pipeline_job_id": self.pipeline_job_id, "step_number": step_number})

        return result_proxy



class ClientServerDataTransformation(DataTransformation):
    """Represents where the client reads into the DB server, e.g., reading a flat file"""


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

    def __init__(self, step_number, field_name=None):
        self.step_number = step_number
        self.field_name = field_name

    def run(self):

        schema_text = self._schema_name()

        if self.field_name is None:
            data_sql_bit = "jsonb_agg(dt.data order by dt.id)"
        else:
            data_sql_bit = "jsonb_insert('{}'::jsonb, '{%s}', jsonb_agg(dt.data order by dt.id))" % self.field_name

        sql_statement = """
insert into %sdata_transformations (common_id, data, meta, created_at, pipeline_job_data_transformation_step_id)
select common_id, %s, jsonb_agg(dt.meta order by dt.id) as meta,
  cast(now() as timestamp) at time zone 'utc', :pipeline_job_data_transformation_step_id
    from %sdata_transformations dt
    join %spipeline_jobs_data_transformation_steps pjdts on dt.pipeline_job_data_transformation_step_id = pjdts.id
    join %sdata_transformation_steps dts on pjdts.data_transformation_step_id = dts.id
    where step_number = :step_number and pjdts.pipeline_job_id = :pipeline_job_id
    group by dt.common_id order by common_id""" % (schema_text, data_sql_bit, schema_text, schema_text, schema_text)


        self._sql_statement_execute(sql_statement, {"step_number": self.step_number,
                                                    "pipeline_job_id": self.pipeline_job_id,
                                                    "pipeline_job_data_transformation_step_id": self.pipeline_job_data_transformation_step_id
                                                    })


class MergeData(ServerServerDataTransformation):
    """Merge JSON in data by the common id. Assumption here is the common_id field is unique"""

    def __init__(self, step_number_1, step_number_2):
        self.step_number_1 = step_number_1
        self.step_number_2 = step_number_2

    def run(self):

        schema = self._schema_name()

        sql_statement = """
insert into %sdata_transformations (common_id, data, meta, created_at, pipeline_job_data_transformation_step_id)
  select t1.common_id,
    case when t2.data is not null then t1.data || t2.data else t1.data end as data, json_build_array(t1.id, t2.id) as meta,
    cast(now() as timestamp) at time zone 'utc', :pipeline_job_data_transformation_step_id
from (
    select dt1.* from %sdata_transformations dt1
        join %spipeline_jobs_data_transformation_steps pjdts1
            on dt1.pipeline_job_data_transformation_step_id = pjdts1.id and pjdts1.pipeline_job_id = :pipeline_job_id
        join %sdata_transformation_steps dts1 on pjdts1.data_transformation_step_id = dts1.id and step_number = :step_number_1) t1
    left outer join (
    select dt2.* from %sdata_transformations dt2
        join %spipeline_jobs_data_transformation_steps pjdts2
            on dt2.pipeline_job_data_transformation_step_id = pjdts2.id and pjdts2.pipeline_job_id = :pipeline_job_id
        join %sdata_transformation_steps dts2 on pjdts2.data_transformation_step_id = dts2.id and step_number = 3) t2
    on t1.common_id = t2.common_id

        """ % (schema, schema, schema, schema, schema, schema, schema)

        self._sql_statement_execute(sql_statement, {"step_number_1": self.step_number_1,
                                                    "step_number_2": self.step_number_2,
                                                    "pipeline_job_id": self.pipeline_job_id,
                                                    "pipeline_job_data_transformation_step_id": self.pipeline_job_data_transformation_step_id
                                                    })


class MapDataWithDict(ServerClientServerDataTransformation):

    def __init__(self, fields_to_map, step_number, json_file_name=None, mapping_rules=None):

        self.fields_to_map = fields_to_map
        self.step_number = step_number

        if json_file_name is not None:
            with open(json_file_name, "r") as f:
                self.mappng_rules = json.load(f)
        else:
            self.mapping_rules = mapping_rules

    def run(self):

        result_proxy = self._get_data_transformation_step_proxy(self.step_number)
        for result in result_proxy:

            result_data = result.data

            if self.fields_to_map[0] in result_data:
                result_value = result_data[self.fields_to_map[0]]
                result_mapped_dict = {}
                meta_list = []
                #TODO: Clean up

                if result_value.__class__ == [].__class__:
                    for element in result_value:
                        if element.__class__ == {}.__class__:
                            field_key = self.fields_to_map[1]
                            if field_key in element:
                                field_value = element[field_key]
                                if field_value in self.mapping_rules:
                                    result_mapped_dict[self.mapping_rules[field_value]] = 1
                                    meta_list += [{field_value: self.mapping_rules[field_value]}]

                self._write_data(result_mapped_dict, result.common_id, meta_list)

            else:
                pass


class ScoreData(ServerClientServerDataTransformation):
    """Handles scoring of datas against a model"""

    def __init__(self, step_number, model_name, model_parameters):

        self.step_number = step_number
        self.model_name = model_name
        self.model_parameters = model_parameters

        self.model_registry = models.ModelsRegistry()

        self.model = self.model_registry.model_name_class_dict[self.model_name]
        self.model_obj = self.model(model_parameters)

    def run(self):
        row_proxy = self._get_data_transformation_step_proxy(self.step_number)
        for row_obj in row_proxy:
            score_result, meta = self.model_obj.score(row_obj.data)
            self._write_data(score_result, row_obj.common_id, meta)




