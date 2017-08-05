import csv
import datetime
from db_classes import PipelineJobDataTranformationStep, DataTransformationStep, DataTransformationDB
from transformations import TransformationsRegistry
from sqlalchemy import text
import models
import json
import os


class DataTransformation(object):
    """Base class for representing a data transformation"""

    def run(self):
        pass

    def set_connection_and_meta_data(self, connection, meta_data):
        """This method will be called by the JobRunner"""
        self.connection = connection
        self.meta_data = meta_data

    def set_file_directory(self, file_directory):
        self.file_directory = file_directory

    def set_pipeline_job_data_transformation_id(self, pipeline_job_data_transformation_id):
        """This method will be called by the JobRunner"""
        self.pipeline_job_data_transformation_step_id = pipeline_job_data_transformation_id
        self.pipeline_job_data_transformation_obj = PipelineJobDataTranformationStep(self.connection, self.meta_data)
        self.pipeline_job_data_trans_row = self.pipeline_job_data_transformation_obj.find_by_id(self.pipeline_job_data_transformation_step_id)
        self.pipeline_job_id = self.pipeline_job_data_trans_row.pipeline_job_id
        self.data_transformation_step_id = self.pipeline_job_data_trans_row.data_transformation_step_id

        self.data_transformation_step_obj = DataTransformationStep(self.connection, self.meta_data)
        self.data_transformation_step_row = self.data_transformation_step_obj.find_by_id(self.data_transformation_step_id)

        self.data_transformation_obj = DataTransformationDB(self.connection, self.meta_data)

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
    """Read a fine into a database"""
    def __init__(self, file_name, file_type, common_id_field_name, delimiter=","):
        self.file_name = file_name
        self.common_id_field_name = common_id_field_name
        self.file_type = file_type
        self.delimiter = delimiter

    def run(self):
        if self.file_type == "csv":

            localized_file_name = os.path.abspath(os.path.join(self.file_directory, self.file_name))
            with open(localized_file_name, "rb") as f:
                csv_dict_reader = csv.DictReader(f)
                i = 0
                for row_dict in csv_dict_reader:
                    common_id = row_dict[self.common_id_field_name]
                    data = row_dict
                    meta = {"row": i}
                    self._write_data(data, common_id, meta=meta)
                    i += 1
        else:
            raise RuntimeError


class FilterBy(ServerServerDataTransformation):
    """Filters and selects a JSONB data or meta_data element"""

    def __init__(self, step_number, filter_criteria, field_name=None):
        self.step_number = step_number
        self.filter_criteria = filter_criteria
        self.field_name = field_name

    def run(self):
        schema_text = self._schema_name()

        if self.field_name is None:
            data_sql_bit = '"data"'
        else:
            data_sql_bit = """jsonb_insert('{}'::jsonb, '{%s}', "data")""" % self.field_name

        sql_statement = """
        insert into %sdata_transformations (common_id, data, meta, created_at, pipeline_job_data_transformation_step_id)
        select common_id, %s, meta,
          cast(now() as timestamp) at time zone 'utc', :pipeline_job_data_transformation_step_id
            from   %sdata_transformations dt 
              join %spipeline_jobs_data_transformation_steps pjdts on pjdts.id = dt.pipeline_job_data_transformation_step_id
              join %sdata_transformation_steps dts ON dts.id = pjdts.data_transformation_step_id
              join %spipeline_jobs pj on pj.id = pjdts.pipeline_job_id
              where dts.step_number = :step_number and pj.id = :pipeline_job_id
                and (%s)                        
            """ % (schema_text, data_sql_bit, schema_text, schema_text, schema_text, schema_text, self.filter_criteria)

        self._sql_statement_execute(sql_statement, {"step_number": self.step_number,
                                                    "pipeline_job_id": self.pipeline_job_id,
                                                    "pipeline_job_data_transformation_step_id": self.pipeline_job_data_transformation_step_id
                                                    })


class CoalesceData(ServerServerDataTransformation):
    """Aggregate JSON in data by the common id into a list"""

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


class SwapMetaToData(ServerServerDataTransformation):
    """Take metadata values and swap to data values"""
    def __init__(self, step_number):
        self.step_number = step_number

    def run(self):
        schema_text = self._schema_name()

        sql_statement = """
                insert into %sdata_transformations (common_id, data, meta, created_at, pipeline_job_data_transformation_step_id)
                select common_id, meta, NULL,
                  cast(now() as timestamp) at time zone 'utc', :pipeline_job_data_transformation_step_id
                    from   %sdata_transformations dt 
                      join %spipeline_jobs_data_transformation_steps pjdts on pjdts.id = dt.pipeline_job_data_transformation_step_id
                      join %sdata_transformation_steps dts ON dts.id = pjdts.data_transformation_step_id
                      join %spipeline_jobs pj on pj.id = pjdts.pipeline_job_id
                      where dts.step_number = :step_number and pj.id = :pipeline_job_id
                                          
                    """ % (
        schema_text, schema_text, schema_text, schema_text, schema_text)

        self._sql_statement_execute(sql_statement, {"step_number": self.step_number,
                                                    "pipeline_job_id": self.pipeline_job_id,
                                                    "pipeline_job_data_transformation_step_id": self.pipeline_job_data_transformation_step_id
                                                    })

class MergeData(ServerServerDataTransformation):
    """Merge JSON in data by the common id.
    Assumption here is the common_id field is unique"""

    def __init__(self, step_numbers):

        step_number_pairs = []
        for step_number in step_numbers:
            if step_number.__class__ == [].__class__:
                step_number_pairs += [(step_number[0], step_number[1])]
            else:
                step_number_pairs += [(step_number, None)]

        self.step_number_pairs = step_number_pairs

    def _field_name_keyed(self, field_name, alias):
        if field_name is None:
            data_sql_bit = "%s.data" % alias
        else:
            data_sql_bit = "jsonb_insert('{}'::jsonb, '{%s}', %s.data)" % (field_name, alias)

        return data_sql_bit

    def run(self):
        schema = self._schema_name()

        step_number_1, field_name_1 = self.step_number_pairs[0]
        step_number_2, field_name_2 = self.step_number_pairs[1]

        field_name_expanded_1 = self._field_name_keyed(field_name_1, "dt1")
        field_name_expanded_2 = self._field_name_keyed(field_name_2, "dt2")

        sql_statement = """
insert into %sdata_transformations (common_id, data, meta, created_at, pipeline_job_data_transformation_step_id)
  select t1.common_id,
    case when t2.data is not null then t1.data || t2.data else t1.data end as data, json_build_array(t1.id, t2.id) as meta,
    cast(now() as timestamp) at time zone 'utc', :pipeline_job_data_transformation_step_id
from (
    select dt1.id, dt1.common_id, %s as data from %sdata_transformations dt1
        join %spipeline_jobs_data_transformation_steps pjdts1
            on dt1.pipeline_job_data_transformation_step_id = pjdts1.id and pjdts1.pipeline_job_id = :pipeline_job_id
        join %sdata_transformation_steps dts1 on pjdts1.data_transformation_step_id = dts1.id and step_number = :step_number_1) t1
    left outer join (
    select dt2.id, dt2.common_id, %s as data from %sdata_transformations dt2
        join %spipeline_jobs_data_transformation_steps pjdts2
            on dt2.pipeline_job_data_transformation_step_id = pjdts2.id and pjdts2.pipeline_job_id = :pipeline_job_id
        join %sdata_transformation_steps dts2 on pjdts2.data_transformation_step_id = dts2.id and step_number = :step_number_2) t2
    on t1.common_id = t2.common_id
        """ % (schema, field_name_expanded_1, schema, schema, schema, field_name_expanded_2, schema, schema, schema)

        self._sql_statement_execute(sql_statement, {"step_number_1": step_number_1,
                                                    "step_number_2":  step_number_2,
                                                    "pipeline_job_id": self.pipeline_job_id,
                                                    "pipeline_job_data_transformation_step_id": self.pipeline_job_data_transformation_step_id
                                                    })

        if len(self.step_number_pairs) > 2:

            for step_number_pair in self.step_number_pairs[2:]:
                step_number_2, field_name_2 = step_number_pair
                field_name_expanded_2 = self._field_name_keyed(field_name_2, "dt2")

                sql_expression = """
                update %sdata_transformations as dtp
            set data =
                case when t2.data is not null then t1.data || t2.data else t1.data end,
                 meta = t1.meta || jsonb_build_array(t2.id)
                 from
                (select dt1.id, dt1.data, dt1.common_id, dt1.meta from
                     %sdata_transformations dt1 join
                        %spipeline_jobs_data_transformation_steps pjdts1
                        on pjdts1.id = dt1.pipeline_job_data_transformation_step_id and pjdts1.pipeline_job_id = :pipeline_job_id
                    join %sdata_transformation_steps dts1
                        on dts1.id = pjdts1.data_transformation_step_id and dts1.step_number = :current_step_number) t1
                left outer join
                (select dt2.id, dt2.common_id, %s as data from
                     %sdata_transformations dt2 join
                     %spipeline_jobs_data_transformation_steps pjdts2
                        on pjdts2.id = dt2.pipeline_job_data_transformation_step_id and pjdts2.pipeline_job_id = :pipeline_job_id
                    join %sdata_transformation_steps dts2
                        on dts2.id = pjdts2.data_transformation_step_id and dts2.step_number = :step_number_2) t2
                on t2.common_id = t1.common_id where dtp.id = t1.id
                """ % (schema, schema, schema, schema, field_name_expanded_2, schema, schema, schema)

                self._sql_statement_execute(sql_expression, {"pipeline_job_id": self.pipeline_job_id,
                                                             "step_number_2": step_number_2,
                                                             "current_step_number": self.data_transformation_step_row.step_number
                                                             })


class TransformIndicatorListToDict(ServerClientServerDataTransformation):
    def __init__(self, step_number):
        self.step_number = step_number

    def run(self):
        result_proxy = self._get_data_transformation_step_proxy(self.step_number)
        for result in result_proxy:

            indicator_dict = {}
            for indicator in result.data:
                indicator_dict[indicator] = 1.0

            self._write_data(indicator_dict, result.common_id, None)



class MapDataWithDict(ServerClientServerDataTransformation):
    """Create an indicator flag based on a look-up of a table"""
    def __init__(self, fields_to_map, step_number, json_file_name=None, mapping_rules=None, field_name=None):

        if fields_to_map.__class__ != [].__class__:
            self.fields_to_map = [fields_to_map]

        self.fields_to_map = fields_to_map # Can be a list of fields to descend into
        self.step_number = step_number
        self.json_file_name = json_file_name
        self.mapping_rules = mapping_rules
        self.field_name = field_name

    def run(self):

        if self.json_file_name is not None:
            local_json_file_name = os.path.abspath(os.path.join(self.file_directory, self.json_file_name))
            with open(local_json_file_name, "r") as f:
                self.mapping_rules = json.load(f)

        result_proxy = self._get_data_transformation_step_proxy(self.step_number)
        for result in result_proxy:

            result_data = result.data

            i = 1
            for field_to_map in self.fields_to_map:  # Traverse down to the field
                if field_to_map not in result_data and i < len(self.fields_to_map):
                    break
                else:

                    if i == len(self.fields_to_map):
                        data_list = []
                        meta_list = []

                        if result_data.__class__ == [].__class__:
                            result_value = result_data
                        else:
                            result_value = [result_data]

                        for element in result_value:

                            if element.__class__ == {}.__class__:
                                field_key = field_to_map

                                if field_key in element:
                                    field_value = element[field_key]

                                    if field_value in self.mapping_rules:

                                        if self.mapping_rules[field_value].__class__ in ([].__class__, u"".__class__, {}.__class__):
                                            mapped_value = self.mapping_rules[field_value]

                                            if mapped_value.__class__ != [].__class__:
                                                mapped_value = [mapped_value]

                                            data_list += mapped_value
                                            meta_list += [{field_value: self.mapping_rules[field_value]}]

                        if self.field_name is not None:
                            data = {self.field_name: data_list}
                        else:
                            data = data_list

                        self._write_data(data, result.common_id, meta_list)

                    elif i < len(self.fields_to_map):
                        try:
                            result_data = result_data[field_to_map]
                        except TypeError:
                            break
                    i += 1


class TransformDataWithFunction(ServerClientServerDataTransformation):
    def __init__(self, step_number, transformation_name):

        self.step_number = step_number
        try:
            import localized_dets as ld
            self.transformation_registry = TransformationsRegistry(ld.LOCAL_TRANSFORMATIONS_TO_REGISTER)
        except ImportError:
            self.transformation_registry = TransformationsRegistry()

        self.transformation_func = self.transformation_registry.transformation_name_dict[transformation_name]

    def run(self):
        row_proxy = self._get_data_transformation_step_proxy(self.step_number)
        for row_obj in row_proxy:
            data, meta = self.transformation_func(row_obj.data)
            # print(data, meta)
            # raise RuntimeError
            self._write_data(data, row_obj.common_id, meta)


class ScoreData(ServerClientServerDataTransformation):
    """Handles scoring of data against a model"""

    def __init__(self, step_number, model_name, model_parameters):

        self.step_number = step_number
        self.model_name = model_name
        self.model_parameters = model_parameters

        try:
            import localized_dets as ld
            self.model_registry = models.ModelsRegistry(ld.LOCAL_MODELS_TO_REGISTER)
        except ImportError:
            self.model_registry = models.ModelsRegistry()

        self.model = self.model_registry.model_name_class_dict[self.model_name]
        self.model_obj = self.model(model_parameters)

    def run(self):
        row_proxy = self._get_data_transformation_step_proxy(self.step_number)
        for row_obj in row_proxy:
            score_result, meta = self.model_obj.score(row_obj.data)
            meta["model name"] = self.model_name
            self._write_data({"score": score_result}, row_obj.common_id, meta)


class WriteFile(ServerClientDataTransformation):
    """Write file to client filesystem from the server database"""

    def __init__(self, step_number, file_name, file_type, fields_to_export=None):
        self.step_number = step_number
        self.file_name = file_name
        self.file_type = file_type
        self.fields_to_export = fields_to_export

    def run(self):
        row_proxy = self._get_data_transformation_step_proxy(self.step_number)

        localized_file_name = os.path.abspath(os.path.join(self.file_directory, self.file_name))

        result_list = []
        if self.file_type == "JSON":

            for row in row_proxy:
                result_list += [row.data]
            with open(localized_file_name, "w") as fw:
                json.dump(result_list, fw, sort_keys=True, indent=4, separators=(',', ': '))
        else:
            raise RuntimeError