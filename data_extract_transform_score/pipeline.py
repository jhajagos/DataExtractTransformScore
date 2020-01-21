"""
Classes for creating and running pipelines against a Postgresql database schema defined
in schema_define.py
"""

import sys
import os


try:
    from data_transformations import *
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0])))
    from .data_transformations import *

import datetime

try:
    from db_classes import *
except ImportError:
    from .db_classes import *


class DataTransformationStepClasses(object):
    """The data translation step class name is registered with a class"""

    def __init__(self):
        self.step_class_callable_obj_dict = {}

        self._register("Load file", ReadFileIntoDB)
        self._register("Coalesce", CoalesceData)
        self._register("Merge", MergeData)
        self._register("Map with Dict", MapDataWithDict)
        self._register("Score", ScoreData)
        self._register("Write file", WriteFile)
        self._register("Transform with function", TransformDataWithFunction)
        self._register("Filter by", FilterBy)
        self._register("Swap metadata to data", SwapMetaToData)
        self._register("Transform indicator list to dict", TransformIndicatorListToDict)
        self._register("Load from DB by query", ReadDataFromExternalDBQuery)
        self._register("Load from DB by id", ReadDataFromExternalDBQueryById)

    def _register(self, data_transformation_step_class_name, class_obj):
        self.step_class_callable_obj_dict[data_transformation_step_class_name] = class_obj

    def get_by_class_name(self, class_name):

        if class_name in self.step_class_callable_obj_dict:
            return self.step_class_callable_obj_dict[class_name]
        else:
            return None


class Pipeline(DBClassName):
    """Class and methods for working with pipeline into the database for processing"""

    def load_steps_into_db(self, pipeline_structure):

        data_trans_fields = ["step_number", "name", "parameters", "description"]

        self.raw_pipeline_structure = pipeline_structure
        self.raw_db_pipeline_structure = []

        for element in pipeline_structure:
            data_transformation_step_dict = {}
            data_transformation_class = element["data_transformation_class"]
            data_transformation_class_obj = DataTransformationStepClass(data_transformation_class,
                                                                        self.connection, self.meta_data)
            for field in data_trans_fields:
                if field in element:
                    data_transformation_step_dict[field] = element[field]

            data_transformation_step_dict["pipeline_id"] = self.get_id()
            data_transformation_step_dict["data_transformation_step_class_id"] = data_transformation_class_obj.get_id()

            self.raw_db_pipeline_structure += [data_transformation_step_dict]

        data_transformation_steps_obj = DataTransformationStep(self.connection, self.meta_data)
        for data_step_dict in self.raw_db_pipeline_structure:
            data_transformation_steps_obj.insert_struct(data_step_dict)

    def rename_pipeline(self, new_name):

        update_struct = {"name": new_name}
        self.update_struct(self.get_id(), update_struct)

    def _table_name(self):
        return "pipelines"


class Jobs(object):
    """Class for running and executing jobs"""

    def __init__(self, name, connection, meta_data, file_directory="./",
                 external_data_connections_dict=None):
        self.connection = connection
        self.meta_data = meta_data
        self.file_directory = file_directory
        self.job_id = None
        self.job_obj = None
        self.pipelines = []
        self.pipeline_jobs_ids = []
        self.name = name
        self.external_data_connections_dict = external_data_connections_dict

        self.data_trans_step_classes_obj = DataTransformationStepClasses()

    def create_jobs_to_run(self, pipelines):

        # TODO: Get last job

        if pipelines.__class__ == [].__class__:
            self.pipelines = pipelines
        else:
            self.pipelines = [pipelines]

        self.job_obj = Job(self.connection, self.meta_data)

        not_start_obj = JobStatus("Not started", self.connection, self.meta_data)
        job_dict = {"job_status_id": not_start_obj.get_id(),
                    "name": self.name,
                    "start_date_time": datetime.datetime.utcnow(),
                    "is_active": True}

        self.job_id = self.job_obj.insert_struct(job_dict)

        pipeline_job_obj = PipelineJob(self.connection, self.meta_data)
        for pipeline in self.pipelines:
            pipeline_obj = Pipeline(pipeline, self.connection, self.meta_data)
            pipeline_id = pipeline_obj.get_id()

            pipeline_obj_dict = {"job_id": self.job_id, "pipeline_id": pipeline_id, "job_status_id": not_start_obj.get_id(),
                                 "start_date_time": datetime.datetime.utcnow(), "is_active": True}

            pipeline_job_obj.insert_struct(pipeline_obj_dict)

    def run_job(self, with_transaction_rollback=False):
        """Execute the job"""

        data_transformation_step_obj = DataTransformationStep(self.connection, self.meta_data)

        data_transformation_step_class_obj = DataTransformationStepClassDB(self.connection, self.meta_data)

        pipeline_job_data_trans_obj = PipelineJobDataTranformationStep(self.connection, self.meta_data)

        start_obj = JobStatus("Started", self.connection, self.meta_data)
        finished_obj = JobStatus("Finished", self.connection, self.meta_data)

        pipeline_job_obj = PipelineJob(self.connection, self.meta_data)

        for pipeline in self.pipelines:
            pipeline_obj = Pipeline(pipeline, self.connection, self.meta_data)
            pipeline_id = pipeline_obj.get_id()

            pjd_row_obj = pipeline_job_obj.find_by_job_id_and_pipeline_id(self.job_id, pipeline_id)

            pipeline_job_obj.update_struct(pjd_row_obj.id, {"job_status_id": start_obj.get_id()})
            data_transform_step_objects = data_transformation_step_obj.find_by_pipeline_id(pipeline_id)

            for data_transform_step in data_transform_step_objects:

                pipeline_job_data_trans_step_dict = {"data_transformation_step_id": data_transform_step.id,
                                                     "pipeline_job_id": pjd_row_obj.id,
                                                     "job_status_id": start_obj.get_id(),
                                                     "start_date_time": datetime.datetime.utcnow(),
                                                     "is_active": True,
                                                     "data_transformations_deleted": False,
                                                     "data_transformations_archived": False
                                                     }

                self.job_obj.update_struct(self.job_id, {"job_status_id": start_obj.get_id()})

                pipeline_job_data_transformation_step_id = \
                    pipeline_job_data_trans_obj.insert_struct(pipeline_job_data_trans_step_dict)

                # Run methods registered for data step class

                parameters = data_transform_step.parameters
                dt_step_class_item = data_transformation_step_class_obj.find_by_id(data_transform_step.data_transformation_step_class_id)

                data_step_class_name = dt_step_class_item.name

                print("Running step %s: '%s'" % (data_transform_step.step_number, data_transform_step.name))

                data_step_class = self.data_trans_step_classes_obj.get_by_class_name(data_step_class_name)
                data_step_class_obj = data_step_class(**parameters) # Call with parameters from function
                data_step_class_obj.set_connection_and_meta_data(self.connection, self.meta_data)  # Set DB connection, metadata, and transaction

                data_step_class_obj.set_external_db_data_connections(self.external_data_connections_dict)

                data_step_class_obj.set_pipeline_job_data_transformation_id(pipeline_job_data_transformation_step_id)
                data_step_class_obj.set_file_directory(self.file_directory)

                data_step_class_obj.run()

                # Update job information associated with completion

                pipeline_job_data_trans_obj.update_struct(pipeline_job_data_transformation_step_id,
                                                          {"end_date_time": datetime.datetime.utcnow(),
                                                           "job_status_id":  finished_obj.get_id(),
                                                           "is_active": False})

            pipeline_job_obj.update_struct(pjd_row_obj.id, {"end_date_time": datetime.datetime.utcnow(),
                                                            "job_status_id":  finished_obj.get_id(),
                                                            "is_active": False})

        self.job_obj.update_struct(self.job_id, {"end_date_time": datetime.datetime.utcnow(),
                                                 "job_status_id":  finished_obj.get_id(),
                                                  "is_active": False,
                                                  "is_latest": True})


class ArchivePipeline(object):
    """Archives and deletes unwanted data transformations"""

    def __init__(self, pipeline_name, connection, meta_data):

        self.pipeline_name = pipeline_name
        self.connection = connection
        self.meta_data = meta_data

        self.pipeline_obj = Pipeline(pipeline_name, self.connection, self.meta_data)

    def _get_associated_pipeline_jobs(self):
        schema = self.meta_data.schema
        query_string = """select pj.id as pipeline_job_id, pj.job_id 
  from %s.pipeline_jobs pj 
  join %s.jobs j on pj.job_id = j.id
  join %s.pipelines p on pj.pipeline_id  = p.id
  where p.id = %s""" % (schema, schema, schema, self.pipeline_obj.get_id())

        return list(self.connection.execute(query_string))

    def _get_associated_pipeline_data_steps(self):

        pipeline_jobs_list = self._get_associated_pipeline_jobs()
        pipeline_jobs_dict = {}

        schema = self.meta_data.schema
        for pipeline_job_item in pipeline_jobs_list:
            dict_key = (pipeline_job_item.job_id, pipeline_job_item.pipeline_job_id)

            query_string = """select pjdt.data_transformation_step_id, dts.step_number,
    pjdt.id as pipeline_job_data_transformation_step_id
  from %s.pipeline_jobs_data_transformation_steps pjdt 
    join %s.job_statuses js on pjdt.job_status_id = js.id
    join %s.data_transformation_steps dts on dts.id = pjdt.data_transformation_step_id
      where js.name = 'Finished' and pjdt.pipeline_job_id = %s and (data_transformations_archived = FALSE and data_transformations_deleted = FALSE)
            """ % (schema, schema, schema, pipeline_job_item.job_id)

            cursor = self.connection.execute(query_string)
            result_list = [(c.pipeline_job_data_transformation_step_id, c.data_transformation_step_id, c.step_number) for c in cursor]

            pipeline_jobs_dict[dict_key] = result_list

        return pipeline_jobs_dict

    def archive_steps(self, steps=None):

        pipeline_job_dict = self._get_associated_pipeline_data_steps()
        schema = self.meta_data.schema

        for job_tuple in pipeline_job_dict:
            #job_id, pipeline_job_id = job_tuple

            for dts in pipeline_job_dict[job_tuple]:
                pipeline_job_data_transformation_step_id, data_transformation_id, step_number = dts

                insert_query_string = """insert into %s.archived_data_transformations select dts.*, cast(now() as timestamp) at time zone 'utc' from %s.data_transformations dts 
                                                        where dts.pipeline_job_data_transformation_step_id = %s""" % (
                schema, schema, pipeline_job_data_transformation_step_id)

                step_archived = False
                if steps is None:
                    self.connection.execute(insert_query_string)
                    step_archived = True

                elif step_number in steps:
                    self.connection.execute(insert_query_string)
                    step_archived = True

                delete_query_string = """
                delete from %s.data_transformations where pipeline_job_data_transformation_step_id = %s 
                """ % (schema, pipeline_job_data_transformation_step_id)

                self.connection.execute(delete_query_string)

                pjdts_obj = PipelineJobDataTranformationStep(self.connection, self.meta_data)
                pjdts_obj_id = pjdts_obj.find_by_id(pipeline_job_data_transformation_step_id)

                pjdts_obj.update_struct(pjdts_obj_id.id, {"data_transformations_deleted": True, "data_transformations_archived": step_archived})