"""
Classes for creating and running pipelines against a Postgresql database schema defined
in schema_define.py
"""

import data_transformations as dt
import time
import datetime

class DBClass(object):
    """Base Class for a PostgreSQL table in a schema"""
    def __init__(self, connection, meta_data):
        self.connection = connection
        self.meta_data = meta_data
        self.schema = meta_data.schema
        self.table_name = self._table_name()

        if self.schema is not None:
            self.table_name_with_schema = self.schema + "." + self.table_name
        else:
            self.table_name_with_schema = self.table_name

        self.table_obj = self.meta_data.tables[self.table_name_with_schema]

    def _table_name(self):
        return ""

    def insert_struct(self, data_struct):
        return self.connection.execute(self.table_obj.insert(data_struct).returning(self.table_obj.c.id)).fetchone()[0]

    def update_struct(self, row_id, update_dict):
        sql_expr = self.table_obj.update().where(self.table_obj.c.id == row_id).values(update_dict)
        self.connection.execute(sql_expr)

    def find_by_id(self, row_id):
        sql_expr = self.table_obj.select().where(self.table_obj.c.id == row_id)
        connection = self.connection.execute(sql_expr)
        return list(connection)[0]

class DataTransformationStep(DBClass):
    def _table_name(self):
        return "data_transformation_steps"

    def find_by_pipeline_id(self, pipeline_id):
        sql_expr = self.table_obj.select().where(self.table_obj.c.pipeline_id == pipeline_id)
        cursor = self.connection.execute(sql_expr)
        return list(cursor)


class DBClassName(DBClass):
    """Base class for working with table name"""

    def __init__(self, name, connection, meta_data, create_if_does_not_exists=True):

        self.connection = connection
        self.meta_data = meta_data
        self.schema = meta_data.schema
        self.table_name = self._table_name()
        self.name = name

        if self.schema is not None:
            self.table_name_with_schema = self.schema + "." + self.table_name
        else:
            self.table_name_with_schema = self.table_name

        self.table_obj = self.meta_data.tables[self.table_name_with_schema]

        self.name_obj = self._find_by_name(self.name)
        if self.name_obj is None and create_if_does_not_exists:
            self._insert_name(self.name)
            self.name_obj = self._find_by_name(self.name)

    def get_id(self):
        return self.name_obj.id

    def _find_by_name(self, name):
        find_expr = self.table_obj.select().where(self.table_obj.columns["name"] == name)
        cursor = self.connection.execute(find_expr)
        cursor_result = list(cursor)
        if len(cursor_result):
            return cursor_result[0]
        else:
            return None

    def _insert_name(self, name):
        self.connection.execute(self.table_obj.insert({"name": name}))


class DataTransformationStepClassDB(DBClass):
    def _table_name(self):
        return "data_transformation_step_classes"


class DataTransformationStepClass(DBClassName):
    """Class Representing the classes for data transformations"""
    def _table_name(self):
        return "data_transformation_step_classes"

    def get_datasteps_by_pipeline_id(self, pipeline_id):
        sql_expr = self.table_obj.select.where(self.table_obj.c.pipeline_id == pipeline_id)
        cursor = self.connection.execute(sql_expr)
        return list(cursor)


class DataTransformationStepClasses(object):
    """The data translation step class name is registered with a class"""

    def __init__(self):
        self.step_class_callable_obj_dict = {}

        self._register("Load file", dt.ReadFileIntoDB)

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

    def _table_name(self):
        return "pipelines"


class Job(DBClass):
    def _table_name(self):
        return "jobs"


class JobStatus(DBClassName):
    def _table_name(self):
        return "job_statuses"


class JobClass(DBClassName):
    def _table_name(self):
        return "job_classes"


class PipelineJob(DBClass):
    def _table_name(self):
        return "pipeline_jobs"
    def find_by_job_id_and_pipeline_id(self, job_id, pipeline_id):
        sql_expr = self.table_obj.select().where(self.table_obj.c.pipeline_id == pipeline_id
                                                 and self.table_obj.c.job_id == job_id)

        cursor = self.connection.execute(sql_expr)
        return list(cursor)[0]

class PipelineJobDataTranformationStep(DBClass):
    def _table_name(self):
        return "pipeline_jobs_data_transformation_steps"


class Jobs(object):
    """Class for running and executing jobs"""

    def __init__(self, name, connection, meta_data):
        self.connection = connection
        self.meta_data = meta_data
        self.job_id = None
        self.job_obj =None
        self.pipelines = []
        self.pipeline_jobs_ids = []
        self.name = name

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
                    "is_active": True
                    }

        self.job_id = self.job_obj.insert_struct(job_dict)

        pipeline_job_obj = PipelineJob(self.connection, self.meta_data)
        for pipeline in self.pipelines:
            pipeline_obj = Pipeline(pipeline, self.connection, self.meta_data)
            pipeline_id = pipeline_obj.get_id()

            pipeline_obj_dict = {"job_id": self.job_id, "pipeline_id": pipeline_id, "job_status_id": not_start_obj.get_id(),
                                 "start_date_time": datetime.datetime.utcnow(), "is_active": True}

            pipeline_job_obj.insert_struct(pipeline_obj_dict)

    def run_job(self):
        """Executes the job"""

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
                                                     "is_active": True}

                self.job_obj.update_struct(self.job_id, {"job_status_id": start_obj.get_id()})

                pipeline_job_data_transformation_step_id = pipeline_job_data_trans_obj.insert_struct(pipeline_job_data_trans_step_dict)

                # Run method registered for data step class

                parameters = data_transform_step.parameters
                dt_step_class_item = data_transformation_step_class_obj.find_by_id(data_transform_step.data_transformation_step_class_id)

                data_step_class_name = dt_step_class_item.name

                data_step_class = self.data_trans_step_classes_obj.get_by_class_name(data_step_class_name)
                data_step_class_obj = data_step_class(**parameters) # Call with parameters from function
                data_step_class_obj._set_connection_and_meta_data(self.connection, self.meta_data) # Set DB connection

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
                                                  "is_latest": True
                                                 })