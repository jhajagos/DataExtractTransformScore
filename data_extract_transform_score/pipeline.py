"""
Classes for creating and running pipelines against a Postgresql database schema defined
in schema_define.py
"""

import data_transformations as dt


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
        self.connection.execute(self.table_obj.insert(data_struct))


class DataTransformationStep(DBClass):
    def _table_name(self):
        return "data_transformation_steps"


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


class DataTransformationStepClass(DBClassName):
    """Class Representing the classes for data transformations"""
    def _table_name(self):
        return "data_transformation_step_classes"


class DataTransformationStepClasses(object):
    """The data translation step class name is registered with a class"""

    def __init__(self):
        self.step_class_callable_obj_dict = {}

    def _register(self, data_transformation_step_class_name, class_obj):
        self.step_class_callable_obj_dict[data_transformation_step_class_name] = class_obj


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
            print(data_step_dict)
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


class Jobs(object):
    def __init__(self, connection, meta_data):
        self.connection = connection
        self.meta_data = meta_data
        self.job_id = None

    def create_jobs_to_run(self, pipelines):
        pass

    def run_job(self):
        """Executes the job"""
        pass

    def _update_job(self):
        pass


