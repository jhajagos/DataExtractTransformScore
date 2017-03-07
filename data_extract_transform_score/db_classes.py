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
        cursor = self.connection.execute(sql_expr)
        return list(cursor)[0]


class DataTransformationStep(DBClass):
    def _table_name(self):
        return "data_transformation_steps"

    def find_by_pipeline_id(self, pipeline_id):
        sql_expr = self.table_obj.select().where(self.table_obj.c.pipeline_id == pipeline_id)
        cursor = self.connection.execute(sql_expr)
        return list(cursor)


class DataTransformationDB(DBClass):
    def _table_name(self):
        return "data_transformations"


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

    def get_data_steps_by_pipeline_id(self, pipeline_id):
        sql_expr = self.table_obj.select.where(self.table_obj.c.pipeline_id == pipeline_id)
        cursor = self.connection.execute(sql_expr)
        return list(cursor)


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