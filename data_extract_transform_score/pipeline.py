import sqlalchemy as sa


class Pipeline(object):
    """Class and methods for inserting a pipeline into the database for processing"""

    def __init__(self, pipeline_name, connection, meta_data):
        self.pipeline_name = pipeline_name
        self.connection = connection
        self.meta_data = meta_data
        self.schema = meta_data.schema
        self.table_name = "pipelines"

        if self.schema is not None:
            self.table_name_with_schema = self.schema + "." + self.table_name
        else:
            self.table_name_with_schema = self.table_name

        self.table_obj = self.meta_data.tables[self.table_name_with_schema]

        self.pipeline_obj = self._find_by_name(self.pipeline_name)
        if self.pipeline_obj is None:
            self._insert_name(self.pipeline_name)
            self.pipeline_obj = self._find_by_name(self.pipeline_name)

    def load_steps_into_db(self, pipeline_structure):
        self.pipeline_structure = pipeline_structure

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


