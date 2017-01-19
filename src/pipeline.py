
class Pipeline(object):
    """Class and methods for inserting a pipeline into the database for processing"""
    def __init__(self, connection, meta_data):

        self.connection = connection
        self.meta_data = meta_data


    def load_into_db(self, pipeline_structure):
        self.pipeline_structure = pipeline_structure