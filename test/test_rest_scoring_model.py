import unittest
import pipeline
import schema_define
import json
import sqlalchemy as sa
import os
import sys


class TestRestModel(unittest.TestCase):

    def setUp(self):
        with open("testing_config.json", "r") as f:
            config = json.load(f)

            self.engine = sa.create_engine(config["connection_uri"])
            self.connection = self.engine.connect()
            self.meta_data = sa.MetaData(self.connection, schema=config["db_schema"])

        schema_define.create_and_populate_schema(self.connection, self.meta_data)

        self.config = config

        if os.path.exists("./test_output.json"):
            os.remove("./test_output.json")

    def test_create_and_run_custom_job(self):

        with open("./test_pipeline_build_rest.json") as f:
            pipeline_structure = json.load(f)

        pipeline_name = "test rest scoring pipeline"

        pipeline_obj = pipeline.Pipeline(pipeline_name, self.connection, self.meta_data)
        pipeline_obj.load_steps_into_db(pipeline_structure)

        jobs_obj = pipeline.Jobs("Test rest scoring job", self.connection, self.meta_data)
        jobs_obj.create_jobs_to_run("test rest scoring pipeline")

        jobs_obj.run_job()


if __name__ == '__main__':
    unittest.main()
