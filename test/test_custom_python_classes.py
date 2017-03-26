import unittest
import pipeline
import schema_define
import json
import sqlalchemy as sa
import os
import sys


class TestUpdateWithCustomClasses(unittest.TestCase):

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

        with open("./test_pipeline_build_custom.json") as f:
            pipeline_structure = json.load(f)

        pipeline_name = "test custom pipeline"
        sys.path.insert(0, self.config["local_pipeline_import_path"][pipeline_name])

        pipeline_obj = pipeline.Pipeline(pipeline_name, self.connection, self.meta_data)
        pipeline_obj.load_steps_into_db(pipeline_structure)

        jobs_obj = pipeline.Jobs("Test custom job", self.connection, self.meta_data)
        jobs_obj.create_jobs_to_run("test custom pipeline")

        jobs_obj.run_job()


if __name__ == '__main__':
    unittest.main()
