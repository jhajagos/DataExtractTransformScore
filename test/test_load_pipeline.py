import unittest
import pipeline
import schema_define
import json
import sqlalchemy as sa
import os


class TestLoadPipeline(unittest.TestCase):
    def setUp(self):
        with open("testing_config.json", "r") as f:
            config = json.load(f)

            self.engine = sa.create_engine(config["connection_uri"])
            self.connection = self.engine.connect()
            self.meta_data = sa.MetaData(self.connection, schema=config["db_schema"])

        schema_define.create_and_populate_schema(self.meta_data, self.connection)

        if os.path.exists("./test_output.json"):
            os.remove("./test_output.json")

    def test_load_pipeline(self):

        with open("./test_pipeline_build.json") as f:
            pipeline_struct = json.load(f)

        pipeline_obj = pipeline.Pipeline("test pipeline", self.connection, self.meta_data)

        cursor = self.connection.execute("select * from testing.pipelines")
        list_of_pipelines = list(cursor)

        self.assertEquals(1, len(list_of_pipelines))

        pipeline_obj.load_steps_into_db(pipeline_struct)

        cursor = self.connection.execute("select * from testing.data_transformation_steps")
        list_of_data_trans_steps = list(cursor)

        self.assertTrue(len(list_of_data_trans_steps))

    def test_create_and_run_jobs(self):

        with open("./test_pipeline_build.json") as f:
            pipeline_structure = json.load(f)

        pipeline_obj = pipeline.Pipeline("test pipeline", self.connection, self.meta_data)
        pipeline_obj.load_steps_into_db(pipeline_structure)

        jobs_obj = pipeline.Jobs("Test job", self.connection, self.meta_data)
        jobs_obj.create_jobs_to_run("test pipeline")

        jobs_obj.run_job()

        with open("./test_output.json") as f:
            pipeline_results = json.load(f)

        self.assertEquals(2, len(pipeline_results))


    def test_create_and_run_multiple_jobs(self):

        with open("./test_pipeline_build.json") as f:
            pipeline_structure = json.load(f)

        pipeline_obj_1 = pipeline.Pipeline("test pipeline", self.connection, self.meta_data)
        pipeline_obj_1.load_steps_into_db(pipeline_structure)

        pipeline_obj_2 = pipeline.Pipeline("test second job pipeline", self.connection, self.meta_data)
        pipeline_obj_2.load_steps_into_db(pipeline_structure)

        jobs_obj_2 = pipeline.Jobs("Test job", self.connection, self.meta_data)
        jobs_obj_2.create_jobs_to_run("test second job pipeline")

        jobs_obj_2.run_job()

        with open("./test_output.json") as f:
            pipeline_results = json.load(f)

        self.assertEquals(2, len(pipeline_results))


if __name__ == '__main__':
    unittest.main()
