import unittest
import pipeline
import schema_define
import json
import sqlalchemy as sa


class TestLoadPipeline(unittest.TestCase):
    def setUp(self):
        with open("testing_config.json", "r") as f:
            config = json.load(f)

            self.engine = sa.create_engine(config["connection_uri"])
            self.connection = self.engine.connect()
            self.meta_data = sa.MetaData(self.connection, schema=config["db_schema"])

        schema_define.create_and_populate_schema(self.meta_data, self.connection)

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

    def test_create_job(self):

        with open("./test_pipeline_build.json") as f:
            pipeline_struct = json.load(f)

        pipeline_obj = pipeline.Pipeline("test pipeline", self.connection, self.meta_data)
        pipeline_obj.load_steps_into_db(pipeline_struct)

        jobs_obj = pipeline.Jobs(self.connection, self.meta_data)

        jobs_obj.create_jobs_to_run("test pipeline")


if __name__ == '__main__':
    unittest.main()
