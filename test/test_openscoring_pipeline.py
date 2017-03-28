import unittest
import pipeline
import schema_define
import json
import sqlalchemy as sa
import os
import requests

class TestOpenScoringPipeline(unittest.TestCase):
    "Test using a model deployed to the OpenScoring REST API. See: https://github.com/openscoring/openscoring"
    def setUp(self):

        r = requests.put("http://localhost:8080/openscoring/model/test_logistic_regression_model",
                         data=open("./files/logistic_regression_model.pmml", "rb"), headers={"Content-type": "text/xml"})

        with open("testing_config.json", "r") as f:
            config = json.load(f)

            self.engine = sa.create_engine(config["connection_uri"])
            self.connection = self.engine.connect()
            self.meta_data = sa.MetaData(self.connection, schema=config["db_schema"])

        schema_define.create_and_populate_schema(self.connection, self.meta_data)

        self.config = config

        if os.path.exists("./test_output.json"):
            os.remove("./test_output.json")

    def test_open_scoring_pipeline(self):

        with open("./test_pipeline_build_openscoring.json") as f:
            pipeline_structure = json.load(f)

        pipeline_name = "test open_scoring"

        pipeline_obj = pipeline.Pipeline(pipeline_name, self.connection, self.meta_data)
        pipeline_obj.load_steps_into_db(pipeline_structure)

        jobs_obj = pipeline.Jobs("Test open_scoring job", self.connection, self.meta_data)
        jobs_obj.create_jobs_to_run("test open_scoring")

        jobs_obj.run_job()

        #TODO: Write test

        self.assertEqual(True, False)

if __name__ == '__main__':
    unittest.main()
