import unittest
import load_pipeline
import schema_define
import json
import sqlalchemy as sa

class TestLoadPipeline(unittest.TestCase):
    def setUp(self):
        with open("testing_config.json", "r") as f:
            config = json.load(f)

            engine = sa.create_engine(config["connection_uri"])
            connection = engine.connect()
            meta_data = sa.MetaData(connection, schema=connection["db_schema"])

        schema_define.create_and_populate_schema(connection, meta_data=meta_data)


    def test_load_pipeline(self):
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
