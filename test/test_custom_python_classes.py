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

    def test_and_run_multiple_pipeline_jobs(self):

        with open("./test_pipeline_build_custom.json") as f:
            pipeline_structure = json.load(f)

        pipeline_name = "test custom pipeline"
        sys.path.insert(0, self.config["local_pipeline_import_path"][pipeline_name])

        pipeline_obj = pipeline.Pipeline(pipeline_name, self.connection, self.meta_data)
        pipeline_obj.load_steps_into_db(pipeline_structure)

        jobs_obj1 = pipeline.Jobs("Test custom job", self.connection, self.meta_data)
        jobs_obj1.create_jobs_to_run("test custom pipeline")

        jobs_obj1.run_job()

        with open("test_output_custom.json", "r") as f:
            output1 = json.load(f)

        jobs_obj2 = pipeline.Jobs("Test custom job", self.connection, self.meta_data)
        jobs_obj2.create_jobs_to_run("test custom pipeline")

        jobs_obj2.run_job()

        with open("test_output_custom.json", "r") as f:
            output2 = json.load(f)

        self.assertEqual(len(output1), len(output2))

    def test_and_run_pipeline_with_load_from_db(self):

        sqlite_file_name = os.path.join(os.path.curdir, "files", "test.db3")

        if os.path.exists(sqlite_file_name):
            os.remove(sqlite_file_name)

        import sqlalchemy as sa

        engine = sa.create_engine("sqlite:///./files/test.db3")
        connection = engine.connect()

        query_string = """
                CREATE TABLE test_summary_file
                (eid INTEGER, person_id VARCHAR(255), month_of_birth VARCHAR(255), year_of_birth VARCHAR(255),
                admit_date DATEIME,discharge_date DATETIME,drg VARCHAR(255), "group" VARCHAR(255))
                ;
        INSERT INTO test_summary_file
        (eid,person_id,month_of_birth,year_of_birth,admit_date,discharge_date,drg, "group")
          VALUES
          (1000,'100x','09','1990','2014-01-01','2014-01-02','701','1')
          ;

        INSERT INTO test_summary_file
        (eid,person_id,month_of_birth,year_of_birth,admit_date,discharge_date,drg,"group") 
        VALUES  
          (2000, 'x200','10','1980', '2015-02-01', '2015-01-31','', 2)
          ;

        CREATE TABLE test_summary_dx_list (
          eid INTEGER, seq_id INTEGER, poa VARCHAR(255), code VARCHAR(255)
          );

        INSERT INTO test_summary_dx_list (eid,seq_id,poa,code) VALUES 
          (1000,1,'1','N10');

        INSERT INTO test_summary_dx_list (eid,seq_id,poa,code) VALUES   
          (1000,2,'1','E119');

        INSERT INTO test_summary_dx_list (eid,seq_id,poa,code) VALUES  
          (1000,3,'0','K219');

                """

        for statement in query_string.split(";"):
            connection.execute(statement)

        connection.close()

        with open("./test_pipeline_build_from_db.json") as f:
            pipeline_structure = json.load(f)

        pipeline_name = "test loading from db"
        sys.path.insert(0, self.config["local_pipeline_import_path"][pipeline_name])

        pipeline_obj = pipeline.Pipeline(pipeline_name, self.connection, self.meta_data)
        pipeline_obj.load_steps_into_db(pipeline_structure)

        jobs_obj = pipeline.Jobs("Test custom job", self.connection, self.meta_data,
                                 external_data_connections_dict=self.config["external_data_connections"])
        jobs_obj.create_jobs_to_run("test loading from db")

        jobs_obj.run_job()

        with open("test_output_custom.json", "r") as f:
            output = json.load(f)

        self.assertEqual(2, len(output))


if __name__ == '__main__':
    unittest.main()
