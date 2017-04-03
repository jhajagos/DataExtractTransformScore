#!/bin/python
import argparse
import os
import json
import sqlalchemy as sa
import random
import sys

try:
    import data_extract_transform_score as dets
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir)))
    import data_extract_transform_score as dets

from data_extract_transform_score.schema_define import create_and_populate_schema
from data_extract_transform_score.pipeline import Pipeline, Jobs

"""
Command line program for creating, managing, and running pipelines jobs.

"""


def get_db_connection(config_dict, reflect_db=True):
    """Connect to the PostgreSQL database"""
    engine = sa.create_engine(config_dict["connection_uri"])
    connection = engine.connect()
    meta_data = sa.MetaData(connection, schema=config_dict["db_schema"], reflect=reflect_db)

    return connection, meta_data


def list_available_pipelines(config_dict):
    connection, meta_data = get_db_connection(config_dict)
    cursor = connection.execute("select * from %s.pipelines" % meta_data.schema)
    list_of_pipelines = list(cursor)
    print([(r.id, r.name) for r in list_of_pipelines])


def initialize_database_schema(config_dict, drop_all_tables=False):
    connection, meta_data = get_db_connection(config_dict, reflect_db=False)

    meta_data, table_dict = create_and_populate_schema(connection, meta_data, drop_all=drop_all_tables)
    print("Initialized %s tables in schema '%s'" % (len(table_dict), meta_data.schema))


def print_pipeline_steps(pipeline_name, config_dict):
    pass


def load_pipeline_json_file(pipeline_json_filename, pipeline_name, config_dict):

    connection, meta_data = get_db_connection(config_dict)

    trans = connection.begin()

    with open(pipeline_json_filename) as f:
        pipeline_struct = json.load(f)

    try:
        pipeline_obj = Pipeline(pipeline_name, connection, meta_data)
        pipeline_obj.load_steps_into_db(pipeline_struct)
        print("Loaded: '%s'" % pipeline_name)
        trans.commit()
    except:
        trans.rollback()
        raise


def run_pipeline(pipeline_name, config_dict, with_rollback=False):
    connection, meta_data = get_db_connection(config_dict)

    if "root_file_path" in config_dict:
        root_file_path = config_dict["root_file_path"]
    else:
        root_file_path = "./"

    if "local_pipeline_import_path" in config_dict:
        if pipeline_name in config_dict["local_pipeline_import_path"]:
            sys.path.insert(0, config_dict["local_pipeline_import_path"][pipeline_name])

    job_name = "Job_" + str(random.randint(1, 10000))

    trans = connection.begin()
    try:

        jobs_obj = Jobs(job_name, connection, meta_data, root_file_path)
        jobs_obj.create_jobs_to_run(pipeline_name)

        jobs_obj.run_job()

        trans.commit()
    except:
        if with_rollback:
            trans.rollback()
        else:
            trans.commit()

        raise

    print("Ran job: '%s' against pipeline: '%s'" % (job_name, pipeline_name))


def main():
    arg_parse_obj = argparse.ArgumentParser(description='Create, manage, and run data extract and pipelines')
    arg_parse_obj.add_argument("-c", "--config-json-filename", dest="config_json_filename",
                               help="JSON configuration file: see 'config.json.example'", default="./config.json")

    arg_parse_obj.add_argument("-p", "--pipeline-json-filename", dest="pipeline_json_filename", help="")

    arg_parse_obj.add_argument("-n", "--pipeline-name", dest="pipeline_name", help="Set name of the pipeline")

    arg_parse_obj.add_argument("-l", "--list-available-pipelines", dest="list_available_pipelines",
                               action="store_true", default=False, help="List name of pipelines that are currently loaded")

    arg_parse_obj.add_argument("-s", "--print-pipeline-steps", dest="print_pipeline_steps", default=False,
                               action="store_true", help="")

    arg_parse_obj.add_argument("-i", "--initialize-database-schema", action="store_true", default=False,
                               dest="initialize_database_schema",
                               help="In an empty PostGreSQL schema initialize database.")

    arg_parse_obj.add_argument("-d", "--drop-all-tables", action="store_true", default=False,
                               dest="drop_all_tables",
                               help="Drop all tables in schema")

    arg_parse_obj.add_argument("--debug-mode", action="store_true", dest="debug_mode", default=False,
                               help="Disables rollback of transactions")

    arg_parse_obj.add_argument("-r", "--run-pipeline", action="store_true", help="Run pipeline")

    arg_obj = arg_parse_obj.parse_args()

    config_json_filename = arg_obj.config_json_filename
    # Configuration file must exist
    if not os.path.exists(config_json_filename):
        raise IOError, "Configuration file: '%s' does not exist" % config_json_filename

    with open(config_json_filename, "r") as f:
        config_dict = json.load(f)

    if arg_obj.list_available_pipelines:
        list_available_pipelines(config_dict)
        return True
    
    if arg_obj.initialize_database_schema:
        initialize_database_schema(config_dict, arg_obj.drop_all_tables)
        return True

    if arg_obj.print_pipeline_steps or arg_obj.run_pipeline or arg_obj.pipeline_json_filename:
        pipeline_name = arg_obj.pipeline_name
        if pipeline_name:
            if arg_obj.print_pipeline_steps:
                print_pipeline_steps(pipeline_name, config_dict)
                return True
            elif arg_obj.pipeline_json_filename:
                load_pipeline_json_file(arg_obj.pipeline_json_filename, pipeline_name, config_dict)
            elif arg_obj.run_pipeline:
                run_pipeline(pipeline_name, config_dict, with_rollback=arg_obj.debug_mode)

        else:
            raise RuntimeError, "Pipeline name must be provided"

if __name__ == "__main__":
    main()
