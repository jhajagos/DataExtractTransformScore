#!/bin/python
import argparse
import os
import json
import sqlalchemy as sa
import random
import sys
import time

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
    meta_data = sa.MetaData(connection, schema=config_dict["db_schema"])
    meta_data.reflect()

    return connection, meta_data


def print_pipelines(config_dict):
    connection, meta_data = get_db_connection(config_dict)
    cursor = connection.execute("select * from %s.pipelines" % meta_data.schema)
    list_of_pipelines = list(cursor)
    print('Pipelines in schema: "%s"' % (meta_data.schema, ))
    print([(r.id, r.name) for r in list_of_pipelines])


def initialize_database_schema(config_dict, drop_all_tables=False):
    connection, meta_data = get_db_connection(config_dict, reflect_db=False)

    meta_data, table_dict = create_and_populate_schema(connection, meta_data, drop_all=drop_all_tables)
    print("Initialized %s tables in schema '%s'" % (len(table_dict), meta_data.schema))


def print_pipeline_steps(pipeline_name, config_dict):
    connection, meta_data = get_db_connection(config_dict)
    pipeline_obj = Pipeline(pipeline_name, connection, meta_data)
    pipeline_id = pipeline_obj.get_id()

    print('Pipeline in schema: "%s"' % (meta_data.schema,))

    cursor = connection.execute(
        "select * from %s.data_transformation_steps where pipeline_id = %s" % (meta_data.schema, pipeline_id))
    print("Steps in %s" % pipeline_name)

    steps = [(r.step_number, r.name) for r in cursor]
    for step in steps:
        print("  " + str(step))


def archive_pipeline(pipeline_name, config_dict, step_numbers=None):

    if step_numbers is not None:
        step_list = step_numbers.split(",")
        step_list = [int(s) for s in step_list]
    else:
        step_list = None

    connection, meta_data = get_db_connection(config_dict)
    print('Pipeline in schema: "%s"' % (meta_data.schema,))
    print("Archiving '%s'" % pipeline_name)
    ap = dets.pipeline.ArchivePipeline(pipeline_name, connection, meta_data)
    ap.archive_steps(step_list)


def rename_pipeline(old_pipeline_name, new_pipeline_name, config_dict):
    connection, meta_data = get_db_connection(config_dict)
    pipeline_obj = Pipeline(old_pipeline_name, connection, meta_data)
    pipeline_obj.rename_pipeline(new_pipeline_name)


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
        raise()


def update_pipeline_json_file(pipeline_json_filename, pipeline_name, config_dict):
    time_stamp = time.strftime("%Y%m%d_%H%M%S")
    update_pipeline_name = pipeline_name + "_" + time_stamp

    print("Renaming old pipeline as '%s'" % update_pipeline_name)

    rename_pipeline(pipeline_name, update_pipeline_name, config_dict)
    load_pipeline_json_file(pipeline_json_filename, pipeline_name, config_dict)


def run_pipeline(pipeline_name, config_dict, with_transaction_rollback=False):
    connection, meta_data = get_db_connection(config_dict)

    if "root_file_path" in config_dict:
        root_file_path = config_dict["root_file_path"]
    else:
        root_file_path = "./"

    if "local_pipeline_import_path" in config_dict:
        if pipeline_name in config_dict["local_pipeline_import_path"]:
            sys.path.insert(0, config_dict["local_pipeline_import_path"][pipeline_name])

    job_name = "Job_" + str(random.randint(1, 10000))

    if "external_data_connections" in config_dict:
        external_data_connections = config_dict["external_data_connections"]
    else:
        external_data_connections = {}

    jobs_obj = Jobs(job_name, connection, meta_data, root_file_path, external_data_connections_dict=external_data_connections)
    jobs_obj.create_jobs_to_run(pipeline_name)

    jobs_obj.run_job(with_transaction_rollback)

    print("Ran job: '%s' against pipeline: '%s'" % (job_name, pipeline_name))


def main():
    arg_parse_obj = argparse.ArgumentParser(description='Load, manage, and run data extraction and scoring pipelines')
    arg_parse_obj.add_argument("-c", "--config-json-filename", dest="config_json_filename",
                               help="JSON configuration file: see 'config.json.example'", default="./config.json")

    arg_parse_obj.add_argument("-p", "--pipeline-json-filename", dest="pipeline_json_filename", help="")

    arg_parse_obj.add_argument("-n", "--pipeline-name", dest="pipeline_name", help="Set name of the pipeline")

    arg_parse_obj.add_argument("-l", "--list-pipelines", dest="list_pipelines",
                               action="store_true", default=False, help="List name of pipelines that are currently loaded")

    arg_parse_obj.add_argument("-s", "--list-pipeline-steps", dest="list_pipeline_steps", default=False,
                               action="store_true", help="")

    arg_parse_obj.add_argument("-i", "--initialize-database-schema", action="store_true", default=False,
                               dest="initialize_database_schema",
                               help="In an empty PostGreSQL schema initialize database.")

    arg_parse_obj.add_argument("-d", "--drop-all-tables", action="store_true", default=False,
                               dest="drop_all_tables",
                               help="Drop all tables in schema")

    arg_parse_obj.add_argument("-u", "--update-pipeline", action="store_true", default=False,
                                  dest="update_pipeline",
                                  help="Update an existing name pipeline"
                                  )

    arg_parse_obj.add_argument("-a", "--archive-pipeline", action="store_true", default=False,
                               dest="archive_pipeline")

    arg_parse_obj.add_argument("--pipeline-step-number", default=None, dest="pipeline_step_number")

    arg_parse_obj.add_argument("--debug-mode", action="store_true", dest="debug_mode", default=False,
                               help="Disables rollback of transactions")

    arg_parse_obj.add_argument("-r", "--run-pipeline", action="store_true", help="Run pipeline")

    arg_obj = arg_parse_obj.parse_args()

    config_json_filename = arg_obj.config_json_filename
    # Configuration file must exist
    if not os.path.exists(config_json_filename):
        raise(IOError("Configuration file: '%s' does not exist" % config_json_filename))

    with open(config_json_filename, "r") as f:
        config_dict = json.load(f)

    if arg_obj.list_pipelines:
        print_pipelines(config_dict)
        return True
    
    if arg_obj.initialize_database_schema:
        initialize_database_schema(config_dict, arg_obj.drop_all_tables)
        return True

    if arg_obj.list_pipeline_steps or arg_obj.run_pipeline or arg_obj.pipeline_json_filename or arg_obj.archive_pipeline:
        pipeline_name = arg_obj.pipeline_name
        if pipeline_name:
            if arg_obj.list_pipeline_steps:
                print_pipeline_steps(pipeline_name, config_dict)
                return True
            elif arg_obj.pipeline_json_filename:
                pipeline_json_filename = arg_obj.pipeline_json_filename
                if arg_obj.update_pipeline:
                    update_pipeline_json_file(pipeline_json_filename, pipeline_name, config_dict)
                else:
                    load_pipeline_json_file(pipeline_json_filename, pipeline_name, config_dict)
            elif arg_obj.archive_pipeline:
                archive_pipeline(pipeline_name,config_dict, step_numbers=arg_obj.pipeline_step_number)
            elif arg_obj.run_pipeline:
                run_pipeline(pipeline_name, config_dict, with_transaction_rollback=arg_obj.debug_mode)

        else:
            raise(RuntimeError, "Pipeline name must be provided")


if __name__ == "__main__":
    main()
