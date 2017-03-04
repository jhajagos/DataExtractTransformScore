#!/bin/python
import argparse
import os
import json
import data_extract_transform_score as dets

"""
Command line program for creating, managing, and running pipelines jobs.

"""


def list_available_pipelines(config_dict):
    pass


def main():
    arg_parse_obj = argparse.ArgumentParser(description='Create, manage, and run data extract and pipelines')
    arg_parse_obj.add_argument("-c", "--config-json-filename", dest="config_json_filename",
                               help="JSON configuration file: see 'config.json.example'", default="./config.json")
    arg_parse_obj.add_argument("-p", "--pipeline-json-filename", dest="pipeline_json_file_name", help="")
    arg_parse_obj.add_argument("-n", "--pipeline-name", dest="pipeline_json_file_name", help="Set name of the pipeline")
    arg_parse_obj.add_argument("-l", "--list-available-pipelines", dest="list_available pipelines",
                               action="store_true", default=False, help="List name of pipelines that are currently loaded")
    arg_parse_obj.add_argument("-s", "--print-pipeline-steps", dest="pipeline_json_file_name", default="false",
                               action="store_true", help="")
    arg_parse_obj.add_argument("-i", "--initialize-database-schema", action="store_true", default=False,
                               help="In an empty PostGreSQL schema initialize database")
    arg_parse_obj.add_argument("-r", "--run-pipeline", action="store_true", help="")

    arg_obj = arg_parse_obj.parse_args()

    config_json_filename = arg_obj.config_json_filename
    # Configuration file must exist
    if not os.path.exists(config_json_filename):
        raise IOError, "Configuration file: '%s' does not exist" % config_json_filename

    with open(config_json_filename, "r") as f:
        config_file_dict = json.load(config_json_filename)


    if arg_obj.list_available_pipelines():
        list_available_pipelines(config_file_dict)
        return 0







if __name__ == "__main__":
    main()