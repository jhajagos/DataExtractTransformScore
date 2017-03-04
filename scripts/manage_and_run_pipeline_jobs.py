#!/bin/python
import argparse
import data_extract_transform_score as dets

"""
Command line program for creating, managing, and running pipelines jobs.

"""


def main():
    arg_parse_obj = argparse.ArgumentParser(description='Create, manage, and run jobs')
    arg_parse_obj.add_argument("-c", "--config-json-filename", dest="config_json_filename", help="")
    arg_parse_obj.add_argument("-p", "--pipeline-json-filename", dest="pipeline_json_file_name", help="")
    arg_parse_obj.add_argument("-n", "--pipeline-name", dest="pipeline_json_file_name", help="Set name of the pipeline")
    arg_parse_obj.add_argument("-l", "--list-available-pipelines", dest="pipeline_json_file_name", help="")
    arg_parse_obj.add_argument("-s", "--print-pipeline-steps", dest="pipeline_json_file_name", help="")
    arg_parse_obj.add_argument("-i", "--initialize-database-schema", action="", help="")
    arg_parse_obj.add_argument("-r", "--run-pipeline", action="", help="")

    args_obj = arg_parse_obj.parse_args()


if __name__ == "__main__":
    main()