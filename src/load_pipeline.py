import json
import sys


class LoadPipeline(object):
    """Class and methods for inserting a pipeline into the database for processing"""
    def __init__(self, connection, meta_data, pipeline_structure):

        self.connection = connection
        self.meta_data = meta_data
        self.pipeline_structure = pipeline_structure


def main(config_json_file_name, pipeline_json_file_name):

    with open(config_json_file_name, "r") as f:
          config = json.load(f)

    with open(pipeline_json_file_name, "r") as f:
          pipeline_structure = json.load(f)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Usage: python load_pipeline.py config.json pipeline.json")
    else:
        main(sys.argv[1], sys.argv[2])