import json
import sys
from pipeline import Pipeline


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