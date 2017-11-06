import os
import json
import argparse
import sqlalchemy as sa


def get_db_connection(config_dict, reflect_db=True):
    """Connect to the PostgreSQL database"""
    engine = sa.create_engine(config_dict["connection_uri"])
    connection = engine.connect()
    meta_data = sa.MetaData(connection, schema=config_dict["db_schema"], reflect=reflect_db)

    return connection, meta_data


def find_last_pipeline_job(pipeline_name, connection, meta_data):
    """Connect"""
    schema = meta_data.schema
    query = """select j.id from %s.pipeline_jobs pj 
    join %s.pipelines p on pj.pipeline_id = p.id
    join %s.jobs j on j.id = pj.job_id 
    where p.name = :pipeline
    order by job_id desc limit 1;
    """ % (schema, schema, schema)

    cursor = connection.execute(sa.text(query), pipeline=pipeline_name)

    result_set = list(cursor)

    if len(result_set):
        return result_set[0].id
    else:
        return None


def find_last_step(pipeline_name, connection, meta_data):
    schema = meta_data.schema
    query = """ select step_number from %s.pipelines p 
  join %s.data_transformation_steps dts ON dts.pipeline_id = p.id 
  where p.name = :pipeline order by step_number desc limit 1
    """ % (schema, schema)

    cursor = connection.execute(sa.text(query), pipeline=pipeline_name)

    result_set = list(cursor)

    if len(result_set):
        return result_set[0].step_number
    else:
        return None


def main(pipeline_name, step_number, job_id, mongodb_import_format, config_json_file_name, directory):

    with open(config_json_file_name, "r") as f:
        config_dict = json.load(f)

    connection, meta_data = get_db_connection(config_dict)

    if job_id is None:
        job_id = find_last_pipeline_job(pipeline_name, connection, meta_data)

    if step_number is None:
        step_number = find_last_step(pipeline_name, connection, meta_data)

    schema = meta_data.schema

    query = """
     select dt.*, j.name as job_name, j.id as job_id, p.name as pipeline_name,
  dts.name as data_step_name, dts.step_number
  from %s.jobs j 
  join %s.pipeline_jobs pj on pj.job_id = j.id
  join %s.pipeline_jobs_data_transformation_steps pjdts ON pjdts.pipeline_job_id = pj.id
  join %s.data_transformation_steps dts ON dts.id = pjdts.data_transformation_step_id
  join %s.pipelines p on p.id = pj.pipeline_id
  join %s.data_transformations dt on dt.pipeline_job_data_transformation_step_id = pjdts.id
  where p.name = :pipeline and dts.step_number = :step_number and j.id = :job_id
    """ % (schema, schema, schema, schema, schema, schema)

    cursor = connection.execute(sa.text(query), pipeline=pipeline_name, step_number=step_number, job_id=job_id)

    file_name_to_export = os.path.join(directory, pipeline_name + "__" + str(step_number) + "__" + str(job_id) + ".json")

    if mongodb_import_format: # each line is a serialized JSON object
        with open(file_name_to_export, "w") as fw:
            i = 0
            for row in cursor:
                fw.write(json.dumps(row.data) + "\n")
                i += 1
    else:  # pretty printed JSON export
        export_list = []
        i = 0
        for row in cursor:
            export_list += [row.data]
            i += 1

        with open(file_name_to_export, "w") as fw:
            json.dump(export_list, fw,  sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description='Utility for exporting in pretty JSON or one line per record JSON for MongoDB')

    arg_parse_obj.add_argument("-c", "--config-json-filename", dest="config_json_filename",
                               help="JSON configuration file: see 'config.json.example'", default="./config.json")

    arg_parse_obj.add_argument("-n", "--pipeline-name", dest="pipeline_name", help="Set name of the pipeline")

    arg_parse_obj.add_argument("-j", "--job-id", dest="job_id", help="Job id to export",  default=None)

    arg_parse_obj.add_argument("-s", "--step-number", dest="step_number", help="Step number to export", default=None)

    arg_parse_obj.add_argument("-m", "--mongodb-import-format", action="store_true", default=False,
                               dest="mongodb_import_format", help="Store JSON for each record on one line")

    arg_parse_obj.add_argument("-d", "--directory", dest="directory", default=os.path.curdir)

    arg_obj = arg_parse_obj.parse_args()

    main(arg_obj.pipeline_name, arg_obj.step_number, arg_obj.job_id, arg_obj.mongodb_import_format,
         arg_obj.config_json_filename, arg_obj.directory)