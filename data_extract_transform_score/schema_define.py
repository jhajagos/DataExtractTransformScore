from sqlalchemy import Table, Column, Integer, Text, String, DateTime, ForeignKey, create_engine, MetaData
from sqlalchemy.dialects.postgresql import JSONB
import json


def schema_define(meta):

    job_classes = Table("job_classes", meta,
                        Column("id", Integer, primary_key=True),
                        Column("name", String(255), nullable=False, unique=True))

    job_statuses = Table("job_statuses", meta,
                         Column("id", Integer, primary_key=True),
                         Column("name", String(255), nullable=False, unique=True))

    jobs = Table("jobs", meta,
                 Column("id", Integer, primary_key=True),
                 Column("name", String(255), nullable=False),
                 Column("start_date_time", DateTime),
                 Column("end_date_time", DateTime),
                 Column("job_class_id", ForeignKey("job_classes.id"), nullable=False),
                 Column("job_status_id", ForeignKey("job_statuses.id"), nullable=False))

    data_transformation_step_classes = Table("data_transformation_step_classes", meta,
                                             Column("id", Integer, primary_key=True),
                                             Column("name", String(255), nullable=False, unique=True),
                                             Column("parent_data_transformation_step_class_id",
                                                    ForeignKey("data_transformation_step_classes.id"), nullable=True))

    pipelines = Table("pipelines", meta,
                      Column("id", Integer, primary_key=True),
                      Column("name", String(255), nullable=False, unique=True))

    data_transformation_steps = Table("data_transformation_steps", meta,
                                      Column("id", Integer, primary_key=True),
                                      Column("step_number", Integer),
                                      Column("name", String(255)),
                                      Column("data_transformation_step_class_id",
                                             ForeignKey("data_transformation_step_classes.id")),
                                      Column("parameters", JSONB),
                                      Column("description", Text),
                                      Column("pipeline_id", ForeignKey("pipelines.id"), nullable=False))

    data_transformation_step_jobs = Table("data_transformation_step_jobs", meta,
                                          Column("id", Integer, primary_key=True),
                                          Column("job_id", ForeignKey("jobs.id"), nullable=False),
                                          Column("data_transformation_step_id",
                                                 ForeignKey("data_transformation_steps.id"),
                                                 nullable=False),
                                          Column("job_status_id", ForeignKey("job_statuses.id"), nullable=False),
                                          Column("start_date_time", DateTime),
                                          Column("end_date_time", DateTime))

    data_transformation_step_relationships = Table("data_transformation_step_relationships", meta,
                                                   Column("id", Integer, primary_key=True),
                                                   Column("parent_data_transformation_step_id",
                                                          ForeignKey("data_transformation_steps.id"), nullable=False),
                                                   Column("child_data_transformation_step_id",
                                                          ForeignKey("data_transformation_steps.id"), nullable=False))

    data_transformations = Table("data_transformations", meta,
                                 Column("id", Integer, primary_key=True),
                                 Column("data", JSONB),
                                 Column("meta", JSONB),
                                 Column("common_id", Integer),
                                 Column("data_transformation_step_id", ForeignKey("data_transformation_steps.id"),
                                        nullable=False),
                                 Column("job_id", ForeignKey("jobs.id"), nullable=False))

    data_transformation_relationships = Table("data_transformation_relationships", meta,
                                              Column("id", Integer, primary_key=True),
                                              Column("parent_data_transformation_id",
                                                     ForeignKey("data_transformations.id")),
                                              Column("child_data_transformation_id",
                                                     ForeignKey("data_transformations.id"), nullable=False))
    return meta


def get_table_names_without_schema(meta):
    table_dict = {}
    for full_table_name in meta.tables:
        if meta.schema is not None:
            schema, table_name = full_table_name.split(".")
            table_dict[table_name] = full_table_name
        else:
            table_dict[full_table_name] = full_table_name
    return table_dict


def populate_reference_table(table_name, meta, connection, list_of_values):
    table_obj = meta.tables[table_name]

    for tuple_value in list_of_values:
        connection.execute(table_obj.insert(tuple_value))


def create_and_populate_schema(meta_data, connection):
    meta_data = schema_define(meta_data)
    meta_data.drop_all()
    meta_data.create_all(checkfirst=True)

    table_dict = get_table_names_without_schema(meta_data)
    job_statuses = [(1, "Started"), (2, "Finished"), (3, "Not started")]
    populate_reference_table(table_dict["job_statuses"], meta_data, connection, job_statuses)

    primary_data_transform_classes = [
                                      (1,"Load", None),
                                      (2, "Merge", None),
                                      (3, "Coalesce"),
                                      (4, "Transform", None),
                                      (5, "Score", None),
                                      (6, "Output", None)
                                     ]

    child_data_transform_child_classes_1 = [(10, "Load file", 1), (40, "Map JSON", 4), (50, "Custom class score", 5)]

    data_transform_classes = primary_data_transform_classes + child_data_transform_child_classes_1

    populate_reference_table(table_dict["data_transformation_step_classes"], meta_data, connection, data_transform_classes)

    return meta_data, table_dict


def main():
    with open("./config.json", "r") as f:
        config = json.load(f)

    connection_uri = config["connection_uri"]
    if "db_schema" in config:
        db_schema = config["db_schema"]
    else:
        db_schema = None

    engine = create_engine(connection_uri)
    connection = engine.connect()
    meta_data = MetaData(connection, schema=db_schema)

    meta_data, table_dict = create_and_populate_schema(meta_data, connection)

    print(meta_data.tables.keys())


if __name__ == "__main__":
    main()