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
                                                    ForeignKey("data_transformation_step_classes.id"), nullable=True)
                                             )

    data_transformation_steps = Table("data_transformation_steps", meta,
                                      Column("id", Integer, primary_key=True),
                                      Column("step_number", Integer),
                                      Column("name", String(255)),
                                      Column("data_transformation_step_class_id",
                                             ForeignKey("data_transformation_step_classes.id")),
                                      Column("parameters", JSONB),
                                      Column("description", Text))

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


def create_and_populate_schema(meta, connection):
    meta = schema_define(meta)
    meta.drop_all()
    meta.create_all(checkfirst=True)

    table_dict = get_table_names_without_schema(meta)
    job_statuses = [(1, "Started"), (2, "Finished"), (3, "Not started")]
    populate_reference_table(table_dict["job_statuses"], meta, connection, job_statuses)

    return meta, table_dict


def main():
    with open("./config.json", "r") as f:
        config = json.load(f)

    connection_uri = config["connection_uri"]
    if "db_schema" in config:
        db_schema = config["db_schema"]
    else:
        db_schema = None

    print(db_schema)

    engine = create_engine(connection_uri)
    connection = engine.connect()
    meta_data = MetaData(connection, schema=db_schema)

    meta_data, table_dict = create_and_populate_schema(meta_data, connection)

    print(meta_data.tables.keys())



if __name__ == "__main__":
    main()