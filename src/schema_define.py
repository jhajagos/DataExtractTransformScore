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
                                              Column("name", String(255), nullable=False, unique=True))

    data_transformation_steps = Table("data_transformation_steps", meta,
                                      Column("id", Integer, primary_key=True),
                                      Column("step_number", Integer),
                                      Column("name", String(255)),
                                      Column("parameters", JSONB),
                                      Column("description", Text),
                                      Column("data_transformation_step_class_id",
                                             ForeignKey("data_transformation_step_classes.id"), nullable=True))

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


def create_and_populate_schema(meta):
    meta = schema_define(meta)
    meta.drop_all()
    meta.create_all(checkfirst=True)
    return meta


def main():
    with open("./config.json", "r") as f:
        config = json.load(f)

    connection_uri = config["connection_uri"]
    if "db_schema" in connection_uri:
        db_schema = config["db_schema"]
    else:
        db_schema = None

    engine = create_engine(connection_uri)
    connection = engine.connect()
    meta_data = MetaData(connection, schema=db_schema)

    meta_data = create_and_populate_schema(meta_data)


if __name__ == "__main__":
    main()