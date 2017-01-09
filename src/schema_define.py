from sqlalchemy import Table, Column, Integer, Text, String, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB


def schema_define(meta):

    job_classes = Table("job_classes", meta,
                        Column("id", Integer, primary_key=True),
                        Column("name", String(255), nullable=False))

    job_statuses = Table("job_statuses", meta,
                         Column("id", Integer, primary_key=True),
                         Column("name", String(255), nullable=False))

    jobs = Table("jobs", meta,
                 Column("id", Integer, primary_key=True),
                 Column("name", String(255), nullable=False),
                 Column("start_date_time", DateTime),
                 Column("end_date_time", DateTime),
                 Column("job_class_id", ForeignKey("job_classes.id"), nullable=False),
                 Column("job_status_id", ForeignKey("job_statuses.id"), nullable=False))

    data_transformation_step_classes = Table("data_transformation_classes", meta,
                                              Column("id", Integer, primary_key=True),
                                              Column("name", String(255), nullable=False))

    data_transformation_steps = Table("data_transformation_steps", meta,
                                      Column("id", Integer, primary_key=True),
                                      Column("trna"),
                                      Column("data_transformation_step_class_id",
                                             ForeignKey("data_transformation_step_classes.id"), nullable=False))

    data_transformations = Table("data_transformations", meta,
                                 Column("id", Integer, primary_key=True),
                                 Column("data", JSONB),
                                 Column("meta", JSONB),
                                 Column("common_id", Integer),
                                 Column("data_transformation_step_id", ForeignKey("data_transformation_steps.id"),
                                        nullable=False))

    data_transformation_relationships = Table("data_transformation_relationships", meta,
                                              Column("id", Integer, primary_key=True),
                                              Column("parent_data_transformation_id",
                                                     ForeignKey("data_transformations.id")),
                                              Column("child_data_transformation_id",
                                                      ForeignKey("data_transformations.id"), nullabe=False))

    return meta


def create_and_populate_schema(meta):
    pass