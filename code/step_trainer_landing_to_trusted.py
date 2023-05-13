import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated
CustomerCurated_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1",
)

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1683851953085 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://jlima-udacity/step_trainer/landing/"],
            "recurse": True,
        },
        transformation_ctx="StepTrainerLandingZone_node1683851953085",
    )
)

# Script generated for node Join
Join_node1683851895441 = Join.apply(
    frame1=CustomerCurated_node1,
    frame2=StepTrainerLandingZone_node1683851953085,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1683851895441",
)

# Script generated for node Drop Fields
DropFields_node1683852174001 = DropFields.apply(
    frame=Join_node1683851895441,
    paths=[
        "customername",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithfriendsasofdate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "birthday",
        "registrationdate",
        "serialnumber",
    ],
    transformation_ctx="DropFields_node1683852174001",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1683852174001,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://jlima-udacity/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
