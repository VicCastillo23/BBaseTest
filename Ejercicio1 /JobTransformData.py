#       I decided to use glue because it's easier to work with data and transform it, also it's cheaper and the most simplest way to load data. 
#       A. Para los ids nulos: ¿qué sugieres hacer con ellos?
#          R: Los id nulos habitualmente se refieren a data que no podria ser pocesada. en mi opinion depende de lo que quiera ver el cliente. haria 2 cosas 
#             1) poner una restricción para los el ID nulo y eliminarlo. 
#             2) Almacenarlo temporalmente en una queue y al final del proceso agregarles un id para conservar la data. 
#       B.Considerando las columnas name y company_id: ¿qué inconsistencias notas y como las mitigas?
#           R: Creando una regla para hacer match entre el name y el company_id. de esta forma no volverian a existir esas inconsistencias. 
#       C. Para el resto de los campos: ¿encuentras valores atípicos y, de ser así,cómo procedes?
#           R: para la ingesta de datos se pueden crear validaciones de campos en los que pueden lograrse validaciones de sintaxis o semantica. de esta forma se mitigan los valores atipicos.   

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsglue import DynamicFrame

#this method is used to find all null fields, uses a context, a schema, a path to find them. 
def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(
                ctx,
                field.dataType,
                new_path + field.name,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(
                ctx,
                schema.elementType,
                path,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split(".")[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set(
                    [
                        item.strip() if isinstance(item, str) else item
                        for item in distinct_
                    ]
                )
            elif isinstance(distinct_, str):
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif (
            isinstance(schema, IntegerType)
            or isinstance(schema, LongType)
            or isinstance(schema, DoubleType)
        ):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output

#This Method is used to drop the null fields. 
def drop_nulls(
    glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx
) -> DynamicFrame:
    nullColumns = _find_null_fields(
        frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame
    )
    return DropFields.apply(
        frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# this method is used to connect to a database using a S3 Bucket. 
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="vicentedata",
    table_name="vicentetabla",
    transformation_ctx="S3bucket_node1",
)

# this method is used to mapping the data from CSC into a glue table to transform process and edit the data.   
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "string", "id", "string"),
        ("name", "string", "name", "string"),
        ("company_id", "string", "company_id", "string"),
        ("amount", "decimal", "amount", "decimal"),
        ("status", "string", "status", "string"),
        ("created_at", "date", "created_at", "date"),
        ("paid_at", "date", "paid_at", "date"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# this method is used to drop the null fields 
DropNullFields_node1654903310630 = drop_nulls(
    glueContext,
    frame=ApplyMapping_node2,
    nullStringSet={"", "null"},
    nullIntegerSet={-1},
    transformation_ctx="DropNullFields_node1654903310630",
)

# this method is used to connect to a AWS S3 service. 
AmazonS3_node1654903657805 = glueContext.write_dynamic_frame.from_options(
    frame=DropNullFields_node1654903310630,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://anoc001-test-data-vicente/processed/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1654903657805",
)

job.commit()


#Also i used ExpanDrive to download the entire folder AWSLogs  you'll see in the Ejercicio1 it's easier and is cheap. 