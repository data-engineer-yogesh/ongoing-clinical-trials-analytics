# from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    from_json,
    explode,
    current_timestamp,
)
import dlt
from pyspark.sql.types import *

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.
catalog_name = spark.conf.get('catalog_name')
volume_path = f"/Volumes/{catalog_name}/bronze/clinical_trials_dev_data/"

primary_key = "nctId"
from pyspark.sql.types import *

properties_schema = StructType(
    [
        StructField("hasResults", BooleanType()),
        StructField(
            "protocolSection",
            StructType(
                [
                    StructField(
                        "identificationModule",
                        StructType(
                            [
                                StructField("nctId", StringType()),
                                StructField("briefTitle", StringType()),
                                StructField("officialTitle", StringType()),
                                StructField(
                                    "organization",
                                    StructType(
                                        [
                                            StructField("fullName", StringType()),
                                            StructField("class", StringType()),
                                        ]
                                    ),
                                ),
                            ]
                        ),
                    ),
                    StructField(
                        "statusModule",
                        StructType(
                            [
                                StructField("overallStatus", StringType()),
                                StructField(
                                    "startDateStruct",
                                    StructType(
                                        [
                                            StructField("date", StringType()),
                                            StructField("type", StringType()),
                                        ]
                                    ),
                                ),
                                StructField(
                                    "completionDateStruct",
                                    StructType(
                                        [
                                            StructField("date", StringType()),
                                            StructField("type", StringType()),
                                        ]
                                    ),
                                ),
                            ]
                        ),
                    ),
                    StructField(
                        "conditionsModule",
                        StructType(
                            [
                                StructField("conditions", ArrayType(StringType())),
                                StructField("keywords", ArrayType(StringType())),
                            ],
                        ),
                    ),
                    StructField(
                        "descriptionModule",
                        StructType(
                            [
                                StructField("briefSummary", StringType()),
                                StructField("detailedDescription", StringType()),
                            ]
                        ),
                    ),
                    StructField(
                        "designModule",
                        StructType(
                            [
                                StructField("studyType", StringType()),
                                StructField("phases", ArrayType(StringType())),
                                StructField(
                                    "designInfo",
                                    StructType(
                                        [
                                            StructField("allocation", StringType()),
                                            StructField(
                                                "interventionModel", StringType()
                                            ),
                                            StructField(
                                                "interventionModelDescription",
                                                StringType(),
                                            ),
                                            StructField("primaryPurpose", StringType()),
                                        ]
                                    ),
                                ),
                                StructField(
                                    "enrollmentInfo",
                                    StructType(
                                        [
                                            StructField("count", IntegerType()),
                                            StructField("type", StringType()),
                                        ]
                                    ),
                                ),
                            ]
                        ),
                    ),
                ]
            ),
        ),
    ]
)

schema = ArrayType(properties_schema)


# clinical_trials_dev
@dlt.table(name="clinical_trials_dev_vw")
def clinical_trials_dev():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
        .withColumn("_load_timestamp", current_timestamp())
    )

    df = df.withColumn("parsed_data", from_json(col("studies"), schema))
    df = df.select(explode(col("parsed_data")).alias("studies"), "_load_timestamp")
    df = df.select(
        "studies.protocolSection.*",
        "studies.protocolSection.identificationModule.nctId",
        "_load_timestamp",
    )
    return df


dlt.create_streaming_table(name="clinical_trials_dev")

dlt.apply_changes(
    target="clinical_trials_dev",
    source="clinical_trials_dev_vw",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1",
)


# clinical_identificationModule_dev
@dlt.table(name="clinical_identificationModule_dev_vw")
def clinical_identificationModule_dev():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
        .withColumn("_load_timestamp", current_timestamp())
    )

    df = df.withColumn("parsed_data", from_json(col("studies"), schema))
    df = df.select(explode(col("parsed_data")).alias("studies"), "_load_timestamp")
    df = df.select(
        "studies.protocolSection.identificationModule.nctId",
        col("studies.protocolSection.identificationModule.briefTitle").alias(
            "briefTitle"
        ),
        col("studies.protocolSection.identificationModule.officialTitle").alias(
            "officialTitle"
        ),
        col("studies.protocolSection.identificationModule.organization.fullName").alias(
            "fullName"
        ),
        col("studies.protocolSection.identificationModule.organization.class").alias(
            "class"
        ),
        "_load_timestamp",
    )
    return df


dlt.create_streaming_table(name="clinical_identificationModule_dev")

dlt.apply_changes(
    target="clinical_identificationModule_dev",
    source="clinical_identificationModule_dev_vw",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1",
)


# clinical_status_dev
@dlt.table(name="clinical_status_dev_vw")
def clinical_status_dev():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
        .withColumn("_load_timestamp", current_timestamp())
    )
    df = df.withColumn("parsed_data", from_json(col("studies"), schema))
    df = df.select(explode(col("parsed_data")).alias("studies"), "_load_timestamp")
    df = df.select(
        "studies.protocolSection.identificationModule.nctId",
        col("studies.protocolSection.statusModule.startDateStruct.date").alias(
            "startDateStructdate"
        ),
        col("studies.protocolSection.statusModule.startDateStruct.type").alias(
            "startDateStructtype"
        ),
        col("studies.protocolSection.statusModule.completionDateStruct.date").alias(
            "completionDateStructdate"
        ),
        col("studies.protocolSection.statusModule.completionDateStruct.type").alias(
            "completionDateStructtype"
        ),
        "_load_timestamp",
    )
    return df


dlt.create_streaming_table(name="clinical_status_dev")

dlt.apply_changes(
    target="clinical_status_dev",
    source="clinical_status_dev_vw",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1",
)


# clinical_conditionsModule_dev_vw
@dlt.table(name="clinical_conditionsModule_dev_vw")
def clinical_conditionsModule_dev():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
        .withColumn("_load_timestamp", current_timestamp())
    )

    df = df.withColumn("parsed_data", from_json(col("studies"), schema))
    df = df.select(explode(col("parsed_data")).alias("studies"), "_load_timestamp")
    df = df.select(
        "studies.protocolSection.identificationModule.nctId",
        col("studies.protocolSection.conditionsModule.conditions").alias("conditions"),
        col("studies.protocolSection.conditionsModule.keywords").alias("keywords"),
        col("studies.protocolSection.descriptionModule.briefSummary").alias(
            "officialTitle"
        ),
        col("studies.protocolSection.descriptionModule.detailedDescription").alias(
            "detailedDescription"
        ),
        "_load_timestamp",
    )
    return df


dlt.create_streaming_table(name="clinical_conditionsModule_dev")

dlt.apply_changes(
    target="clinical_conditionsModule_dev",
    source="clinical_conditionsModule_dev_vw",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1",
)

#clinical_designModule_dev_vw
@dlt.table(name="clinical_designModule_dev_vw")
def clinical_designModule_dev():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
        .withColumn("_load_timestamp", current_timestamp())
    )

    df = df.withColumn("parsed_data", from_json(col("studies"), schema))
    df = df.select(explode(col("parsed_data")).alias("studies"), "_load_timestamp")
    df = df.select(
        "studies.protocolSection.identificationModule.nctId",
        col("studies.protocolSection.designModule.studyType").alias("studyType"),
        col("studies.protocolSection.designModule.phases").alias("phases"),
        col("studies.protocolSection.designModule.enrollmentInfo.count").alias("count"),
        col("studies.protocolSection.designModule.enrollmentInfo.type").alias("type"),
        col("studies.protocolSection.designModule.designInfo.allocation").alias(
            "designInfo_allocation"
        ),
        col("studies.protocolSection.designModule.designInfo.interventionModel").alias(
            "designInfo_interventionModel"
        ),
        col("studies.protocolSection.designModule.designInfo.primaryPurpose").alias(
            "designInfo_primaryPurpose"
        ),
        "_load_timestamp",
    )
    return df


dlt.create_streaming_table(name="clinical_designModule_dev")

dlt.apply_changes(
    target="clinical_designModule_dev",
    source="clinical_designModule_dev_vw",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1",
)
