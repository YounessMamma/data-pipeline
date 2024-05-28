from dagster import (op, Field, List, String, Int,  # type: ignore
                     DependencyDefinition, 
                     GraphDefinition, 
                     success_hook, 
                     In, Out)

from slack_sdk import *  # type: ignore
import boto3  # type: ignore
import json 
import pandas as pd  # type: ignore
import os 
from slack_sdk.web import WebClient # type: ignore
from ..utils import *

import json
import shutil
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError # type: ignore
from slack_sdk.web import WebClient
import tempfile
from evidently.report import Report # type: ignore
from evidently.test_suite import TestSuite # type: ignore
from evidently.tests import * # type: ignore
from evidently import ColumnMapping # type: ignore
from evidently.ui.workspace import Workspace # type: ignore
from evidently.ui.dashboards import CounterAgg # type: ignore
from evidently.ui.dashboards import DashboardPanelCounter
from evidently.ui.dashboards import DashboardPanelPlot
from evidently.ui.dashboards import PanelValue # type: ignore
from evidently.ui.dashboards import PlotType
from evidently.ui.dashboards import ReportFilter
from evidently.renderers.html_widgets import WidgetSize
from evidently.ui.dashboards import DashboardPanelTestSuite
from evidently.ui.dashboards import TestSuitePanelType
from evidently import metrics


@op
def setup_workspace(project_name):
    ws= Workspace.create(WORKSPACE)
    project = ws.create_project(project_name)
    project.description = PROJECT_DESCRIPTION

    # title only panel :
    project.dashboard.add_panel(
        DashboardPanelCounter(
            filter = ReportFilter(metadata_values={}, tag_values=["full"]),
            agg = CounterAgg.NONE,
            title = "income data and model monitoring"
        )
    )
    # number of rows counter
    project.dashboard.add_panel(
        DashboardPanelCounter(
            title = "current number of rows",
            filter = ReportFilter(metadata_values={},tag_values=["classification","full"]),
            value = PanelValue(
                metric_id = "DatasetSummaryMetric",
                field_path = metrics.DatasetSummaryMetric.fields.current.number_of_rows,
                legend = "count"
            ),
            text = "rows",
            agg = CounterAgg.LAST,
            size = WidgetSize.HALF
        )
    )
    project.dashboard.add_panel(
        DashboardPanelCounter(
            title = "reference number of rows",
            filter = ReportFilter(metadata_values={},tag_values=["classification","full"]),
            value = PanelValue(
                metric_id = "DatasetSummaryMetric",
                field_path = metrics.DatasetSummaryMetric.fields.reference.number_of_rows,
                legend = "count"
            ),
            text = "rows",
            agg = CounterAgg.LAST,
            size = WidgetSize.HALF
        )
    )
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title = "number of rows overtime",
            filter = ReportFilter(metadata_values={},tag_values=["classification","full"]),
            values = [PanelValue(
                metric_id = "DatasetSummaryMetric",
                field_path = metrics.DatasetSummaryMetric.fields.current.number_of_rows,
                legend = "count"
            )],
            plot_type = PlotType.LINE,
            size = WidgetSize.FULL
        )
    )
    # accuracy plot
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title = "model accuracy",
            filter = ReportFilter(metadata_values={},tag_values=["classification","full"]),
            values = [
                PanelValue(
                    metric_id = "ClassificationQualityMetric",
                    field_path = metrics.ClassificationQualityMetric.fields.current.accuracy,
                    legend = "current"
                ),
                PanelValue(
                    metric_id = "ClassificationQualityMetric",
                    field_path = metrics.ClassificationQualityMetric.fields.reference.accuracy,
                    legend = "reference"
                )
            ],
            plot_type = PlotType.LINE,
            size = WidgetSize.FULL
        )
    )
    # recall and precision plot
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title = "model precision",
            filter = ReportFilter(metadata_values={},tag_values=["classification","full"]),
            values = [
                PanelValue(
                    metric_id = "ClassificationQualityMetric",
                    field_path = metrics.ClassificationQualityMetric.fields.current.precision,
                    legend = "current"
                ),
                PanelValue(
                    metric_id = "ClassificationQualityMetric",
                    field_path = metrics.ClassificationQualityMetric.fields.reference.precision,
                    legend = "reference"
                )
            ],
            plot_type = PlotType.LINE,
            size = WidgetSize.HALF
        )
    )
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title = "model recall",
            filter = ReportFilter(metadata_values={},tag_values=["classification","full"]),
            values = [
                PanelValue(
                    metric_id = "ClassificationQualityMetric",
                    field_path = metrics.ClassificationQualityMetric.fields.current.recall,
                    legend = "current"
                ),
                PanelValue(
                    metric_id = "ClassificationQualityMetric",
                    field_path = metrics.ClassificationQualityMetric.fields.reference.recall,
                    legend = "reference"
                )
            ],
            plot_type=PlotType.LINE,
            size=WidgetSize.HALF
        )
    )        
    # f1 score plot
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title = "model f1 score",
            filter = ReportFilter(metadata_values={},tag_values=["classification","full"]),
            values = [
                PanelValue(
                    metric_id = "ClassificationQualityMetric",
                    field_path = metrics.ClassificationQualityMetric.fields.current.f1,
                    legend = "current"
                ),
                PanelValue(
                    metric_id = "ClassificationQualityMetric",
                    field_path = metrics.ClassificationQualityMetric.fields.reference.f1,
                    legend = "reference"
                )
            ],
            plot_type = PlotType.LINE,
            size = WidgetSize.FULL
        )
    )
    # classe based metrics : not yet available as an option by the evidently library
    #ressources ( needs debugging)
    ### test
    # columns drift score moving by time :
    project.dashboard.add_panel(
        DashboardPanelPlot(
            title="columns drift score moving by time",
            filter=ReportFilter(metadata_values={}, tag_values=["classification","full"]),
            values=[
                PanelValue(
                    metric_id="ColumnDriftMetric",
                    metric_args={"column_name.name": "target"},
                    field_path= metrics.ColumnDriftMetric.fields.drift_score,
                    legend="target",
                ),
            ],
            plot_type=PlotType.LINE,
            size=WidgetSize.FULL,
        )
    )
    # data quality tests :
    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="data quality tests: aggregated",
            filter=ReportFilter(metadata_values={}, tag_values=["data quality tests","full"], include_test_suites=True),
            size=WidgetSize.HALF,
            time_agg="1D",
        )
    )
    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="data quality tests: detailed",
            filter=ReportFilter(metadata_values={}, tag_values=["data quality tests","full"], include_test_suites=True),
            size=WidgetSize.HALF,
            panel_type=TestSuitePanelType.DETAILED,
            time_agg="1D",
        )
    )
    # data drift tests :
    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="data drift tests: aggregated",
            filter=ReportFilter(metadata_values={}, tag_values=["data drift tests","full"], include_test_suites=True),
            size=WidgetSize.HALF,
            time_agg="1D",
        )
    )
    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="data drift tests: detailed",
            filter=ReportFilter(metadata_values={}, tag_values=["data drift tests","full"], include_test_suites=True),
            size=WidgetSize.HALF,
            panel_type=TestSuitePanelType.DETAILED,
            time_agg="1D",
        )
    )
    # model tests :
    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="model tests: aggregated",
            filter=ReportFilter(metadata_values={}, tag_values=["model tests","full"], include_test_suites=True),
            size=WidgetSize.HALF,
            time_agg="1D",
        )
    )
    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="model tests: detailed",
            filter=ReportFilter(metadata_values={}, tag_values=["model tests","full"], include_test_suites=True),
            size=WidgetSize.HALF,
            panel_type=TestSuitePanelType.DETAILED,
            time_agg="1D",
        )
    )
    # all tests :
    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="all tests: aggregated",
            filter=ReportFilter(metadata_values={}, tag_values=["full"], include_test_suites=True),
            size=WidgetSize.FULL,
            time_agg="1D",
        )
    )
    project.dashboard.add_panel(
        DashboardPanelTestSuite(
            title="all tests: detailed",
            filter=ReportFilter(metadata_values={}, tag_values=["full"], include_test_suites=True),
            size=WidgetSize.FULL,
            panel_type=TestSuitePanelType.DETAILED,
            time_agg="1D",
        )
    )
    project.save()



@op(
    required_resource_key={"s3_resource"},
    out={"result": Out()}
)

def load_json_files(context):
    """Load JSON files from an S3 bucket that have the tag 'In Progress'."""
    s3_client = context.resources.s3_resource
    bucket_name = context.resource_config["bucket_name"]

    # List objects in the specified bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    # Check if the bucket contains any objects
    if 'Contents' not in response:
        context.log.info("No files found in the bucket.")
        return []

    json_files = []
    for obj in response['Contents']:
        key = obj['Key']
        
        # Get object tags
        tags_response = s3_client.get_object_tagging(Bucket=bucket_name, Key=key)
        tags = {tag['Key']: tag['Value'] for tag in tags_response['TagSet']}
        
        # Check for 'In Progress' tag
        if tags.get('Status') == 'In Progress':
            # Download the file content
            file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
            file_content = file_obj['Body'].read().decode('utf-8')

            try:
                json_data = json.loads(file_content)
                json_files.append(json_data)
                context.log.info(f"Successfully loaded {key} from bucket {bucket_name}")
            except json.JSONDecodeError as e:
                context.log.error(f"Failed to decode JSON from {key}: {e}")

    return json_files


@op(
     required_resource_keys={"s3_resource"}
)

def process_s3_files(context, bucket_name, download_directory, columns_to_select, columns_rename_map):
    """
    List .json files with 'pending' tag in the specified S3 bucket, download them, 
    transform the data, and concatenate them into a single DataFrame.

    Parameters:
    bucket_name (str): The name of the S3 bucket.
    download_directory (str): The directory where the files should be downloaded.
    columns_to_select (list): The list of columns to select from each JSON file.
    columns_rename_map (dict): A dictionary mapping old column names to new column names.

    Returns:
    pd.DataFrame: The concatenated DataFrame containing data from all JSON files with selected and renamed columns.
    """

    bucket_name = context.resource.s3_resource[1]
    try:
        s3 = context.resource.s3_resource[0]
        
        # List .json files with 'pending' tag
        response = s3.list_objects_v2(Bucket=bucket_name)
        files_to_download = []
        
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('.json'):
                    try:
                        # Check if the file has the 'pending' tag
                        tagging = s3.get_object_tagging(Bucket=bucket_name, Key=key)
                        tags = {tag['Key']: tag['Value'] for tag in tagging['TagSet']}
                        if tags.get('STATUS') == 'In Progress':
                            files_to_download.append(key)
                    except ClientError as e:
                        print(f"Error retrieving tags for {key}: {str(e)}")
                        update_tag_to_fail(s3, bucket_name, key)
        else:
            print(f"No files found in bucket: {bucket_name}")
            return pd.DataFrame()

        all_dataframes = []

        # Download and process the files
        for file_name in files_to_download:
            try:
                download_path = os.path.join(download_directory, file_name)
                
                # Ensure the directory exists
                os.makedirs(os.path.dirname(download_path), exist_ok=True)
                
                s3.download_file(bucket_name, file_name, download_path)
                print(f"Downloaded {file_name} to {download_path}")

                # Load JSON file and process it
                with open(download_path, 'r') as file:
                    data = json.load(file)
                    df = pd.DataFrame(data)
                    
                    # Select and rename columns
                    df = df[columns_to_select].rename(columns=columns_rename_map)
                    
                    all_dataframes.append(df)
                    update_tag_to_done(s3, bucket_name, file_name)  # Mark file as done if successfully processed
            except ClientError as e:
                print(f"Error downloading {file_name}: {str(e)}")
                update_tag_to_fail(s3, bucket_name, file_name)
            except FileNotFoundError:
                print(f"Error: The file {file_name} was not found.")
                update_tag_to_fail(s3, bucket_name, file_name)
            except json.JSONDecodeError:
                print(f"Error: The file {file_name} could not be decoded.")
                update_tag_to_fail(s3, bucket_name, file_name)
            except Exception as e:
                print(f"An unexpected error occurred while processing {file_name}: {e}")
                update_tag_to_fail(s3, bucket_name, file_name)
        
        try:
            # Concatenate all DataFrames
            concatenated_df = pd.concat(all_dataframes, ignore_index=True)
            crdt_df = concatenated_df[concatenated_df['type'] == 'CRDT']
            dbit_df = concatenated_df[concatenated_df['type'] == 'DBIT']
            return crdt_df, dbit_df,files_to_download,s3
        except Exception as e:
            print(f"An unexpected error occurred while concatenating DataFrames: {e}")
            for file_name in files_to_download:
                update_tag_to_fail(s3, bucket_name, file_name)
            return pd.DataFrame()

    except (NoCredentialsError, PartialCredentialsError) as e:
        print("Credentials not available or incomplete:", str(e))
        return pd.DataFrame()
    except Exception as e:
        print(f"Error processing files: {str(e)}")
        return pd.DataFrame()



@op(
     required_resource_keys={"slack_resource"}
)

def add_test_to_projectcontextm (context, project_name, ref, current,tag, column_mapping, s3, bucket_name, files_to_process):
    slack_token = context.resource.slack_resource[0]
    channel_id = context.resource.slack_resource[1]

    ws = Workspace.create(WORKSPACE)
    project = ws.search_project(project_name)[0]

    try :
        # data quality test

        data_quality, data_quality_failed = data_quality_tests(ref, current,tag, column_mapping)
        ws.add_test_suite(project.id, data_quality)
        print("data quality test added")
        if any(data_quality_failed):
            get_evidently_html_and_send_to_slack(data_quality, slack_token, channel_id, filename = "data_quality_test.html",comment = "ALERT"+ tag + "\n"+"Failure in \n"+"++"+"Data Quality test :"+"\n"+ data_quality_failed )

        # data drift test

        data_drift, data_drift_failed = data_drift_tests(ref, current,tag, column_mapping)
        ws.add_test_suite(project.id, data_drift)
        print("data drift test added")
        if any(data_drift_failed):
            get_evidently_html_and_send_to_slack(data_drift, slack_token, channel_id, filename = "data_drift_test.html",comment =  "ALERT"+ tag + "\n"+"Failure in \n"+"++"+"Data Drift test :"+"\n"+ data_drift_failed )

        # model performance test

        model_performance, model_performance_failed = model_performance_tests(ref, current,tag, column_mapping)
        ws.add_test_suite(project.id, model_performance)
        print("model performance test added")
        if any(model_performance_failed):
            get_evidently_html_and_send_to_slack(model_performance, slack_token, channel_id, filename = "model_performance_test.html",comment =  "ALERT"+ tag + "\n"+"Failure in \n"+"++"+"Model Performance test :"+"\n"+ model_performance_failed )
        update_tag_to_done(s3, bucket_name, files_to_process)
    except Exception as e:
        print(f"Error adding tests to project: {str(e)}")
        for file in files_to_process:
            send_alert_to_slack(f"with {file} we had the error of :\n Error adding tests to project: {str(e)}", slack_token, channel_id)
            update_tag_to_fail(s3, bucket_name, file)


@op
def create_report(ref, current, tag, column_mapping):
    print("classification_report")
    classification_report = Report(metrics=[
    metrics.ClassificationQualityMetric(),
    metrics.ConflictTargetMetric(),
    metrics.ConflictPredictionMetric(),
    metrics.DataDriftTable(columns=['target','prediction','text'],cat_stattest = "TVD",cat_stattest_threshold = 0.6,text_stattest= "perc_text_content_drift",text_stattest_threshold=0.5),
    metrics.ClassificationConfusionMatrix(),
    metrics.ClassificationQualityByClass(),
    metrics.ColumnDriftMetric(column_name="target", stattest ="TVD", stattest_threshold = 0.6),
    metrics.DatasetSummaryMetric(),
    ],
    metadata= {"classification report":4},
    tags=["classification",tag,"data"]
    )

    classification_report.run(reference_data= ref[["target","prediction","text"]],
                            current_data= current[['target','prediction','text']], 
                            column_mapping=column_mapping)
    print('classification_report done')
    return classification_report


@op
def create_report_2(ref, current, tag, column_mapping):
    print("text_report")
    text_report = Report(metrics=[
    metrics.TextDescriptorsDistribution(column_name="text"),
    #metrics.EmbeddingsDriftMetric('set')
    ],
    tags = ["text",tag])
    text_report.run(reference_data= ref ,
                    current_data= current, 
                    column_mapping=column_mapping)
    print('text_report done')
    return text_report


@op(
     required_resource_keys={"slack_resource"}
)

def add_report_to_project(context, project_name, ref, current,tag, column_mapping, s3, bucket_name, files_to_process):
    slack_token = context.resource.slack_resource[0]
    channel_id = context.resource.slack_resource[1]


    ws = Workspace.create(WORKSPACE)
    project = ws.search_project(project_name)[0]
    try :
        report = create_report(ref, current,tag, column_mapping)
        ws.add_report(project.id, report)
        print("classification_report added")
        report_2 = create_report_2(ref, current,tag, column_mapping)
        ws.add_report(project.id, report_2)
        print("text_report added")
        update_tag_to_done(s3, bucket_name, files_to_process)
    except Exception as e:
        print(f"Error adding report to project: {str(e)}")
        for file in files_to_process:
            send_alert_to_slack(f"with {file} we had the error of :\n Error adding reports to project: {str(e)}", slack_token, channel_id)
            update_tag_to_fail(s3, bucket_name, file)

