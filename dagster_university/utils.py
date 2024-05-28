import os 
import shutil
import tempfile
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError # type: ignore
from evidently import ColumnMapping # type: ignore
import panadas as pd  # type: ignore
import json 
from slack_sdk import WebClient # type: ignore
from evidently.test_suite import TestSuite # type: ignore
from evidently.tests import * # type: ignore
##########################################################################################################################

payments_names = [
                'Housing','Food & Drinks',
                'Leisure & Entertainment','Transportation',
                'Recurrent Payments','Investment',
                'Shopping','Healthy & Beauty',
                'Bank services','Other'
                ]


WORKSPACE = "monitor income"
PROJECT_NAME = "income Data and model monitoring "
PROJECT_DESCRIPTION = "Dashboard for monitoring data and model performance."
PROJECTS_NAMES = ["income Data and model monitoring ","debit name here please"]

current_path = "project-dagster-university/data/current"
ref_path = "project-dagster-university/data/ref"

columns_to_select = ['REMITTANCE_INFORMATION',
                    'CATEGORY',
                    'category',
                    'CREDIT_DEBIT_INDICATOR'] 
columns_rename_map = {
    'REMITTANCE_INFORMATION': 'text',
    'CATEGORY': 'target',
    'category': 'prediction',
    'CREDIT_DEBIT_INDICATOR': 'type'
}
column_mapping = ColumnMapping(
    #target= 'target',
    categorical_features=['prediction', 'target'],
    text_features=['text'],
    #task='classification',
    #embeddings={'set':data_tot.columns[7:]}
    )



#############################################  Utils Functions ###########################################################
def split_category(row):
    return row.split(' / ')[0]

def clear_directory(directory):
    """Delete all files and subdirectories in the specified directory."""
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory)


def update_tag_to_fail(s3, bucket_name, key):
    """
    Update the tag of a specific file or files to 'fail'.

    Parameters:
    s3 (boto3.client): The S3 client.
    bucket_name (str): The name of the S3 bucket.
    key (str or list): The key(s) of the file(s).
    """
    try:
        if isinstance(key, str):
            keys_to_update = [key]
        elif isinstance(key, list):
            keys_to_update = key
        else:
            raise ValueError("key must be a string or a list of strings")

        for k in keys_to_update:
            s3.put_object_tagging(
                Bucket=bucket_name,
                Key=k,
                Tagging={
                    'TagSet': [
                        {
                            'Key': 'status_monitoring',
                            'Value': 'fail'
                        }
                    ]
                }
            )
            print(f"Updated tag to 'fail' for {k}")
    except ClientError as e:
        print(f"Error updating tag for {key}: {str(e)}")


def update_tag_to_done(s3, bucket_name, key):
    """
    Update the tag of a specific file or files to 'done'.

    Parameters:
    s3 (boto3.client): The S3 client.
    bucket_name (str): The name of the S3 bucket.
    key (str or list): The key(s) of the file(s).
    """
    try:
        if isinstance(key, str):
            keys_to_update = [key]
        elif isinstance(key, list):
            keys_to_update = key
        else:
            raise ValueError("key must be a string or a list of strings")

        for k in keys_to_update:
            s3.put_object_tagging(
                Bucket=bucket_name,
                Key=k,
                Tagging={
                    'TagSet': [
                        {
                            'Key': 'status_monitoring',
                            'Value': 'done'
                        }
                    ]
                }
            )
            print(f"Updated tag to 'done' for {k}")
    except ClientError as e:
        print(f"Error updating tag for {key}: {str(e)}")


def load_json_into_df(directory):
    """
    Reads all JSON files in the specified directory, selects and renames specified columns,
    converts them into DataFrames, and concatenates them into a single DataFrame.

    Parameters:
    directory (str): The directory where the JSON files are located.
    columns_to_select (list): The list of columns to select from each JSON file.
    columns_rename_map (dict): A dictionary mapping old column names to new column names.

    Returns:
    pd.DataFrame: The concatenated DataFrame containing data from all JSON files with selected and renamed columns.
    """
    all_dataframes = []
    
    try:
        # List all files in the directory
        files = os.listdir(directory)
        
        # Filter out JSON files
        json_files = [file for file in files if file.endswith('.json')]
        
        if not json_files:
            print("No JSON files found in the directory.")
            return pd.DataFrame()
        
        # Read each JSON file, select and rename columns, and append the DataFrame to the list
        for json_file in json_files:
            file_path = os.path.join(directory, json_file)
            with open(file_path, 'r') as file:
                data = json.load(file)
                df = pd.DataFrame(data)
                
                # Select and rename columns
                df = df[columns_to_select].rename(columns=columns_rename_map)
                
                all_dataframes.append(df)
        
        # Concatenate all DataFrames
        concatenated_df = pd.concat(all_dataframes, ignore_index=True)
        crdt_df = concatenated_df[concatenated_df['type'] == 'CRDT']
        dbit_df = concatenated_df[concatenated_df['type'] == 'DBIT']
        return crdt_df, dbit_df
    
    except FileNotFoundError:
        print(f"Error: The directory {directory} was not found.")
        return pd.DataFrame()
    except json.JSONDecodeError:
        print(f"Error: One of the files could not be decoded.")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame()        
    


def send_alert_to_slack(message, slack_token, channel_id): 
    client = WebClient(token=slack_token)
    response = client.chat_postMessage(channel=channel_id, text=message)
    assert response["ok"]


def data_quality_tests(ref, current,tag, column_mapping):
    print("data_quality_tests")
    failed_tests = ""
    data_quality_tests = TestSuite(
        tests=[
            TestConflictPrediction(),
            # TestTargetPredictionCorrelation(),
        ],
        metadata= {"data quality tests":1},
        tags=["data quality tests",tag,"data"]
        )
    data_quality_tests.run(reference_data= ref ,
                        current_data= current, 
                        column_mapping=column_mapping)
    print("data_quality_tests done")
    test_summary = data_quality_tests.as_dict()
    for test in test_summary["tests"]:
        if test["status"].lower() == "fail":
            failed_tests = failed_tests + (str("  --"+test["name"]+":"+"\n"+"    "+test["description"])+"\n"+"\n")
    return data_quality_tests, failed_tests


def data_drift_tests(ref, current,tag, column_mapping):
    print("data_drift_tests")
    failed_tests = ""
    data_drift_tests = TestSuite(
        tests=[
            TestColumnDrift(column_name='target',stattest = "TVD", stattest_threshold = 0.6),
            TestColumnDrift(column_name='prediction',stattest = "TVD", stattest_threshold = 0.6),
            #TestEmbeddingsDrift(embeddings_name='set')
        ],
        metadata= {"data drift tests":1},
        tags=["data drift tests",tag,"data"]
        )
    data_drift_tests.run(reference_data= ref ,
                        current_data= current, 
                        column_mapping=column_mapping)
    print("data_drift_tests done")
    test_summary = data_drift_tests.as_dict()
    for test in test_summary["tests"]:
        if test["status"].lower() == "fail":
            failed_tests = failed_tests + (str("  --"+test["name"]+":"+"\n"+"    "+test["description"])+"\n"+"\n")
    return data_drift_tests, failed_tests


def model_performance_tests(ref, current,tag, column_mapping):
    print("model_performance_tests")
    failed_tests = ""
    model_performance_tests = TestSuite(
        tests=[
            TestAccuracyScore(),
            TestF1Score(),
            TestPrecisionScore(),
            TestRecallScore(),
            #TestPrecisionByClass(label='labeeeeeeeel')
        ],
        metadata= {"model tests":1},
        tags=["classification",tag,"model tests"]
        )
    model_performance_tests.run(reference_data= ref ,
                        current_data= current, 
                        column_mapping=column_mapping)
    print("model_performance_tests done")
    test_summary = model_performance_tests.as_dict()
    for test in test_summary["tests"]:
        if test["status"].lower() == "fail":
            failed_tests = failed_tests + (str("  --"+test["name"]+":"+"\n"+"    "+test["description"])+"\n"+"\n")
    # if any(failed_tests):
    #     return model_performance_tests, failed_tests, 
    return model_performance_tests, failed_tests

def send_alert_to_slack(message, slack_token, channel_id): 
    client = WebClient(token=slack_token)
    response = client.chat_postMessage(channel=channel_id, text=message)
    assert response["ok"]


def get_evidently_html_and_send_to_slack(context, evidently_object, slack_token, channel_id, filename,comment) -> None:
    """Generates HTML content from EvidentlyAI and sends it to Slack channel without saving it locally"""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        evidently_object.save_html(tmp.name)
        with open(tmp.name, 'r', encoding='utf-8') as fh:
            html_content = fh.read()

    client = WebClient(token=slack_token)
    response = client.files_upload(
        channels=channel_id,
        file=html_content.encode("utf-8"),
        filename=filename,
        filetype="html",
        initial_comment= comment,
    )



