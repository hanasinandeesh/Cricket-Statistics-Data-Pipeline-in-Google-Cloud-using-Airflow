from googleapiclient.discovery import build

def trigger_df_job(cloud_event, environment):   
    # Initialize the Dataflow service
    service = build('dataflow', 'v1b3')
    project = "prj-poc-001"  # Project ID

    # Dataflow template path (update if needed)
    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    # Set the job parameters and paths, ensuring the correct bucket names are used
    template_body = {
        "jobName": "bq-load",  # Provide a unique name for the job
        "parameters": {
            # Update the paths to point to the correct buckets
            "javascriptTextTransformGcsPath": "gs://cc-lab-96.appspot.com/ETL-bucket/udf.js",  # Example change
            "JSONPath": "gs://cc-lab-96.appspot.com/ETL-bucket/bq.json",  # Example change
            "javascriptTextTransformFunctionName": "transform",
            "outputTable": "prj-poc-001:cricket_dataset.icc_odi_batsman_ranking",
            "inputFilePattern": "gs://cc-lab-96.appspot.com/ETL-bucket/batsmen_rankings.csv",  # Example change
            "bigQueryLoadingTemporaryDirectory": "gs://cc-lab-96.appspot.com/ETL-bucket/temp-dir",  # Example change
        }
    }

    # Launch the Dataflow template with the specified parameters
    request = service.projects().templates().launch(projectId=project, gcsPath=template_path, body=template_body)
    response = request.execute()  # Execute the request and trigger the job
    print(response)  # Print the response to check the result
