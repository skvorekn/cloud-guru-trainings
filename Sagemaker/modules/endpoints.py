import time

def create_endpoint_config(sm_client, model_config, data_capture_config): 
    return sm_client.create_endpoint_config(
                                                EndpointConfigName=model_config,
                                                ProductionVariants=[
                                                        {
                                                            'VariantName': 'AllTraffic',
                                                            'ModelName': model_config,
                                                            'InitialInstanceCount': 1,
                                                            'InstanceType': 'ml.m4.xlarge',
                                                            'InitialVariantWeight': 1.0,
                                                },
                                                    
                                                    ],
                                                DataCaptureConfig=data_capture_config
                                                )

def create_endpoint(sm_client, endpoint_name, config_name):
    return sm_client.create_endpoint(
                                    EndpointName=endpoint_name,
                                    EndpointConfigName=config_name
                                )

def attempt_create_endpoint_config(sm_client, training_job_name, data_capture_configuration):
    try:
        endpoint_config = create_endpoint_config(sm_client, training_job_name, data_capture_configuration)
    except Exception as e:
        print('Endpoint configuration already exists. Deleting & re-creating.')
        sm_client.delete_endpoint_config(EndpointConfigName=training_job_name)
        endpoint_config = create_endpoint_config(sm_client, training_job_name, data_capture_configuration)

    print('Endpoint configuration created as endpoint_config["EndpointConfigArn"]')
    
def extract_captured_files(s3_client, prefix, endpoint_name, rawbucket):
    data_capture_prefix = '{}/monitoring'.format(prefix)
    current_endpoint_capture_prefix = '{}/datacapture/{}/AllTraffic'.format(data_capture_prefix, endpoint_name)
    print(current_endpoint_capture_prefix)
    result = s3_client.list_objects(Bucket=rawbucket, Prefix=current_endpoint_capture_prefix)

    capture_files = [capture_file.get("Key") for capture_file in result.get('Contents')]
    print(f"Found {len(capture_files)} Capture Files like:")
    print(capture_files[0])
    return capture_files

def invoke_endpoint(ep_name, file_name, runtime_client):
    with open(file_name, 'r') as f:
        for row in f:
            payload = row.rstrip('\n')
            response = runtime_client.invoke_endpoint(EndpointName=ep_name,
                                          ContentType='text/csv', 
                                          Body=payload)
            time.sleep(1)
            
def invoke_endpoint_forever(endpoint_name, runtime_client, test_name = 'test-data-input-cols.csv'):
    while True:
        invoke_endpoint(endpoint_name, test_name, runtime_client)
    