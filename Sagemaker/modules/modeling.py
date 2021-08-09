import time
import boto3
import sagemaker
from sagemaker.amazon.amazon_estimator import get_image_uri
from smexperiments.trial import Trial
from smexperiments.trial_component import TrialComponent

def create_model(sm_client, role, model_name, container_uri, model_data):
    return sm_client.create_model(
        ModelName=model_name,
        PrimaryContainer={
        'Image': container_uri,
        'ModelDataUrl': model_data,
        },
        ExecutionRoleArn=role)

def create_estimator(role, rawbucket, prefix, sess, train_data_location, tracker, cc_experiment, sm):
    container = get_image_uri(boto3.Session().region_name, 'xgboost', '1.0-1')
    s3_input_train = sagemaker.s3_input(s3_data='s3://' + train_data_location, content_type='csv')

    xgb = sagemaker.estimator.Estimator(container,
                                    role, 
                                    train_instance_count=1, 
                                    train_instance_type='ml.m4.xlarge',
                                    train_max_run=86400,
                                    output_path='s3://{}/{}/models'.format(rawbucket, prefix),
                                    sagemaker_session=sess) # set to true for distributed training

    xgb.set_hyperparameters(max_depth=5,
                            eta=0.2,
                            gamma=4,
                            min_child_weight=6,
                            subsample=0.8,
                            verbosity=0,
                            objective='binary:logistic',
                            num_round=100)
    
    trial_name = f"cc-default-training-job-{int(time.time())}"
    cc_trial = Trial.create(
            trial_name=trial_name, 
            experiment_name=cc_experiment.experiment_name,
            sagemaker_boto_client=sm
        )
    preprocessing_trial_component = tracker.trial_component
    cc_trial.add_trial_component(preprocessing_trial_component)
    cc_training_job_name = "cc-training-job-{}".format(int(time.time()))
    xgb.fit(inputs = {'train':s3_input_train},
           job_name=cc_training_job_name,
            experiment_config={
                "TrialName": cc_trial.trial_name, #log training job in Trials for lineage
                "TrialComponentDisplayName": "Training",
            },
            wait=True,
            logs=False
        )
    return xgb