import boto3
import time
import os

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

ec2 = boto3.resource(
    'ec2',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name='us-east-1'
)

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name='us-east-1'
)

bucket = boto3.resource(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name='us-east-1'
        )

sqs = boto3.client(
    'sqs',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name='us-east-1'
)


# Creation
def create_ec2_instance(ami_id, instance_type, key_pair_name, instance_name):
    instances = ec2.create_instances(
        ImageId=ami_id, 
        InstanceType=instance_type,
        KeyName=key_pair_name, 
        MinCount=1, 
        MaxCount=1,
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [
                    {'Key': 'Name', 'Value': instance_name}
                ]
            }
        ]
    )

    print(f"EC2 instance created with Name: {instance_name} and ID: {instances[0].id}")
    return instances
    

def create_s3_bucket(bucket_name):
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"S3 bucket '{bucket_name}' created successfully")
    except s3.exceptions.BucketAlreadyExists as e:
        print(f"Bucket '{bucket_name}' already exists. Please use a unique name.")
    except Exception as e:
        print(f"Error creating bucket: {e}")
        

def create_sqs_queue(queue_name):
    try:
        if not queue_name.endswith(".fifo"):
            queue_name += ".fifo"

        response = sqs.create_queue(
            QueueName=queue_name,
            Attributes={
                'FifoQueue': 'true',
                'ContentBasedDeduplication': 'true'
            }
        )

        print(f"SQS queue '{queue_name}' created successfully.")
        return response['QueueUrl']

    except Exception as e:
        print(f"Error creating SQS FIFO queue: {e}")
        return None
        
    
# Listing
def list_ec2_instances():
    print("Listing all EC2 instances:")
    for instance in ec2.instances.all():
        instance_name = ''
        for tag in instance.tags or []:
            if tag['Key'] == 'Name':
                instance_name = tag['Value']
        print(f'Instance Name: {instance_name}, Instance ID: {instance.id}, State: {instance.state["Name"]}')
    print("\n")
        
def list_s3_buckets():
    print("Listing all S3 buckets:")
    try:
        response = s3.list_buckets()
        buckets = response['Buckets']
        
        if buckets:
            for bucket in buckets:
                bucket_name = bucket['Name']
                bucket_location = s3.get_bucket_location(Bucket=bucket_name)
                region = bucket_location['LocationConstraint']
                if region == None:
                    region = 'us-east-1'
                else:
                    region = bucket_location['LocationConstraint']
                print(f'Bucket Name: {bucket_name}, Region: {region}')
        else:
            print("No buckets found.")
    except Exception as e:
        print(f"Error listing buckets: {e}")
    print("\n")
    
def list_sqs_queues():
    print("Listing all SQS Queues:")
    try:
        response = sqs.list_queues()
        
        if 'QueueUrls' in response:
            for queue_url in response['QueueUrls']:
                print(f"Queue URL: {queue_url}")
        else:
            print("No SQS queues found.")
    
    except Exception as e:
        print(f"Error listing SQS queues: {e}")
        
    print("\n")
    

# Uploading
def upload_to_s3(bucket_name, file, object_name = None):
    if object_name == None:
        object_name = file
    try:
        s3.upload_file(file, bucket_name, object_name)
        print(f"File '{file}' uploaded to bucket '{bucket_name}' as '{object_name}'")
    except Exception as e:
        print(f"Error uploading file: {e}")
    print("\n")
        
def send_msg_to_sqs(queue_url, msg_body, msg_name):
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=msg_body,
            MessageAttributes={
                'MessageName': {
                    'StringValue': msg_name,
                    'DataType': 'String'
                }
            },
            MessageGroupId='testGroupId',
            MessageDeduplicationId=msg_name
        )
        print(f"Message '{msg_name}' sent successfully.")

    except Exception as e:
        print(f"Error sending message to SQS: {e}")
    
    print("\n")

def check_sqs_count(queue_url):
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )

        message_count = int(response['Attributes']['ApproximateNumberOfMessages'])
        print(f"Number of messages in the queue: {message_count}")

    except Exception as e:
        print(f"Error checking message count in SQS queue: {e}")
    
    print("\n")
        

def delete_from_sqs(queue_url, receipt_handle):
    try:
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print("Message pulled and processed successfully.")
    except Exception as e:
        print(f"Error deleting message from SQS: {e}")
    print("\n")


def pull_from_sqs(queue_url, max_messages=1):
    print("Pulling message from SQS Queue:")
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=5
        )

        if 'Messages' in response:
            messages = response['Messages']
            for message in messages:
                msg_body = message['Body']
                msg_name = message['MessageAttributes']['MessageName']['StringValue'] if 'MessageAttributes' in message and 'MessageName' in message['MessageAttributes'] else "No name"
                receipt_handle = message['ReceiptHandle']
                
                print(f"Message Name: {msg_name}")
                print(f"Message Body: {msg_body}")
                print("\n")
                
                delete_from_sqs(queue_url, receipt_handle)
        else:
            print("No messages received.")
            return []

    except Exception as e:
        print(f"Error receiving messages from SQS: {e}")
        return []
        

# Termination - EC2       
def terminate_ec2_instance(instance_id):
    print(f"Terminating instance {instance_id}...")
    ec2.Instance(instance_id).terminate()
    ec2.Instance(instance_id).wait_until_terminated()
    print(f"Instance {instance_id} terminated.")
    

# Deletion - S3
def delete_s3_bucket(bucket, bucket_name):
    print("\n")
    try:
        print(f"Deleting all objects from bucket '{bucket_name}'...")
        cur_bucket = bucket.Bucket(bucket_name)
        cur_bucket.objects.all().delete()

        s3.delete_bucket(Bucket=bucket_name)
        print(f"S3 bucket '{bucket_name}' deleted successfully.")
    except s3.exceptions.NoSuchBucket as e:
        print(f"Bucket '{bucket_name}' does not exist.")
    except Exception as e:
        print(f"Error deleting bucket: {e}")
        

def delete_sqs_queue(queue_url):
    try:
        print(f"Deleting all queues from '{queue_url}'...")
        sqs.delete_queue(QueueUrl=queue_url)
        print(f"SQS queue '{queue_url}' deleted successfully.")
    except Exception as e:
        print(f"Error deleting SQS queue: {e}")
        

ami_id = 'ami-0e86e20dae9224db8'
instance_type = 't2.micro'
key_pair_name = 'sarfraznawaz'
instance_name = 'sarfraz-ec2'

bucket_name = "sarfraz-s3"

queue_name = "sarfraz-sqs"


ec2_instances = create_ec2_instance(ami_id, instance_type, key_pair_name, instance_name)
s3_bucket = create_s3_bucket(bucket_name)
sqs_queue = create_sqs_queue(queue_name)
if sqs_queue:
    print(f"Queue URL: {sqs_queue}")


print("\n")
print("Initializing... Please wait for 1 minute")
time.sleep(60)
print("\n")

list_ec2_instances()
list_s3_buckets()
list_sqs_queues()

file_name = "CSE546test.txt"
upload_to_s3(bucket_name, file_name)

send_msg_to_sqs(sqs_queue, "This is a test message", "Test_Message")
time.sleep(30)

check_sqs_count(sqs_queue)
pull_from_sqs(sqs_queue)
check_sqs_count(sqs_queue)

print("Initiating deletion of resources... Please wait for 10 seconds")
time.sleep(10)

instance_id = ec2_instances[0].id
terminate_ec2_instance(instance_id)
delete_s3_bucket(bucket, bucket_name)
delete_sqs_queue(sqs_queue)
print("Deletion completing... Please wait for 20 seconds")
time.sleep(20)
print("\n")

list_ec2_instances()
list_s3_buckets()
list_sqs_queues()