#!/usr/bin/env python
#-*- coding: utf-8 -*-

import boto3

class S3_operator():
    def __init__(self, bucket_name='credit-cards-data'):        
        self.bucket = bucket_name
        
        session = boto3.session.Session()
        self.s3 = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net')         
        
    
    def create_bucket(self):
        self.s3.create_bucket(Bucket=self.bucket) 
        
        
    def get_object_list(self):
        for key in self.s3.list_objects(Bucket=self.bucket)['Contents']:
            print(key['Key'])
        
    
    def get_object(self, object_name):        
        get_object_response = self.s3.get_object(Bucket=self.bucket, Key=object_name)
        object_s3 = get_object_response['Body'].read()        
        with open(object_name, 'wb') as f:
            f.write(object_s3)
        
        
    def send_object(self, object_name, path='/'):
        self.s3.upload_file(path + object_name, self.bucket, object_name)
        
        
    def delete_object_list(self, object_list):
        to_delete = [{'Key': item} for item in object_list]
        response = self.s3.delete_objects(Bucket=self.bucket, Delete={'Objects': to_delete})


if __name__ == "__main__":
    s3_operator = S3_operator(bucket_name='credit-cards-data')
    s3_operator.get_object_list()
