import boto3
import pandas as pd
import json
import re

class Talent_Extractor():
    def __init__(self):
        s3 = boto3.resource('s3')
        bucket_name = 'data-eng-210-final-project'
        self.objects = s3.Bucket(bucket_name).objects.all().filter(Prefix='Talent')

    # generator function to lazily read the data from S3 and yield each dataframe one at a time, rather than storing them all in memory at once. 
    def applicants_to_df(self):
        for obj in self.objects:
            if obj.key.endswith('.csv'):
                contents = obj.get()['Body']
                yield pd.read_csv(contents)

    def txt_to_df(self):
        for obj in self.objects:
            if obj.key.endswith('txt'):
                contents = obj.get()['Body'].read().decode('utf-8')
                split = contents.replace('\r', '').split('\n')
                table = []
                for line in split[3:-1]:
                    try:
                        names, rest = line.rsplit("-", 1)
                    except:
                        print(line)
                    names = names.replace(',', '')
                    columns = (names + ',' + rest).split(',')
                    row = columns[0].split(" ", 1)+[re.search('([0-9]{1,3})/', columns[-2]).group(
                        1), re.search('([0-9]{1,3})/', columns[-1]).group(1)]+[split[0], split[1].split(' ')[0]]
                    table.append(row)
                yield pd.DataFrame(table)


    def jsons_to_df(self):
        for obj in self.objects:
            if obj.key.endswith('.json'):
                json_data = obj.get()['Body'].read().decode('utf-8')
                data = json.loads(json_data)
                df = pd.json_normalize(data)
                yield df



    # def jsons_to_df(self):
    #     for obj in self.objects:
    #         if obj.key.endswith('.json'):
    #             contents = obj.get()['Body']
    #             yield pd.read_json(contents)
