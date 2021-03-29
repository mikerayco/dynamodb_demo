import time
from decimal import Decimal
import json
from pprint import pprint

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from IPython import embed

table_name = 'movies-apper2'
dynamodb = boto3.resource('dynamodb')


# create table
def create_movie_table():
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'year',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    print(f"Table Status : {table.table_status}")
    return None


# load data to table
def load_movies():
    table = dynamodb.Table(table_name)

    with open("moviedata.json") as json_file:
        movie_list = json.load(json_file, parse_float=Decimal)

    for movie in movie_list:
        year = int(movie['year'])
        title = movie['title']
        print("Adding movie:", year, title)
        table.put_item(Item=movie)


# add movie
def put_movie(title, year, plot, rating):
    table = dynamodb.Table(table_name)
    response = table.put_item(
        Item={
            'year': year,
            'title': title,
            'info': {
                'plot': plot,
                'rating': rating
            }
        }
    )
    return response


# retrieve a movie
def get_movie(title, year):
    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(Key={'year': year, 'title': title})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']


def update_movie(title, year, rating, plot, actors: list):
    table = dynamodb.Table(table_name)
    response = table.update_item(
        Key={
            'year': year,
            'title': title
        },
        UpdateExpression="set info.rating=:r, info.plot=:p, info.actors=:a",
        ExpressionAttributeValues={
            ':r': Decimal(rating),
            ':p': plot,
            ':a': actors
        },
        ReturnValues="UPDATED_NEW"
    )
    return response


# conditional update
def remove_actors(title, year, actor_count):
    table = dynamodb.Table(table_name)

    try:
        response = table.update_item(
            Key={
                'year': year,
                'title': title
            },
            UpdateExpression="remove info.actors[0]",
            ConditionExpression="size(info.actors) >= :num",
            ExpressionAttributeValues={':num': actor_count},
            ReturnValues="UPDATED_NEW"
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        return response


# delete a movie
def delete_underrated_movie(title, year, rating):
    table = dynamodb.Table(table_name)

    try:
        response = table.delete_item(
            Key={
                'year': year,
                'title': title
            },
            ConditionExpression="info.rating <= :val",
            ExpressionAttributeValues={
                ":val": Decimal(rating)
            }
        )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    else:
        return response


# query function
def query_movies(year):
    table = dynamodb.Table(table_name)
    response = table.query(
        KeyConditionExpression=Key('year').eq(year)
    )
    return response['Items']


# scan function
def scan_movies(year_range: tuple):
    table = dynamodb.Table(table_name)
    scan_kwargs = {
        'FilterExpression': Key('year').between(*year_range),
        'ProjectionExpression': "#yr, title, info.rating",
        'ExpressionAttributeNames': {"#yr": "year"}
    }

    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        movies = response.get('Items', [])
        for movie in movies:
            print(f"\n{movie['year']} : {movie['title']}")
            pprint(movie['info'])
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None


def delete_movie_table():
    table = dynamodb.Table(table_name)
    table.delete()
    print('table deleted')


embed()
