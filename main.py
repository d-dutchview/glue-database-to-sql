import json
import boto3

session = boto3.Session(aws_access_key_id='ID',
                        aws_secret_access_key='SECRET')
glue_client = session.client('glue', region_name='eu-central-1') ##specify region


def translate(data):
    temp = []
    for i in data:
        if i['Type'] == 'string':
            temp.append({
                "Name": i['Name'],
                "Type": "VARCHAR(255)",
            })
        elif i['Type'] == 'bigint':
            temp.append({
                "Name": i['Name'],
                "Type": "BIGINT",
            })
        elif i['Type'] == 'double':
            temp.append({
                "Name": i['Name'],
                "Type": "DOUBLE",
            })
        elif i['Type'] == 'boolean':
            temp.append({
                "Name": i['Name'],
                "Type": "BOOLEAN",
            })
        elif i['Type'] == 'timestamp':
            temp.append({
                "Name": i['Name'],
                "Type": "TIMESTAMP",
            })
        elif i['Type'] == 'date':
            temp.append({
                "Name": i['Name'],
                "Type": "DATE",
            })
        elif i['Type'] == 'decimal':
            temp.append({
                "Name": i['Name'],
                "Type": "DECIMAL",
            })
        elif i['Type'] == 'char':
            temp.append({
                "Name": i['Name'],
                "Type": "CHAR",
            })
        elif i['Type'] == 'smallint':
            temp.append({
                "Name": i['Name'],
                "Type": "SMALLINT",
            })
        elif i['Type'] == 'tinyint':
            temp.append({
                "Name": i['Name'],
                "Type": "TINYINT",
            })
        elif i['Type'] == 'int':
            temp.append({
                "Name": i['Name'],
                "Type": "INT",
            })
        elif i['Type'] == 'float':
            temp.append({
                "Name": i['Name'],
                "Type": "FLOAT",
            })
        elif i['Type'] == 'binary':
            temp.append({
                "Name": i['Name'],
                "Type": "BINARY",
            })
        elif i['Type'] == 'array':
            temp.append({
                "Name": i['Name'],
                "Type": "ARRAY",
            })
        elif i['Type'] == 'map':
            temp.append({
                "Name": i['Name'],
                "Type": "MAP",
            })
        elif i['Type'] == 'struct':
            temp.append({
                "Name": i['Name'],
                "Type": "STRUCT",
            })
        elif i['Type'] == 'uniontype':
            temp.append({
                "Name": i['Name'],
                "Type": "UNIONTYPE",
            })
        elif i['Type'] == 'null':
            temp.append({
                "Name": i['Name'],
                "Type": "NULL",
            })
        else:
            temp.append({
                "Name": i['Name'],
                "Type": "VARCHAR(255)",
            })
    return temp



def get_tables_for_database(database):
    starting_token = None
    next_page = True
    tables = []
    while next_page:
        paginator = glue_client.get_paginator(operation_name="get_tables")
        response_iterator = paginator.paginate(
            DatabaseName=database,
            PaginationConfig={"PageSize": 100, "StartingToken": starting_token},
        )
        for elem in response_iterator:
            # print(elem)
            for table in elem["TableList"]:
                tables.append({
                    "name": table["Name"],
                    "Colums": translate(table['StorageDescriptor']["Columns"]),
                })
            try:
                starting_token = elem["NextToken"]
            except:
                next_page = False
    return tables


tableinfo = get_tables_for_database("DATABASE")

for table in tableinfo:
    s = f"""CREATE TABLE {table['name']} ("""
    for column in table["Colums"]:
          s += f"{column['Name']} {column['Type']},\n"

    s = s[:-2]
    s+= ");"
    print(s.strip())
