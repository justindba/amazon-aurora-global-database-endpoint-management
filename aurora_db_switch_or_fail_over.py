import sys
sys.path.append('/opt')

import os
import time
import json 
import sys
sys.path.append('/opt')

import os
import time
import json 
import boto3 
#import psycopg2
import pg8000 as pg
import aws_db_helper
from datetime import datetime
from datetime import timedelta
from datetime import datetime
from datetime import timedelta
from botocore.exceptions import ClientError as boto3_client_error

custom_functions = aws_db_helper.Functions()

app_db_credentials = custom_functions.get_db_credentials('App')


def getCredentials():
    credential = {}
    credential['username'] = app_db_credentials['username']
    credential['password'] = app_db_credentials['password']
    credential['host'] = app_db_credentials['host']
    #credential['host'] = os.environ['GLOBAL_APP_DB_READER_ENDPOINT_EAST1_INVALID']
    credential['port'] = app_db_credentials['port']
    credential['db'] = app_db_credentials['database']
    
    return credential
    
def test_db_connection_pg8():
    """ Connect to the PostgreSQL database server """
    conn = None
    credential = {}
    credential['username'] = app_db_credentials['username']
    credential['password'] = app_db_credentials['password']
    credential['host'] = app_db_credentials['host']
    #credential['port'] = app_db_credentials['port']
    #using proxy
    #credential['host'] = os.environ['GLOBAL_APP_DB_PROXY_READER_ENDPOINT_EAST1']
    credential['host'] = os.environ['GLOBAL_APP_DB_PROXY_WRITER_ENDPOINT_EAST1']
    
    credential['db'] = app_db_credentials['database']

    try:
        conn = pg.Connection(host=credential['host'], 
                         database=credential['db'], 
                         user=credential['username'], 
                         password=credential['password'])
    

        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        #print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print('Connection to Aurora PG at ' + credential['host'] + ' Successfully')
        
        # close the communication with the PostgreSQL
        cur.close()
    except pg.errors.Error:
        raise Exception
    return db_version


def detach_and_promote_failover_cluster():
    
    rds_client = boto3.client('rds')
    
    try:
        
        print('Attempting to Retrieve Global DB Cluster Members: "' + os.environ['GLOBAL_APP_DB_CLUSTER_IDENTIFIER'] + '"')
            
        describe_cluster_resp = rds_client.describe_global_clusters(
            GlobalClusterIdentifier = os.environ['GLOBAL_APP_DB_CLUSTER_IDENTIFIER']
        )
        
        '''
            For each Global Cluster member
        '''
        for cluster_member in describe_cluster_resp['GlobalClusters'][0]['GlobalClusterMembers']:
            
                '''
                    If this failover cluster is a member of the Global Cluster
                '''
                if os.environ['REGIONAL_APP_DB_CLUSTER_ARN'] == cluster_member['DBClusterArn']:
                    
                    try:
                        
                        print('Attempting to Detach Regional Cluster "' + os.environ['REGIONAL_APP_DB_CLUSTER_ARN'] + '" from Global DB Cluster "' + os.environ['GLOBAL_APP_DB_CLUSTER_IDENTIFIER'] + '"')
                        
                        rds_client.remove_from_global_cluster(
                            DbClusterIdentifier = os.environ['REGIONAL_APP_DB_CLUSTER_ARN'],
                            GlobalClusterIdentifier = os.environ['GLOBAL_APP_DB_CLUSTER_IDENTIFIER'],
                        )
                        
                        print('Successfully Detached Regional Cluster "' + os.environ['REGIONAL_APP_DB_CLUSTER_ARN'] + '" from Global DB Cluster "' + os.environ['GLOBAL_APP_DB_CLUSTER_IDENTIFIER'] + '"')
                    
                    except boto3_client_error as e:
                        raise Exception('Failed to Detach Failover Cluster from Global Cluster: ' + str(e))
                
    except boto3_client_error as e:
        raise Exception('Failed to Retrieve Global Cluster Members: ' + str(e))
                    
    return True 

def get_global_cluster_details():
    
    cluster_data = {}
    allregions = {}
    rds_client = boto3.client('rds')
    
    try:
        
        print('Attempting to Retrieve Global DB Cluster Members: "' + os.environ['GLOBAL_APP_DB_CLUSTER_IDENTIFIER'] + '"')
            
        describe_cluster_resp = rds_client.describe_global_clusters(
            GlobalClusterIdentifier = os.environ['GLOBAL_APP_DB_CLUSTER_IDENTIFIER']
        )
        
        '''
            For each Global Cluster member
        '''
        for cluster_member in describe_cluster_resp['GlobalClusters'][0]['GlobalClusterMembers']:
            
            '''
                If this switchover cluster is a member of the Global Cluster
            '''
            resourcename = cluster_member['DBClusterArn']
            resourcename = resourcename.split(':')
            regioname = resourcename[3] #region name is in the 3rd postion
            cluname = resourcename[6] #clustername is in the 6th position
            allregions[cluname]=regioname
            print('DBClusterArn', resourcename)
            print('IsWriter', cluster_member['IsWriter'])
            #for reader in cluster_member['Readers']):
            print('Readers', cluster_member['Readers'])
            print(regioname)
            print(cluname)
            
                
    except boto3_client_error as e:
        raise Exception('Failed to Retrieve Global Cluster Members: ' + str(e))
                    
    return True

def perform_switchover_global_cluster():

    allregions = {}
    rds_client = boto3.client('rds')
    reader_cluster = ''
    
    try:
        
        print('Attempting to Retrieve Global DB Cluster Members: "' + os.environ['GLOBAL_APP_DB_CLUSTER_IDENTIFIER'] + '"')
            
        describe_cluster_resp = rds_client.describe_global_clusters(
            GlobalClusterIdentifier = os.environ['GLOBAL_APP_DB_CLUSTER_IDENTIFIER']
        )
        for cluster_member in describe_cluster_resp['GlobalClusters'][0]['GlobalClusterMembers']:
            if cluster_member['IsWriter']:
                #print Current Cluster details
                resourcename = cluster_member['DBClusterArn']  #This is the ARN
                resourcename = resourcename.split(':') #Arn splits values with semicolon
                regioname = resourcename[3] #region name is in the 3rd postion
                cluname = resourcename[6] #clustername is in the 6th position
                print ("Current Writer: ", cluname,'in region', regioname)
             
        #SwitchOver to the Reader
        for cluster_member in describe_cluster_resp['GlobalClusters'][0]['GlobalClusterMembers']:
            
            resourcename = cluster_member['DBClusterArn']  #This is the ARN
            resourcename = resourcename.split(':') #Arn splits values with semicolon
            regioname = resourcename[3] #region name is in the 3rd postion
            cluname = resourcename[6] #clustername is in the 6th position

            if not cluster_member['IsWriter']: #Look for the next cluster if current cluster is reader
                print("Processing regional cluster", cluname, ":...")
                print ('switch over to ', cluname , ' in Region ', regioname)  
                response = rds_client.switchover_global_cluster(
                            GlobalClusterIdentifier = os.environ['GLOBAL_APP_DB_CLUSTER_IDENTIFIER'],
                            TargetDbClusterIdentifier = cluster_member['DBClusterArn'],
                    )
                print(response)
                return True
            
    except boto3_client_error as e:
        raise Exception('Failed to Retrieve Global Cluster Members: ' + str(e))
                    
    return True

def lambda_handler(event, context):
    
    failures = 0
    end_time = datetime.now() + timedelta(seconds = 6)
    #print(boto3.__version__)
    
    while (datetime.now() < end_time):  
        try:
            #test DB connection
            test_db_connection_pg8()
            failures = 1
    
        except Exception as e:
            
            failures += 1
            print('Failed to Establish DB Connection')
        
        print('failures = ', failures)
        if failures >= 1:
            
            #print('Connection Failure Tolerance Exceeded')            
            #detach_and_promote_failover_cluster()
            print('Connection Failure Tolerance Exceeded') 
            perform_switchover_global_cluster()
            
            #disable_canary_rule()
            
            #log_failover_event()
            
            return False
            
        time.sleep(10)
    
    return True
#Following code not required if running as lambda function
#if __name__ == '__main__':
#   event=""
#   context=""
#   lambda_handler(event,context)
