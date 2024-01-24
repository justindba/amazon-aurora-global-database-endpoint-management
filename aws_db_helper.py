_A='%H:%M:%S'
import dateutil.tz
import io,os,json,boto3
from botocore.exceptions import ClientError as boto3_client_error
from datetime import datetime
from datetime import timedelta
class Functions:
    def __init__(self):''
    def add_five_seconds(self,start_time):return(datetime.strptime(str(start_time),_A)+timedelta(seconds=5)).strftime(_A)
    def subtract_five_seconds(A,start_time):return(datetime.strptime(str(start_time),_A)+timedelta(seconds=-5)).strftime(_A)
    def add_time(self,label,data):
        A=label;C=dateutil.tz.gettz('US/Pacific');D=datetime.now(tz=C)
        while datetime.strptime(A[len(A)-1],_A)+timedelta(seconds=9)<datetime.strptime(D.strftime(_A),_A):A.pop(0);data.pop(0);A.append(B.add_five_seconds(A[len(A)-1]));data.append('0')
    def get_db_credentials(self,db_identifier):
        db_id=db_identifier
        sm=boto3.client('secretsmanager')
        try:
            print('REGIONAL_'+db_id.upper()+'_DB_SECRET_ARN')
            smval=sm.get_secret_value(SecretId=os.environ['REGIONAL_'+db_id.upper()+'_DB_SECRET_ARN'])
            print(smval)
        except boto3_client_error as e:
            raise Exception('Failed to Retrieve '+db_id.upper()+' Database Secret: '+str(e))
        else:
            return json.loads(smval['SecretString'])
    def update_dns_record(C,fqdn,new_value,hosted_zone_id,ttl=1,record_type='CNAME'):
        A=boto3.client('route53')
        try:A.change_resource_record_sets(ChangeBatch={'Changes':[{'Action':'UPSERT','ResourceRecordSet':{'Name':fqdn,'ResourceRecords':[{'Value':new_value}],'TTL':ttl,'Type':record_type}}]},HostedZoneId=hosted_zone_id)
        except boto3_client_error as B:raise Exception('Failed to Update DNS Record: '+str(B))
        return True
