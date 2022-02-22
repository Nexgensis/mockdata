"""This file is written to update the schema for json file """
import json
import datetime
from utility_codes.constants_declearation import FAILED
from traceback import print_exc
import math


# This file check for any none value or empty string for the column distrubution along with number of columns and uniquevalue
def validate_schema_json(schemaJsonPath):
 try: 


    def arrange_schema_json_by_relation(wholeSchemaDict):
      
        schema=[]  
        for table in wholeSchemaDict['schema']: 
                flag=False
                for col_details in table['columndetails']:
                        if "fk" in col_details:
                                if col_details["fk"]==True: 
                                        col_details["entity"]="integer"
                                        flag=True
                        else:
                                col_details["fk"]=False
                        if not "pk" in col_details:
                                col_details["pk"]=False
                                        
                if flag:
                        schema.append(table)
                else:
                        schema=[table]+schema


        wholeSchemaDict['schema']=schema

        return wholeSchemaDict

    with open(schemaJsonPath) as f:
        wholeSchemaDict = json.load(f)
    
    wholeSchemaDict=arrange_schema_json_by_relation(wholeSchemaDict)
   
            
    # logic for validating schema dict
    parent_table={}
    query_list={}
    pk_col={}
    for table in wholeSchemaDict['schema']:
        query_string="CREATE TABLE IF NOT EXISTS "+str(table["schemaName"])+"("
        #Logic for schema validation begins from here
        pk_string=''
        lastcol=','
        index=1
        for col_details in table['columndetails']:
               
                if(len(table['columndetails']) ==index):
                       lastcol=''

                #check for primary key and foreign key
                if "pk" in col_details:
                        if col_details["pk"]:
                        
                                query_string=query_string+str(col_details["columnName"])+" INT NOT NULL"+str(lastcol)
                                pk_string=pk_string+",primary key"+"("+str(col_details["columnName"])+")"
               
                if "fk" in col_details:
                        if "fk" in col_details:
                                if col_details["fk"]:
                                        #Forein key statement creation

                                
                                        query_string=query_string+str(col_details["columnName"])+" INT"
                                        query_string=query_string+" "+",FOREIGN KEY ("+str(col_details["columnName"])+")"\
                                                +"REFERENCES"+" "+str(col_details["parent_table"])+"("+str(pk_col[col_details["parent_table"]])+")"+str(lastcol)


                # if unique value not specified then take unique values as 20% of total values
                if col_details["uniqueValuesCount"] is None or (isinstance(col_details["uniqueValuesCount"], str) and len(col_details["uniqueValuesCount"]) == 0):
                        col_details["uniqueValuesCount"] = str(int((table['numberOfRecords'])*(20/100)))

                
                # chek for integer and long data type
                if (col_details['dataType'].lower() == 'integer' and col_details['entity'].lower() == 'integer') \
                        or (col_details['dataType'].lower() == 'long' and col_details['entity'].lower() == 'long'):

                        #UPDATE QUERY
                        if "fk" in col_details and "pk" in col_details:
                                if (not col_details["pk"]) and (not col_details["fk"]):
                                        query_string=query_string+str(col_details["columnName"])+" INT"+str(lastcol)
                      
                        
                        if "distributions" in col_details:
                                
                                if not col_details['isDistribution']:
                                        distrubution={}
                                        distrubution['minimum'] = "1"
                                        distrubution['maximum'] = str(table['numberOfRecords'])
                                        distrubution['stepSize'] = "1"
                                        col_details["distributions"]=[distrubution]
                                        col_details['isDistribution']=True
                                   
                                else:
                                        for param in col_details["distributions"]:
                        
                                                if param['minimum'] is None or (isinstance(param['minimum'], str) and len(param['minimum']) == 0):
                                                        param['minimum'] = "1"
                                                if param['maximum'] is None or (isinstance(param['maximum'], str) and len(param['maximum']) == 0):
                                                        param['maximum'] = str(table['numberOfRecords'])
                                                if param['stepSize'] is None or (isinstance(param['stepSize'], str) and len(param['stepSize']) == 0):
                                                       param['stepSize'] = "1"
                
                elif (col_details['dataType'].lower() == 'integer' and col_details['entity'].lower() == 'numericid'): 

                        if "pk" in col_details:
                                if col_details["pk"]:
                                        parent_table[table["schemaName"]]=int(table['numberOfRecords'])
                                        pk_col[table["schemaName"]]=col_details["columnName"]


                
                # chek for float and double data type
                elif (col_details['dataType'].lower() == 'float' and col_details['entity'].lower() == 'float') or \
                        (col_details['dataType'].lower() == 'double' and col_details['entity'].lower() == 'double'):

                        if "fk" in col_details and "pk" in col_details:
                                #UPDATE QUERY
                                if (not col_details["pk"]) and (not col_details["fk"]):
                                        query_string=query_string+str(col_details["columnName"])+" FLOAT"+str(lastcol)

                        if "distributions" in col_details:
                                
                                if not col_details["isDistribution"]:
                                        distrubution={}
                                        distrubution['minimum'] = "1.23"
                                        distrubution['maximum'] = str(float(table['numberOfRecords']))
                                        distrubution['stepSize'] = "0.234"
                                        col_details["distributions"]=[distrubution]
                                        col_details['isDistribution']=True

                                else:
                                        for param in col_details["distributions"]:
                                                if param['minimum'] is None or (isinstance(param['minimum'], str) and len(param['minimum']) == 0):
                                                        param['minimum'] = "1.23"
                                                if param['maximum'] is None or (isinstance(param['maximum'], str) and len(param['maximum']) == 0):
                                                        param['maximum'] = str(float(table['numberOfRecords']))
                                                if param['stepSize'] is None or (isinstance(param['stepSize'], str) and len(param['stepSize']) == 0):
                                                        param['stepSize'] = "0.234"

                # chek for string and alphanumeric data type
                elif (col_details['dataType'].lower() == 'string' and col_details['entity'].lower() == 'string') \
                        or (col_details['dataType'].lower() == 'string' and col_details['entity'].lower() == 'alphanumeric'):

                        if "fk" in col_details and "pk" in col_details:
                                if (not col_details["pk"]) and (not col_details["fk"]):
                                        query_string=query_string+str(col_details["columnName"])+" VARCHAR(255)"+str(lastcol)


                        if "distributions" in col_details:  
                                if not col_details["isDistribution"]:
                                                distrubution={}
                                                distrubution['minChars'] = "3"
                                                distrubution['maxChars'] = "6"
                                                col_details["distributions"]=[distrubution]
                                                col_details['isDistribution']=True

                                else:
                                        for param in col_details["distributions"]:
                                                if param['minChars'] is None or (isinstance(param['minChars'], str) and len(param['minChars']) == 0):
                                                        param['minChars'] = "3"
                                                if param['maxChars'] is None or (isinstance(param['maxChars'], str) and len(param['maxChars']) == 0):
                                                        param['maxChars'] = "6"

                # chek for date type
                elif col_details['dataType'].lower() == 'date' and col_details['entity'].lower() == 'date':
                        
                        if "fk" in col_details and "pk" in col_details:

                                #query update
                        
                                if (not col_details["pk"]) and (not col_details["fk"]):
                                        query_string=query_string+str(col_details["columnName"])+" DATE"+str(lastcol)


                        if "distributions" in col_details:
                                if not col_details["isDistribution"]:
                                                distrubution={}
                                                distrubution['minimum'] = str(datetime.datetime.now() - datetime.timedelta(days=365)).split(" ")[0]
                                                distrubution['maximum'] = str(datetime.datetime.now()).split(" ")[0]
                                                distrubution['dateFormat'] = "yyyy-MM-dd"
                                                distrubution['stepSize'] = "1"
                                                col_details["distributions"]=[distrubution]
                                                col_details['isDistribution']=True

                                else:
                                        for param in col_details["distributions"]:
                                                if param['minimum'] is None or (isinstance(param['minimum'], str) and len(param['minimum']) == 0):
                                                        param['minimum'] = str(datetime.datetime.now() - datetime.timedelta(days=365)).split(" ")[0]
                                                if param['maximum'] is None or (isinstance(param['maximum'], str) and len(param['maximum']) == 0):
                                                        param['maximum'] = str(datetime.datetime.now()).split(" ")[0]
                                                if param['stepSize'] is None or (isinstance(param['stepSize'], str) and len(param['stepSize']) == 0):
                                                        param['stepSize'] = "1"

                                                if param['dateFormat'] is None or (isinstance(param['dateFormat'], str) and len(param['dateFormat']) == 0):
                                                        param['dateFormat'] = "yyyy-MM-dd"
                # chek for timestamp
                elif col_details['dataType'].lower() == 'timestamp' and col_details['entity'].lower() == 'timestamp':

                        if "fk" in col_details and "pk" in col_details:
                                #query update
                        
                                if (not col_details["pk"]) and (not col_details["fk"]):
                                        query_string=query_string+str(col_details["columnName"])+" TIMESTAMP"+str(lastcol)

                        if "distributions" in col_details:
                                if not col_details["isDistribution"]:
                                                distrubution={}
                                                distrubution['minimum'] = str(datetime.datetime.now() - datetime.timedelta(days=365))
                                                distrubution['maxChars'] = str(datetime.datetime.now())
                                                distrubution['dateFormat'] = "yyyy-MM-dd hh:mm:ss"
                                                distrubution['stepSize'] = "1"
                                                col_details["distributions"]=[distrubution]
                                                col_details['isDistribution']=True
                                else:

                                        for param in col_details["distributions"]:
                                                if param['minimum'] is None or (isinstance(param['minimum'], str) and len(param['minimum']) == 0):
                                                        param['minimum'] = str(datetime.datetime.now() - datetime.timedelta(days=365))
                                                if param['maximum'] is None or (isinstance(param['maximum'], str) and len(param['maximum']) == 0):
                                                        param['maxChars'] = str(datetime.datetime.now())
                                                if param['stepSize'] is None or (isinstance(param['stepSize'], str) and len(param['stepSize']) == 0):
                                                        param['stepSize'] = "1"

                                                if param['dateFormat'] is None or (isinstance(param['dateFormat'], str) and len(param['dateFormat']) == 0):
                                                        param['dateFormat'] = "yyyy-MM-dd hh:mm:ss"

                # check for timeseries

                elif col_details['dataType'].lower() == 'datetime' and col_details['entity'].lower() == 'timeseries':
                        
                        #datetime query update with fk
                        if "fk" in col_details and "pk" in col_details:

                                if (not col_details["pk"]) and (not col_details["fk"]):
                                        query_string=query_string+str(col_details["columnName"])+" DATETIME"+str(lastcol)
                              
                       
                        if "transactionDate" in col_details:
                                if col_details["transactionDate"][0].split("-")[0]==col_details["transactionDate"][1].split("-")[0] and col_details["frequency"]=="Y":
                                       
                                        to_date = col_details["transactionDate"][1]
                                        col_details["transactionDate"][1]=str(int(to_date.split("-")[0])+1)+"-"+to_date.split("-")[1]+"-"+to_date.split("-")[2]

                                elif col_details["transactionDate"][0].split("-")[0]==col_details["transactionDate"][1].split("-")[0] and \
                                        col_details["transactionDate"][0].split("-")[1]==col_details["transactionDate"][1].split("-")[1] and col_details["frequency"]=="M":
                                        if col_details["transactionDate"][1].split("-")[1]!='12':
                                               
                                                to_date = col_details["transactionDate"][1]
                                                col_details["transactionDate"][1]=to_date.split("-")[0]+"-"+str(int(to_date.split("-")[1])+1)+"-"+to_date.split("-")[2]
                                        else:
                                                to_date = col_details["transactionDate"][1]
                                                col_details["transactionDate"][1]=str(int(to_date.split("-")[0])+1)+"-"+"01"+"-"+to_date.split("-")[2]

                
                elif col_details['dataType'].lower() == 'integer' and col_details['entity'].lower() == 'numericid':

                        if "fk" in col_details and "pk" in col_details:
                                if (not col_details["pk"]):
                                        query_string=query_string+str(col_details["columnName"])+" INT"+str(lastcol)

                
                elif col_details['dataType'].lower() == 'boolean' and col_details['entity'].lower() == 'boolean':

                        if "fk" in col_details and "pk" in col_details:
                                if (not col_details["pk"]):
                                        query_string=query_string+str(col_details["columnName"])+" boolean"+str(lastcol)

                

                else:
                     print("$$$$$$$$$$4")
                     if col_details['dataType'].lower()=='string' or col_details['dataType'].lower()=='alphanumeric':
                             query_string=query_string+str(col_details["columnName"])+" VARCHAR(255)"+str(lastcol)
                     elif col_details['dataType'].lower()=='long':
                             query_string=query_string+str(col_details["columnName"])+" BIGINT"+str(lastcol)



                
                if "fk" in col_details:
                        if col_details["fk"]:
                                        for param in col_details["distributions"]:
                                                param['minimum'] = "1"
                                                param['maximum']=str(parent_table[col_details["parent_table"]])
                                                col_details['dataType']='integer'
                                                col_details['entity']='integer'
                                                col_details['stepSize']='1'
        
                index+=1


        query_list[table["schemaName"]]=query_string+pk_string+");"
                #check for foreign key status
  
    if wholeSchemaDict["connectioninfo"]["targetType"]=='SQL':       
        wholeSchemaDict["relational_table"]=query_list
    else:
            if "relational_table" in wholeSchemaDict:
                    del wholeSchemaDict["relational_table"]
 
    with open(schemaJsonPath, "w") as outfile:
        json.dump(wholeSchemaDict, outfile)
    
 except:
   print_exc()
   exit()






   