###########################################
#### Written By: SATYAKI DE            ####
#### Written On: 06-Feb-2021           ####
#### Package Flask package needs to    ####
#### install in order to run this      ####
#### script.                           ####
####                                   ####
#### Objective: Main Calling scripts.  ####
####                                   ####
#### However, to meet the functionality####
#### we've enhanced as per our logic.  ####
###########################################
 
import logging
import json
import requests
import os
import pandas as p
import numpy as np
 
import azure.functions as func
 
 
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Dynamic-Covid-Status HTTP trigger function processed a request.')
 
    try:
 
        # Application Variable
        url = os.environ['URL']
        appType = os.environ['appType']
        conType = os.environ['conType']
 
        # API-Configuration
        payload={}
        headers = {
            "Connection": conType,
            "Content-Type": appType
        }
 
        # Validating input parameters
        typeSel = req.params.get('typeSel')
        if not typeSel:
            try:
                req_body = req.get_json()
            except ValueError:
                pass
            else:
                typeSel = req_body.get('typeSel')
         
        typeVal = req.params.get('typeVal')
        if not typeVal:
            try:
                req_body = req.get_json()
            except ValueError:
                pass
            else:
                typeVal = req_body.get('typeVal')
 
        # Printing Key-Element Values
        str1 = 'typeSel: ' + str(typeSel)
        logging.info(str1)
 
        str2 = 'typeVal: ' + str(typeVal)
        logging.info(str2)
 
        # End of API-Inputs
 
        # Getting Covid data from the REST-API
        response = requests.request("GET", url, headers=headers, data=payload)
        ResJson  = response.text
 
        if typeSel == '*':
            if typeVal != '':
                # Converting it to Json
                jdata = json.loads(ResJson)
 
                df_ret = p.io.json.json_normalize(jdata)
                df_ret.columns = df_ret.columns.map(lambda x: x.split(".")[-1])
 
                rJson = df_ret.to_json(orient ='records') 
 
                return func.HttpResponse(rJson, status_code=200)
            else:
                x_stat = 'Failed'
                x_msg = 'Important information is missing for all values!'
 
                rJson = {
                    "status": x_stat,
                    "details": x_msg
                }
 
                xval = json.dumps(rJson)
                return func.HttpResponse(xval, status_code=200)
        elif typeSel == 'Cols':
            if typeVal != '':
                # Converting it to Json
                jdata = json.loads(ResJson)
 
                df_ret = p.io.json.json_normalize(jdata)
                df_ret.columns = df_ret.columns.map(lambda x: x.split(".")[-1])
 
                # Fetching for the selected columns
                # Extracting the columns from the list
                lstHead = []
 
                listX = typeVal.split (",")
 
                for i in listX:
                    lstHead.append(str(i).strip())
 
                str3 = 'Main List: ' + str(lstHead)
                logging.info(str3)
 
                slice_df = df_ret[np.intersect1d(df_ret.columns, lstHead)]
                rJson = slice_df.to_json(orient ='records') 
                 
                return func.HttpResponse(rJson, status_code=200)
            else:
                x_stat = 'Failed'
                x_msg = 'Important information is missing for selected values!'
 
                rJson = {
                    "status": x_stat,
                    "details": x_msg
                }
 
                xval = json.dumps(rJson)
                return func.HttpResponse(xval, status_code=200)
        else:
            x_stat = 'Failed'
            x_msg = 'Important information is missing for typeSel!'
 
            rJson = {
                "status": x_stat,
                "details": x_msg
            }
 
            xval = json.dumps(rJson)
            return func.HttpResponse(xval, status_code=200)
    except Exception as e:
        x_msg = str(e)
        x_stat = 'Failed'
 
        rJson = {
                    "status": x_stat,
                    "details": x_msg
                }
 
        xval = json.dumps(rJson)
        return func.HttpResponse(xval, status_code=200)
