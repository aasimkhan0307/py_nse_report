import subprocess
from datetime import datetime
import os
import json
import requests #ext
import pandas as pd #ext
import time
import matplotlib.pyplot as plt #ext
from datetime import datetime

from io import StringIO

# pip3 install pydrive google-auth
from pydrive.auth import GoogleAuth

# pip3 install pandas google-auth-oauthlib
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
import io


from datetime import date

from datetime import datetime

# FOLDER_ID ="1oyrdQy3dPd541VHFWx3ntKkCaauTCFZd"
FOLDER_ID = "1Uv4bRac0Y1AQ2xqYlZ2PQ4RCx2CihEC_"


# MODE="dev"
MODE ="prod"

SERVICE_ACCOUNT_CREDENTIALS = {
  "type": "service_account",
  "project_id": "pyreport-395001",
  "private_key_id": "f186b49e3dec8d0e7bfcf42366af4b3a292982dd",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC6jtdfRVk+TtM3\nica01EVNuRG+iCoizDusjwekHwCP01DiQZ2uNkUxuyAQnLoLJFDYRzfTZJzBgHKm\nSPEbK4HIfvhWUwW3ye7o2IAUm6aBeRN35AAoNMxRN2l3328GAaB1XtzXaTua6QUr\nSMrTVYRvUd/hV/15oQ12Z0msYrmhDOxXU/hyJGZWPgmAyTOULn7mVdgHnl1XWGvX\nPl97iBgL9D+zp37kNPlm0Afd+tkckUlVv2lyIdevOaFG2m3Zclmc5XYIE/VZqbkj\ntWPoMGm2TO6asTgj2mpj84P9l+EuxIlkQDMY7mpFJkfC+FBAg6H8E+A/pE/wRf93\ngqNvTydJAgMBAAECggEAFqqFM0+PLLlSA0TrjgveUbejFsWfbPPoaipEkOWtYUwY\n3yx+tmxkksq79hi8p97lLanVeAsY2o+7HkXteVIbZhs7G+3hW3ee3c50HUzd+YwW\n1/GtLTVg/5seZtCQQigPcUMxzMA93C/kj2I8NloFCatmapAQvWbaZmDdPCMQj6b3\nQw126S033AYgvBlVXslGWvRmsDVQaMjx8Y1/KwFmVMEAAqpMN/Q3efN4O07AUI5K\njHAalWuPcmwwRVPbQLg5NBNLjKF8UnvoPD6hgqIMHbVWVfAuOdfhxiktOMOnOqPR\ntwObSdioh2cfalUGdzIm+HRAI1qVhXjF7qd+bvVCSwKBgQD4gizGCfP1vCHmSecC\nMWmlJvAJ5+p8jVo/adH1pCUKNZfJGt2mjsEw0k4WQU1FNzqSwuu13Bn6LDJ5MsTE\nFK7y5Q+Jhgu0kGFPrBVGBVAj8GamvAwjNJJkMFSw86HLj/lT33qU12+ibmzD4GH+\n9pQggmG7zZNVhaUf3ElRG2eDOwKBgQDALpKxp6qo26IqYG+oMeUsbpADsxC5vSMj\ntvQ+OpX0MTVcCPfrtmu/ghphaD2kH0E7r6lHKDMEC4T43VgNQatnORDBccdoF2rz\na8xdLre9E3sOhSMCxM6J/GtSnWRxJNg3+8iVGCrOlqS4Mh0l5PJIDQDYJgnw8kNF\nD2s/sq3PSwKBgQC9WfLjsxHAW+k4n3b1pNDqlhCwe5Wf8dSiHO9uS/QtI0jUYzQg\nhQWPPa9iJb/KzZpD2WHg4CSrCqIq1xQ2k8v6J67/CrRAkQKUwnozMDkwKS0OPprA\n+H3S+UCO1BSkFr/TuKSeZMka6yjU11PUAe48FxyBDNMGhx6aeC8lLcxhoQKBgH33\nCNfBGi8LpsM36nfIHHp/DY4fNHtJ6VpjdLvFEry39E2a40VwkDc6Q6hM9vo4Mj24\n6a9mPeoM0t971Vb7ECncUYs51IX5s3RR1+XL1UrtFd9yjHwoG2h+Nqoz174BdGdd\ndR+kc4ptWxIQZuKLqfJn84G5jRKM+GKvtJBPwXT5AoGAWAQrUDn1vFiHMg17BfsH\nQYl03pYuzLInbiXD16MWhL2FeOUJTZIkmIer+pdAWc1pOgl/hdHChGmpBWwS+c4A\nk6+04jPbpO4tffY7iOPteyuyGBWiDyRZJjGOMpKYDwzfpbxC88OWKJAtYls+Yder\n5k2z4z4FDE7lIL1P7jYFs7s=\n-----END PRIVATE KEY-----\n",
  "client_email": "drive-service@pyreport-395001.iam.gserviceaccount.com",
  "client_id": "101013435219073984571",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/drive-service%40pyreport-395001.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}



credentials = Credentials.from_service_account_info(SERVICE_ACCOUNT_CREDENTIALS, scopes=['https://www.googleapis.com/auth/drive'])
curl_command = "curl -H 'user-agent:Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36' -H 'accept-encoding:gzip, deflate, br' -H 'accept-language:en-US,en;q=0.9' 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY' --compressed"

def _execute_curl(curl_command):
    try:
        completed_process = subprocess.run(curl_command, capture_output=True, text=True, shell=True)
        if completed_process.returncode == 0:
            response = completed_process.stdout.strip()
            return json.loads(response) if response else None
        else:
            print('Error:', completed_process.stderr)
            return None
    except Exception as e:
        print('Exception:', e)
        return None

def convert_get_to_curl(url, query_params=None, headers=None):
    curl_command = ['curl', url]

    if query_params:
        query_string = '&'.join([f"{key}={value}" for key, value in query_params.items()])
        curl_command.append(f"'{url}?{query_string}'")

    if headers:
        for key, value in headers.items():
            curl_command.extend(['-H', f"'{key}:{value}'"])

    curl_command.append("--compressed")
    return ' '.join(curl_command)


now = datetime.now()

current_time = now.strftime("%H:%M:%S")


ceoptiontype="CE"
peoptiontype="PE"

ceL=[]
peL=[]
xceL=[]
xpeL=[]
lot=50

xsum=[]


Nf_Add_list=[]
Bf_Add_list=[]
nloop=1



celisttemp=[]
pelisttemp=[]

lp1=0
while lp1<60:
    try:

        url='https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'

        headers= {

            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
            'accept-encoding' : 'gzip, deflate, br',
            'accept-language' : 'en-US,en;q=0.9'

        }

        # session =requests.session()
        # request=session.get(url,headers=headers)
        # cookies=dict(request.cookies)
        # response=session.get(url,headers=headers,cookies=cookies).json()

        curl_command = convert_get_to_curl(url,{},headers)
        response = _execute_curl(curl_command)

        rawdate=pd.DataFrame(response)
        rawop=pd.DataFrame(rawdate['filtered'],['data']).fillna(0)


        pe_values= rawop ['filtered']
        pe1_values= pe_values ['data']

        pe2_data=pd.DataFrame.from_dict(pe1_values, orient='columns')
        #print("st1")


        break
    except:
        lp1+=1
        # gap in run time
        time.sleep(2)



#df=pe2_data.sort_values(by=['expiryDate'])


#CURRENT DATA-------------------------------------------------------------------------------

#df2=df[df["expiryDate"]==expiryDate]




    #collection of CE DATA

CeDataRaw=pe2_data["CE"]
CeDataRaw=CeDataRaw.fillna(0)
CeDataRaw=CeDataRaw.items()
CeDataRaw = list (CeDataRaw)
CeDataRaw1 = pd.DataFrame(CeDataRaw)
sortlength = len(CeDataRaw1)
k = 0
while k < sortlength:
    if CeDataRaw1.iloc[k,1]!=0 :
        celisttemp.append(CeDataRaw1.iloc[k,1])



    k+=1
CeDataList=celisttemp
CedataFinal=pd.DataFrame.from_dict(CeDataList, orient='columns')
CedataFinal1=CedataFinal.sort_values(by=['strikePrice']).copy()





#collection of PE DATA

PeDataRaw=pe2_data["PE"]
PeDataRaw=PeDataRaw.dropna()
PeDataRaw=PeDataRaw.items()
PeDataRaw = list (PeDataRaw)
PeDataRaw1 = pd.DataFrame(PeDataRaw)
sortlength = len(PeDataRaw1)
j = 0
while j < sortlength:
    if PeDataRaw1.iloc[j,1]!=0 :
        pelisttemp.append(PeDataRaw1.iloc[j,1])
    j+=1
PeDataList=pelisttemp
PedataFinal=pd.DataFrame.from_dict(PeDataList, orient='columns')
PedataFinal1=PedataFinal.sort_values(by=['strikePrice']).copy()




#data analysis

cetotaloi=(CedataFinal1['openInterest'].sum())/10000000
cetotalvol=(CedataFinal1['totalTradedVolume'].sum())/10000000
cetradequantity=cetotalvol/cetotaloi

CedataFinal1['amount']=CedataFinal1['openInterest']*CedataFinal1['lastPrice']*lot

CedataFinal1['oldOi']=(CedataFinal1['changeinOpenInterest']*100)/CedataFinal1['changeinOpenInterest']
CedataFinal1['oldLtp']=(CedataFinal1['change']*100)/CedataFinal1['pChange']
CedataFinal1['oldAmount']=CedataFinal1['oldLtp']*CedataFinal1['oldOi']
CedataFinal1['ValueChange']=CedataFinal1['amount']-CedataFinal1['oldAmount']
CedataFinal1=(CedataFinal1.fillna(0)).copy()


CedataFinal2=CedataFinal1.sort_values(by=['ValueChange']).copy()







petotaloi=(PedataFinal1['openInterest'].sum())/10000000
petotalvol=(PedataFinal1['totalTradedVolume'].sum())/10000000
petradequantity=petotalvol/petotaloi

PedataFinal1['amount']=PedataFinal1['openInterest']*PedataFinal1['lastPrice']*lot

PedataFinal1['oldOi']=(PedataFinal1['changeinOpenInterest']*100)/PedataFinal1['changeinOpenInterest']
PedataFinal1['oldLtp']=(PedataFinal1['change']*100)/PedataFinal1['pChange']
PedataFinal1['oldAmount']=PedataFinal1['oldLtp']*PedataFinal1['oldOi']
PedataFinal1['ValueChange']=PedataFinal1['amount']-PedataFinal1['oldAmount']
PedataFinal1=(PedataFinal1.fillna(0)).copy()


PedataFinal2=PedataFinal1.sort_values(by=['ValueChange']).copy()










#calculation for ATM strike Price
celistlength=len(CedataFinal1)-1
x=CedataFinal1.iloc[celistlength,18]

zz=round(celistlength/2)
zz1=zz-1
strikediff=CedataFinal1.iloc[zz,0]-CedataFinal1.iloc[zz1,0]




x=round(x / strikediff)

y=x*strikediff


k=0
for k in range (celistlength):
    if y==CedataFinal1.iloc[k,0]:
        atmstrike=k
        break



ceatmstrikeprice=CedataFinal1.iloc[k,0]
ceatmchanval=round((CedataFinal1.iloc[k,23]/10000000),3)
ceatmltp=round((CedataFinal1.iloc[k,9]),2)
ceatmimp=round((CedataFinal1.iloc[k,8]),2)
ceatmoi=round((CedataFinal1.iloc[k,4]),3)



pelistlength=len(PedataFinal1)-1
x=PedataFinal1.iloc[pelistlength,18]
x=round(x / strikediff)

y=x*strikediff



k=0
for k in range (pelistlength):
    if y==PedataFinal1.iloc[k,0]:
        atmstrike=k
        break



peatmstrikeprice=PedataFinal1.iloc[k,0]
peatmchanval=round((PedataFinal1.iloc[k,23]/10000000),3)
peatmltp=round((PedataFinal1.iloc[k,9]),2)
peatmimp=round((PedataFinal1.iloc[k,8]),2)
peatmoi=round((PedataFinal1.iloc[k,4]),3)


newatm=y-strikediff*3
xnewatm=y+strikediff*4
xsum=[]
xbottomdetail=[]
xnonzero=[]
ii=-3


while newatm < (xnewatm):

    ceL=[]
    peL=[]
    xceL=[]
    xpeL=[]





    #_______________________________________________________________
    #_______________________________________________________________

    atm=newatm

    a=atm
    b=atm+strikediff
    c=atm+strikediff*2
    d=atm+strikediff*3
    e=atm+strikediff*4

    xa=atm
    xb=atm-strikediff
    xc=atm-strikediff*2
    xd=atm-strikediff*3
    xe=atm-strikediff*4



    #_______________________________________________________________
    #_______________________________________________________________


    #-----------------new price------ce--------------
    #-----1-----
    k=0
    for k in range (celistlength):
        if a==CedataFinal1.iloc[k,0]:
            ce_a_ltp=round((CedataFinal1.iloc[k,9]),2)
            ce_a_oi=round((CedataFinal1.iloc[k,4]),3)

            break

    k=0
    for k in range (pelistlength):
        if a==PedataFinal1.iloc[k,0]:
            pe_a_ltp=round((PedataFinal1.iloc[k,9]),2)
            pe_a_oi=round((PedataFinal1.iloc[k,4]),3)

            break

    #-----2-----

    k=0
    for k in range (celistlength):
        if b==CedataFinal1.iloc[k,0]:
            ce_b_ltp=round((CedataFinal1.iloc[k,9]),2)
            ce_b_oi=round((CedataFinal1.iloc[k,4]),3)

            break
    k=0
    for k in range (pelistlength):
        if b==PedataFinal1.iloc[k,0]:
            pe_b_ltp=round((PedataFinal1.iloc[k,9]),2)
            pe_b_oi=round((PedataFinal1.iloc[k,4]),3)

            break


    #-----3------

    k=0
    for k in range (celistlength):
        if c==CedataFinal1.iloc[k,0]:
            ce_c_ltp=round((CedataFinal1.iloc[k,9]),2)
            ce_c_oi=round((CedataFinal1.iloc[k,4]),3)

            break

    k=0
    for k in range (pelistlength):
        if c==PedataFinal1.iloc[k,0]:
            pe_c_ltp=round((PedataFinal1.iloc[k,9]),2)
            pe_c_oi=round((PedataFinal1.iloc[k,4]),3)

            break



    #-----4------

    k=0
    for k in range (celistlength):
        if d==CedataFinal1.iloc[k,0]:
            ce_d_ltp=round((CedataFinal1.iloc[k,9]),2)
            ce_d_oi=round((CedataFinal1.iloc[k,4]),3)

            break

    k=0
    for k in range (pelistlength):
        if d==PedataFinal1.iloc[k,0]:
            pe_d_ltp=round((PedataFinal1.iloc[k,9]),2)
            pe_d_oi=round((PedataFinal1.iloc[k,4]),3)

            break



    #-----5------


    k=0
    for k in range (celistlength):
        if e==CedataFinal1.iloc[k,0]:
            ce_e_ltp=round((CedataFinal1.iloc[k,9]),2)
            ce_e_oi=round((CedataFinal1.iloc[k,4]),3)

            break


    k=0
    for k in range (pelistlength):
        if e==PedataFinal1.iloc[k,0]:
            pe_e_ltp=round((PedataFinal1.iloc[k,9]),2)
            pe_e_oi=round((PedataFinal1.iloc[k,4]),3)

            break

    #-----end------


    #-----pe----------------------------

    #-------x1-----------------
    k=0
    for k in range (celistlength):
        if xa==CedataFinal1.iloc[k,0]:
            xce_a_ltp=round((CedataFinal1.iloc[k,9]),2)
            xce_a_oi=round((CedataFinal1.iloc[k,4]),3)

            break



    k=0
    for k in range (pelistlength):
        if xa==PedataFinal1.iloc[k,0]:
            xpe_a_ltp=round((PedataFinal1.iloc[k,9]),2)
            xpe_a_oi=round((PedataFinal1.iloc[k,4]),3)

            break


    #-------x2-----------------

    k=0
    for k in range (celistlength):
        if xb==CedataFinal1.iloc[k,0]:
            xce_b_ltp=round((CedataFinal1.iloc[k,9]),2)
            xce_b_oi=round((CedataFinal1.iloc[k,4]),3)

            break





    k=0
    for k in range (pelistlength):
        if xb==PedataFinal1.iloc[k,0]:
            xpe_b_ltp=round((PedataFinal1.iloc[k,9]),2)
            xpe_b_oi=round((PedataFinal1.iloc[k,4]),3)

            break

    #-------x3-----------------

    k=0
    for k in range (celistlength):
        if xc==CedataFinal1.iloc[k,0]:
            xce_c_ltp=round((CedataFinal1.iloc[k,9]),2)
            xce_c_oi=round((CedataFinal1.iloc[k,4]),3)

            break




    k=0
    for k in range (pelistlength):
        if xc==PedataFinal1.iloc[k,0]:
            xpe_c_ltp=round((PedataFinal1.iloc[k,9]),2)
            xpe_c_oi=round((PedataFinal1.iloc[k,4]),3)

            break



    #-------x4-----------------
    k=0
    for k in range (celistlength):
        if xd==CedataFinal1.iloc[k,0]:
            xce_d_ltp=round((CedataFinal1.iloc[k,9]),2)
            xce_d_oi=round((CedataFinal1.iloc[k,4]),3)

            break




    k=0
    for k in range (pelistlength):
        if xd==PedataFinal1.iloc[k,0]:
            xpe_d_ltp=round((PedataFinal1.iloc[k,9]),2)
            xpe_d_oi=round((PedataFinal1.iloc[k,4]),3)

            break


    #-------x5-----------------

    k=0
    for k in range (celistlength):
        if xe==CedataFinal1.iloc[k,0]:
            xce_e_ltp=round((CedataFinal1.iloc[k,9]),2)
            xce_e_oi=round((CedataFinal1.iloc[k,4]),3)

            break



    k=0
    for k in range (pelistlength):
        if xe==PedataFinal1.iloc[k,0]:
            xpe_e_ltp=round((PedataFinal1.iloc[k,9]),2)
            xpe_e_oi=round((PedataFinal1.iloc[k,4]),3)

            break


    #-------x--end-----------------



    amt_a=(((ce_a_ltp)*(ce_a_oi) + (pe_a_ltp)*(pe_a_oi) )*lot) /10000000
    amt_b=(((ce_b_ltp)*(ce_b_oi) + (pe_b_ltp)*(pe_b_oi) )*lot) /10000000
    amt_c=(((ce_c_ltp)*(ce_c_oi) + (pe_c_ltp)*(pe_c_oi) )*lot) /10000000
    amt_d=(((ce_d_ltp)*(ce_d_oi) + (pe_d_ltp)*(pe_d_oi) )*lot) /10000000
    amt_e=(((ce_e_ltp)*(ce_e_oi) + (pe_e_ltp)*(pe_e_oi) )*lot) /10000000



    amt_cez=(((ce_a_ltp)*(ce_a_oi)+(ce_b_ltp)*(ce_b_oi)+(ce_c_ltp)*(ce_c_oi)+(ce_d_ltp)*(ce_d_oi)+(ce_e_ltp)*(ce_e_oi))*lot)/10000000


    amt_xa=(((xce_a_ltp)*(xce_a_oi) + (xpe_a_ltp)*(xpe_a_oi) )*lot) /10000000
    amt_xb=(((xce_b_ltp)*(xce_b_oi) + (xpe_b_ltp)*(xpe_b_oi) )*lot) /10000000
    amt_xc=(((xce_c_ltp)*(xce_c_oi) + (xpe_c_ltp)*(xpe_c_oi) )*lot) /10000000
    amt_xd=(((xce_d_ltp)*(xce_d_oi) + (xpe_d_ltp)*(xpe_d_oi) )*lot) /10000000
    amt_xe=(((xce_e_ltp)*(xce_e_oi) + (xpe_e_ltp)*(xpe_e_oi) )*lot) /10000000




    amt_pez= (((xpe_a_ltp)*(xpe_a_oi)+(xpe_b_ltp)*(xpe_b_oi)+(xpe_c_ltp)*(xpe_c_oi)+(xpe_d_ltp)*(xpe_d_oi)+(xpe_e_ltp)*(xpe_e_oi))*lot)/10000000

    #---------calculation---part 1-----
    atm_xyce_a=(((xce_a_ltp)*(xce_a_oi))*lot)  /10000000
    atm_xype_a=(((xpe_a_ltp)*(xpe_a_oi))*lot)  /10000000

    atm_xyce_b=(((xce_b_ltp)*(xce_b_oi))*lot)  /10000000
    atm_xype_b=(((xpe_b_ltp)*(xpe_b_oi))*lot)  /10000000

    atm_xyce_c=(((xce_c_ltp)*(xce_c_oi))*lot)  /10000000
    atm_xype_c=(((xpe_c_ltp)*(xpe_c_oi))*lot)  /10000000

    atm_xyce_d=(((xce_d_ltp)*(xce_d_oi))*lot)  /10000000
    atm_xype_d=(((xpe_d_ltp)*(xpe_d_oi))*lot)  /10000000

    atm_xyce_e=(((xce_e_ltp)*(xce_e_oi))*lot)  /10000000
    atm_xype_e=(((xpe_e_ltp)*(xpe_e_oi))*lot)  /10000000


    #---------------------part 2------------
    atm_yce_a=(((ce_a_ltp)*(ce_a_oi))*lot)  /10000000
    atm_ype_a=(((pe_a_ltp)*(pe_a_oi))*lot)  /10000000

    atm_yce_b=(((ce_b_ltp)*(ce_b_oi))*lot)  /10000000
    atm_ype_b=(((pe_b_ltp)*(pe_b_oi))*lot)  /10000000

    atm_yce_c=(((ce_c_ltp)*(ce_c_oi))*lot)  /10000000
    atm_ype_c=(((pe_c_ltp)*(pe_c_oi))*lot)  /10000000

    atm_yce_d=(((ce_d_ltp)*(ce_d_oi))*lot)  /10000000
    atm_ype_d=(((pe_d_ltp)*(pe_d_oi))*lot)  /10000000

    atm_yce_e=(((ce_e_ltp)*(ce_e_oi))*lot)  /10000000
    atm_ype_e=(((pe_e_ltp)*(pe_e_oi))*lot)  /10000000


    Totalamt=atm_ype_b+atm_ype_c+atm_ype_d+atm_ype_e
    xTotalamt=atm_xyce_b+atm_xyce_c+atm_xyce_d+atm_xyce_e



    #-------------------calculation for other range---------------------------

    k=0
    for k in range (celistlength):
        if e<CedataFinal1.iloc[k,0]:
            yce_a_ltp=round((CedataFinal1.iloc[k,9]),2)
            yce_a_oi=round((CedataFinal1.iloc[k,4]),3)

            ceL.append([CedataFinal1.iloc[k,0],yce_a_ltp,yce_a_oi])


    k=0
    for k in range (pelistlength):
        if e<PedataFinal1.iloc[k,0]:
            ype_a_ltp=round((PedataFinal1.iloc[k,9]),2)
            ype_a_oi=round((PedataFinal1.iloc[k,4]),3)

            peL.append([PedataFinal1.iloc[k,0],ype_a_ltp,ype_a_oi])

    #-----------------

    k=0
    for k in range (celistlength):
        if xe>CedataFinal1.iloc[k,0]:
            zce_a_ltp=round((CedataFinal1.iloc[k,9]),2)
            zce_a_oi=round((CedataFinal1.iloc[k,4]),3)

            xceL.append([CedataFinal1.iloc[k,0],zce_a_ltp,zce_a_oi])


    k=0
    for k in range (pelistlength):
        if xe>PedataFinal1.iloc[k,0]:
            zpe_a_ltp=round((PedataFinal1.iloc[k,9]),2)
            zpe_a_oi=round((PedataFinal1.iloc[k,4]),3)

            xpeL.append([PedataFinal1.iloc[k,0],zpe_a_ltp,zpe_a_oi])


    #------calculation for range sum

    dfce = pd.DataFrame(ceL, columns = ['stk','ltp','0i'])
    dfpe = pd.DataFrame(peL, columns = ['stk','ltp','0i'])

    xdfce = pd.DataFrame(xceL, columns = ['stk','ltp','0i'])
    xdfpe = pd.DataFrame(xpeL, columns = ['stk','ltp','0i'])


    dfce['sum']=dfce['ltp']*dfce['0i']
    dfpe['sum']=dfpe['ltp']*dfpe['0i']

    xdfce['sum']=xdfce['ltp']*xdfce['0i']
    xdfpe['sum']=xdfpe['ltp']*xdfpe['0i']

    topsum=((dfce['sum'].sum()+dfpe['sum'].sum())*lot)/10000000


    bottomsum=((xdfce['sum'].sum()+xdfpe['sum'].sum())*lot)/10000000

    topsumce=((dfce['sum'].sum())*lot)/10000000
    topsumpe=((dfpe['sum'].sum())*lot)/10000000


    bottomsumce=((xdfce['sum'].sum())*lot)/10000000
    bottomsumpe=((xdfpe['sum'].sum())*lot)/10000000


    #--------------------------------------------

    #other calculation

    pcr_tt=round( petotaloi/cetotaloi  ,3)

    celenght=len(CedataFinal2)-1
    pelenght=len(PedataFinal2)-1

    cehighChanval=round((CedataFinal2.iloc[celenght,23]/10000000),3)
    cehighstrikeval=CedataFinal2.iloc[celenght,0]
    cehighltp=round((CedataFinal2.iloc[celenght,9]),2)
    cehighimp=round((CedataFinal2.iloc[celenght,8]),2)
    cechoi=round((CedataFinal2.iloc[celenght,4]),3)



    pehighChanval=round((PedataFinal2.iloc[pelenght,23]/10000000),3)
    pehighstrikeval=PedataFinal2.iloc[pelenght,0]
    pehighltp=round((PedataFinal2.iloc[pelenght,9]),2)
    pehighimp=round((PedataFinal2.iloc[pelenght,8]),2)
    pechoi=round((PedataFinal2.iloc[pelenght,4]),3)


    underlyval=CedataFinal2.iloc[celenght,18]
    atmpcr=round(peatmoi/ceatmoi  ,3)
    pcr_ch=round(cechoi/pechoi  ,3)



    TotalAmount=(CedataFinal1['amount'].sum()+PedataFinal1['amount'].sum())
    amountincr=TotalAmount/10000000
    ceper=(CedataFinal1['amount'].sum()/TotalAmount)*100
    peper=(PedataFinal1['amount'].sum()/TotalAmount)*100


    #print ("------",newatm,"---",ii,"---")
    #print(round(atm_xyce_a,2),"-",xa,"-",round(atm_xype_a,2),"-",round(atm_xyce_a+atm_xype_a,2))
    #print(round(amt_cez,2),"--z--",round(amt_pez,2),"-T-",round((amt_cez+amt_pez ),2))
    #print(round((xTotalamt),2),"-nz--",round((Totalamt),2),"-T-",round((xTotalamt+Totalamt),2))
    #print("TT-", round(((Totalamt+xTotalamt)+(amt_cez+amt_pez)),2))
    #print("\n")
    xsum.append((round((amt_cez+amt_pez ),2),round(atm_xyce_a,2),round(atm_xype_a,2)))
    xnonzero.append ((round((xTotalamt),2),round((Totalamt),2),round((amt_cez),2),round((amt_pez),2)))
    xbottomdetail.append((round(bottomsumce,2),round(bottomsumpe,2),round(bottomsum,2),round(topsumce,2),round(topsumpe,2),round(topsum,2)))




    ii=ii+1
    newatm=newatm+strikediff


xsum = pd.DataFrame(xsum, columns = ['sum','atmce','atmpe'])
botsum=xsum.iloc[0,0]+xsum.iloc[1,0]+xsum.iloc[2,0]
topsum=xsum.iloc[4,0]+xsum.iloc[5,0]+xsum.iloc[6,0]
atsum=xsum.iloc[3,0]

xnonzero= pd.DataFrame(xnonzero, columns = ['cenonzero','penonzero','cezero','pezero'])

xbottomdetail = pd.DataFrame(xbottomdetail, columns = ['bce','bpe','bsum','tce','tpe','tsum'])

#data storage

Nf_Add_list.append((current_time,underlyval,round((xbottomdetail.iloc[3,2]/amountincr),2),round((xbottomdetail.iloc[3,5]/amountincr),2),round(amountincr,2),round(((botsum/3)/amountincr),2),round((atsum/amountincr),2),round(((topsum/3)/amountincr),2)))



#------------------------------bk-----------------------------bk-----------------------------------------------------------
#------------------------------bk-----------------------------bk-----------------------------------------------------------
#------------------------------bk-----------------------------bk-----------------------------------------------------------

#------------------------------bk-----------------------------bk-----------------------------------------------------------

ceL=[]
peL=[]
xceL=[]
xpeL=[]
lot=15

xsum=[]


celisttemp=[]
pelisttemp=[]

lp2=0
while lp2<60:
    try:
        url='https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY'

        headers= {

            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
            'accept-encoding' : 'gzip, deflate, br',
            'accept-language' : 'en-US,en;q=0.9'

        }

        # session =requests.session()
        # request=session.get(url,headers=headers)
        # cookies=dict(request.cookies)
        # response=session.get(url,headers=headers,cookies=cookies).json()

        curl_command = convert_get_to_curl(url,{},headers)
        response = _execute_curl(curl_command)

        rawdate=pd.DataFrame(response)
        rawop=pd.DataFrame(rawdate['filtered'],['data']).fillna(0)


        pe_values= rawop ['filtered']
        pe1_values= pe_values ['data']

        pe2_data=pd.DataFrame.from_dict(pe1_values, orient='columns')
        #print("st2")

        break
    except:
        lp2+=1
        # gap in run time
        time.sleep(2)



#df=pe2_data.sort_values(by=['expiryDate'])


#CURRENT DATA-------------------------------------------------------------------------------

#df2=df[df["expiryDate"]==expiryDate]




    #collection of CE DATA

CeDataRaw=pe2_data["CE"]
CeDataRaw=CeDataRaw.fillna(0)
CeDataRaw=CeDataRaw.items()
CeDataRaw = list (CeDataRaw)
CeDataRaw1 = pd.DataFrame(CeDataRaw)
sortlength = len(CeDataRaw1)
k = 0
while k < sortlength:
    if CeDataRaw1.iloc[k,1]!=0 :
        celisttemp.append(CeDataRaw1.iloc[k,1])



    k+=1
CeDataList=celisttemp
CedataFinal=pd.DataFrame.from_dict(CeDataList, orient='columns')
CedataFinal1=CedataFinal.sort_values(by=['strikePrice']).copy()





#collection of PE DATA

PeDataRaw=pe2_data["PE"]
PeDataRaw=PeDataRaw.dropna()
PeDataRaw=PeDataRaw.items()
PeDataRaw = list (PeDataRaw)
PeDataRaw1 = pd.DataFrame(PeDataRaw)
sortlength = len(PeDataRaw1)
j = 0
while j < sortlength:
    if PeDataRaw1.iloc[j,1]!=0 :
        pelisttemp.append(PeDataRaw1.iloc[j,1])
    j+=1
PeDataList=pelisttemp
PedataFinal=pd.DataFrame.from_dict(PeDataList, orient='columns')
PedataFinal1=PedataFinal.sort_values(by=['strikePrice']).copy()




#data analysis

cetotaloi=(CedataFinal1['openInterest'].sum())/10000000
cetotalvol=(CedataFinal1['totalTradedVolume'].sum())/10000000
cetradequantity=cetotalvol/cetotaloi

CedataFinal1['amount']=CedataFinal1['openInterest']*CedataFinal1['lastPrice']*lot

CedataFinal1['oldOi']=(CedataFinal1['changeinOpenInterest']*100)/CedataFinal1['changeinOpenInterest']
CedataFinal1['oldLtp']=(CedataFinal1['change']*100)/CedataFinal1['pChange']
CedataFinal1['oldAmount']=CedataFinal1['oldLtp']*CedataFinal1['oldOi']
CedataFinal1['ValueChange']=CedataFinal1['amount']-CedataFinal1['oldAmount']
CedataFinal1=(CedataFinal1.fillna(0)).copy()


CedataFinal2=CedataFinal1.sort_values(by=['ValueChange']).copy()







petotaloi=(PedataFinal1['openInterest'].sum())/10000000
petotalvol=(PedataFinal1['totalTradedVolume'].sum())/10000000
petradequantity=petotalvol/petotaloi

PedataFinal1['amount']=PedataFinal1['openInterest']*PedataFinal1['lastPrice']*lot

PedataFinal1['oldOi']=(PedataFinal1['changeinOpenInterest']*100)/PedataFinal1['changeinOpenInterest']
PedataFinal1['oldLtp']=(PedataFinal1['change']*100)/PedataFinal1['pChange']
PedataFinal1['oldAmount']=PedataFinal1['oldLtp']*PedataFinal1['oldOi']
PedataFinal1['ValueChange']=PedataFinal1['amount']-PedataFinal1['oldAmount']
PedataFinal1=(PedataFinal1.fillna(0)).copy()


PedataFinal2=PedataFinal1.sort_values(by=['ValueChange']).copy()










#calculation for ATM strike Price
celistlength=len(CedataFinal1)-1
x=CedataFinal1.iloc[celistlength,18]

zz=round(celistlength/2)
zz1=zz-1
strikediff=CedataFinal1.iloc[zz,0]-CedataFinal1.iloc[zz1,0]




x=round(x / strikediff)

y=x*strikediff


k=0
for k in range (celistlength):
    if y==CedataFinal1.iloc[k,0]:
        atmstrike=k
        break



ceatmstrikeprice=CedataFinal1.iloc[k,0]
ceatmchanval=round((CedataFinal1.iloc[k,23]/10000000),3)
ceatmltp=round((CedataFinal1.iloc[k,9]),2)
ceatmimp=round((CedataFinal1.iloc[k,8]),2)
ceatmoi=round((CedataFinal1.iloc[k,4]),3)



pelistlength=len(PedataFinal1)-1
x=PedataFinal1.iloc[pelistlength,18]
x=round(x / strikediff)

y=x*strikediff



k=0
for k in range (pelistlength):
    if y==PedataFinal1.iloc[k,0]:
        atmstrike=k
        break



peatmstrikeprice=PedataFinal1.iloc[k,0]
peatmchanval=round((PedataFinal1.iloc[k,23]/10000000),3)
peatmltp=round((PedataFinal1.iloc[k,9]),2)
peatmimp=round((PedataFinal1.iloc[k,8]),2)
peatmoi=round((PedataFinal1.iloc[k,4]),3)


newatm=y-strikediff*3
xnewatm=y+strikediff*4
xsum=[]
xbottomdetail=[]
xnonzero=[]
ii=-3


while newatm < (xnewatm):

    ceL=[]
    peL=[]
    xceL=[]
    xpeL=[]





    #_______________________________________________________________
    #_______________________________________________________________

    atm=newatm

    a=atm
    b=atm+strikediff
    c=atm+strikediff*2
    d=atm+strikediff*3
    e=atm+strikediff*4

    xa=atm
    xb=atm-strikediff
    xc=atm-strikediff*2
    xd=atm-strikediff*3
    xe=atm-strikediff*4



    #_______________________________________________________________
    #_______________________________________________________________


    #-----------------new price------ce--------------
    #-----1-----
    k=0
    for k in range (celistlength):
        if a==CedataFinal1.iloc[k,0]:
            ce_a_ltp=round((CedataFinal1.iloc[k,9]),2)
            ce_a_oi=round((CedataFinal1.iloc[k,4]),3)

            break

    k=0
    for k in range (pelistlength):
        if a==PedataFinal1.iloc[k,0]:
            pe_a_ltp=round((PedataFinal1.iloc[k,9]),2)
            pe_a_oi=round((PedataFinal1.iloc[k,4]),3)

            break

    #-----2-----

    k=0
    for k in range (celistlength):
        if b==CedataFinal1.iloc[k,0]:
            ce_b_ltp=round((CedataFinal1.iloc[k,9]),2)
            ce_b_oi=round((CedataFinal1.iloc[k,4]),3)

            break
    k=0
    for k in range (pelistlength):
        if b==PedataFinal1.iloc[k,0]:
            pe_b_ltp=round((PedataFinal1.iloc[k,9]),2)
            pe_b_oi=round((PedataFinal1.iloc[k,4]),3)

            break


    #-----3------

    k=0
    for k in range (celistlength):
        if c==CedataFinal1.iloc[k,0]:
            ce_c_ltp=round((CedataFinal1.iloc[k,9]),2)
            ce_c_oi=round((CedataFinal1.iloc[k,4]),3)

            break

    k=0
    for k in range (pelistlength):
        if c==PedataFinal1.iloc[k,0]:
            pe_c_ltp=round((PedataFinal1.iloc[k,9]),2)
            pe_c_oi=round((PedataFinal1.iloc[k,4]),3)

            break



    #-----4------

    k=0
    for k in range (celistlength):
        if d==CedataFinal1.iloc[k,0]:
            ce_d_ltp=round((CedataFinal1.iloc[k,9]),2)
            ce_d_oi=round((CedataFinal1.iloc[k,4]),3)

            break

    k=0
    for k in range (pelistlength):
        if d==PedataFinal1.iloc[k,0]:
            pe_d_ltp=round((PedataFinal1.iloc[k,9]),2)
            pe_d_oi=round((PedataFinal1.iloc[k,4]),3)

            break



    #-----5------


    k=0
    for k in range (celistlength):
        if e==CedataFinal1.iloc[k,0]:
            ce_e_ltp=round((CedataFinal1.iloc[k,9]),2)
            ce_e_oi=round((CedataFinal1.iloc[k,4]),3)

            break


    k=0
    for k in range (pelistlength):
        if e==PedataFinal1.iloc[k,0]:
            pe_e_ltp=round((PedataFinal1.iloc[k,9]),2)
            pe_e_oi=round((PedataFinal1.iloc[k,4]),3)

            break

    #-----end------


    #-----pe----------------------------

    #-------x1-----------------
    k=0
    for k in range (celistlength):
        if xa==CedataFinal1.iloc[k,0]:
            xce_a_ltp=round((CedataFinal1.iloc[k,9]),2)
            xce_a_oi=round((CedataFinal1.iloc[k,4]),3)

            break



    k=0
    for k in range (pelistlength):
        if xa==PedataFinal1.iloc[k,0]:
            xpe_a_ltp=round((PedataFinal1.iloc[k,9]),2)
            xpe_a_oi=round((PedataFinal1.iloc[k,4]),3)

            break


    #-------x2-----------------

    k=0
    for k in range (celistlength):
        if xb==CedataFinal1.iloc[k,0]:
            xce_b_ltp=round((CedataFinal1.iloc[k,9]),2)
            xce_b_oi=round((CedataFinal1.iloc[k,4]),3)

            break





    k=0
    for k in range (pelistlength):
        if xb==PedataFinal1.iloc[k,0]:
            xpe_b_ltp=round((PedataFinal1.iloc[k,9]),2)
            xpe_b_oi=round((PedataFinal1.iloc[k,4]),3)

            break

    #-------x3-----------------

    k=0
    for k in range (celistlength):
        if xc==CedataFinal1.iloc[k,0]:
            xce_c_ltp=round((CedataFinal1.iloc[k,9]),2)
            xce_c_oi=round((CedataFinal1.iloc[k,4]),3)

            break




    k=0
    for k in range (pelistlength):
        if xc==PedataFinal1.iloc[k,0]:
            xpe_c_ltp=round((PedataFinal1.iloc[k,9]),2)
            xpe_c_oi=round((PedataFinal1.iloc[k,4]),3)

            break



    #-------x4-----------------
    k=0
    for k in range (celistlength):
        if xd==CedataFinal1.iloc[k,0]:
            xce_d_ltp=round((CedataFinal1.iloc[k,9]),2)
            xce_d_oi=round((CedataFinal1.iloc[k,4]),3)

            break




    k=0
    for k in range (pelistlength):
        if xd==PedataFinal1.iloc[k,0]:
            xpe_d_ltp=round((PedataFinal1.iloc[k,9]),2)
            xpe_d_oi=round((PedataFinal1.iloc[k,4]),3)

            break


    #-------x5-----------------

    k=0
    for k in range (celistlength):
        if xe==CedataFinal1.iloc[k,0]:
            xce_e_ltp=round((CedataFinal1.iloc[k,9]),2)
            xce_e_oi=round((CedataFinal1.iloc[k,4]),3)

            break



    k=0
    for k in range (pelistlength):
        if xe==PedataFinal1.iloc[k,0]:
            xpe_e_ltp=round((PedataFinal1.iloc[k,9]),2)
            xpe_e_oi=round((PedataFinal1.iloc[k,4]),3)

            break


    #-------x--end-----------------



    amt_a=(((ce_a_ltp)*(ce_a_oi) + (pe_a_ltp)*(pe_a_oi) )*lot) /10000000
    amt_b=(((ce_b_ltp)*(ce_b_oi) + (pe_b_ltp)*(pe_b_oi) )*lot) /10000000
    amt_c=(((ce_c_ltp)*(ce_c_oi) + (pe_c_ltp)*(pe_c_oi) )*lot) /10000000
    amt_d=(((ce_d_ltp)*(ce_d_oi) + (pe_d_ltp)*(pe_d_oi) )*lot) /10000000
    amt_e=(((ce_e_ltp)*(ce_e_oi) + (pe_e_ltp)*(pe_e_oi) )*lot) /10000000



    amt_cez=(((ce_a_ltp)*(ce_a_oi)+(ce_b_ltp)*(ce_b_oi)+(ce_c_ltp)*(ce_c_oi)+(ce_d_ltp)*(ce_d_oi)+(ce_e_ltp)*(ce_e_oi))*lot)/10000000


    amt_xa=(((xce_a_ltp)*(xce_a_oi) + (xpe_a_ltp)*(xpe_a_oi) )*lot) /10000000
    amt_xb=(((xce_b_ltp)*(xce_b_oi) + (xpe_b_ltp)*(xpe_b_oi) )*lot) /10000000
    amt_xc=(((xce_c_ltp)*(xce_c_oi) + (xpe_c_ltp)*(xpe_c_oi) )*lot) /10000000
    amt_xd=(((xce_d_ltp)*(xce_d_oi) + (xpe_d_ltp)*(xpe_d_oi) )*lot) /10000000
    amt_xe=(((xce_e_ltp)*(xce_e_oi) + (xpe_e_ltp)*(xpe_e_oi) )*lot) /10000000




    amt_pez= (((xpe_a_ltp)*(xpe_a_oi)+(xpe_b_ltp)*(xpe_b_oi)+(xpe_c_ltp)*(xpe_c_oi)+(xpe_d_ltp)*(xpe_d_oi)+(xpe_e_ltp)*(xpe_e_oi))*lot)/10000000

    #---------calculation---part 1-----
    atm_xyce_a=(((xce_a_ltp)*(xce_a_oi))*lot)  /10000000
    atm_xype_a=(((xpe_a_ltp)*(xpe_a_oi))*lot)  /10000000

    atm_xyce_b=(((xce_b_ltp)*(xce_b_oi))*lot)  /10000000
    atm_xype_b=(((xpe_b_ltp)*(xpe_b_oi))*lot)  /10000000

    atm_xyce_c=(((xce_c_ltp)*(xce_c_oi))*lot)  /10000000
    atm_xype_c=(((xpe_c_ltp)*(xpe_c_oi))*lot)  /10000000

    atm_xyce_d=(((xce_d_ltp)*(xce_d_oi))*lot)  /10000000
    atm_xype_d=(((xpe_d_ltp)*(xpe_d_oi))*lot)  /10000000

    atm_xyce_e=(((xce_e_ltp)*(xce_e_oi))*lot)  /10000000
    atm_xype_e=(((xpe_e_ltp)*(xpe_e_oi))*lot)  /10000000


    #---------------------part 2------------
    atm_yce_a=(((ce_a_ltp)*(ce_a_oi))*lot)  /10000000
    atm_ype_a=(((pe_a_ltp)*(pe_a_oi))*lot)  /10000000

    atm_yce_b=(((ce_b_ltp)*(ce_b_oi))*lot)  /10000000
    atm_ype_b=(((pe_b_ltp)*(pe_b_oi))*lot)  /10000000

    atm_yce_c=(((ce_c_ltp)*(ce_c_oi))*lot)  /10000000
    atm_ype_c=(((pe_c_ltp)*(pe_c_oi))*lot)  /10000000

    atm_yce_d=(((ce_d_ltp)*(ce_d_oi))*lot)  /10000000
    atm_ype_d=(((pe_d_ltp)*(pe_d_oi))*lot)  /10000000

    atm_yce_e=(((ce_e_ltp)*(ce_e_oi))*lot)  /10000000
    atm_ype_e=(((pe_e_ltp)*(pe_e_oi))*lot)  /10000000


    Totalamt=atm_ype_b+atm_ype_c+atm_ype_d+atm_ype_e
    xTotalamt=atm_xyce_b+atm_xyce_c+atm_xyce_d+atm_xyce_e



    #-------------------calculation for other range---------------------------

    k=0
    for k in range (celistlength):
        if e<CedataFinal1.iloc[k,0]:
            yce_a_ltp=round((CedataFinal1.iloc[k,9]),2)
            yce_a_oi=round((CedataFinal1.iloc[k,4]),3)

            ceL.append([CedataFinal1.iloc[k,0],yce_a_ltp,yce_a_oi])


    k=0
    for k in range (pelistlength):
        if e<PedataFinal1.iloc[k,0]:
            ype_a_ltp=round((PedataFinal1.iloc[k,9]),2)
            ype_a_oi=round((PedataFinal1.iloc[k,4]),3)

            peL.append([PedataFinal1.iloc[k,0],ype_a_ltp,ype_a_oi])

    #-----------------

    k=0
    for k in range (celistlength):
        if xe>CedataFinal1.iloc[k,0]:
            zce_a_ltp=round((CedataFinal1.iloc[k,9]),2)
            zce_a_oi=round((CedataFinal1.iloc[k,4]),3)

            xceL.append([CedataFinal1.iloc[k,0],zce_a_ltp,zce_a_oi])


    k=0
    for k in range (pelistlength):
        if xe>PedataFinal1.iloc[k,0]:
            zpe_a_ltp=round((PedataFinal1.iloc[k,9]),2)
            zpe_a_oi=round((PedataFinal1.iloc[k,4]),3)

            xpeL.append([PedataFinal1.iloc[k,0],zpe_a_ltp,zpe_a_oi])


    #------calculation for range sum

    dfce = pd.DataFrame(ceL, columns = ['stk','ltp','0i'])
    dfpe = pd.DataFrame(peL, columns = ['stk','ltp','0i'])

    xdfce = pd.DataFrame(xceL, columns = ['stk','ltp','0i'])
    xdfpe = pd.DataFrame(xpeL, columns = ['stk','ltp','0i'])


    dfce['sum']=dfce['ltp']*dfce['0i']
    dfpe['sum']=dfpe['ltp']*dfpe['0i']

    xdfce['sum']=xdfce['ltp']*xdfce['0i']
    xdfpe['sum']=xdfpe['ltp']*xdfpe['0i']

    topsum=((dfce['sum'].sum()+dfpe['sum'].sum())*lot)/10000000


    bottomsum=((xdfce['sum'].sum()+xdfpe['sum'].sum())*lot)/10000000

    topsumce=((dfce['sum'].sum())*lot)/10000000
    topsumpe=((dfpe['sum'].sum())*lot)/10000000


    bottomsumce=((xdfce['sum'].sum())*lot)/10000000
    bottomsumpe=((xdfpe['sum'].sum())*lot)/10000000


    #--------------------------------------------

    #other calculation

    pcr_tt=round( petotaloi/cetotaloi  ,3)

    celenght=len(CedataFinal2)-1
    pelenght=len(PedataFinal2)-1

    cehighChanval=round((CedataFinal2.iloc[celenght,23]/10000000),3)
    cehighstrikeval=CedataFinal2.iloc[celenght,0]
    cehighltp=round((CedataFinal2.iloc[celenght,9]),2)
    cehighimp=round((CedataFinal2.iloc[celenght,8]),2)
    cechoi=round((CedataFinal2.iloc[celenght,4]),3)



    pehighChanval=round((PedataFinal2.iloc[pelenght,23]/10000000),3)
    pehighstrikeval=PedataFinal2.iloc[pelenght,0]
    pehighltp=round((PedataFinal2.iloc[pelenght,9]),2)
    pehighimp=round((PedataFinal2.iloc[pelenght,8]),2)
    pechoi=round((PedataFinal2.iloc[pelenght,4]),3)


    underlyval=CedataFinal2.iloc[celenght,18]
    atmpcr=round(peatmoi/ceatmoi  ,3)
    pcr_ch=round(cechoi/pechoi  ,3)



    TotalAmount=(CedataFinal1['amount'].sum()+PedataFinal1['amount'].sum())
    amountincr=TotalAmount/10000000
    ceper=(CedataFinal1['amount'].sum()/TotalAmount)*100
    peper=(PedataFinal1['amount'].sum()/TotalAmount)*100
    
    xsum.append((round((amt_cez+amt_pez ),2),round(atm_xyce_a,2),round(atm_xype_a,2)))
    xnonzero.append ((round((xTotalamt),2),round((Totalamt),2),round((amt_cez),2),round((amt_pez),2)))
    xbottomdetail.append((round(bottomsumce,2),round(bottomsumpe,2),round(bottomsum,2),round(topsumce,2),round(topsumpe,2),round(topsum,2)))




    ii=ii+1
    newatm=newatm+strikediff


xsum = pd.DataFrame(xsum, columns = ['sum','atmce','atmpe'])
botsum=xsum.iloc[0,0]+xsum.iloc[1,0]+xsum.iloc[2,0]
topsum=xsum.iloc[4,0]+xsum.iloc[5,0]+xsum.iloc[6,0]
atsum=xsum.iloc[3,0]

xnonzero= pd.DataFrame(xnonzero, columns = ['cenonzero','penonzero','cezero','pezero'])

xbottomdetail = pd.DataFrame(xbottomdetail, columns = ['bce','bpe','bsum','tce','tpe','tsum'])





#for data
Bf_Add_list.append((underlyval,round((xbottomdetail.iloc[3,2]/amountincr),2),round((xbottomdetail.iloc[3,5]/amountincr),2),round(amountincr,2),round(((botsum/3)/amountincr),2),round((atsum/amountincr),2),round(((topsum/3)/amountincr),2)))


nfdata=pd.DataFrame(Nf_Add_list, columns = ['Time','mak','BP','TP','sn','val1','val2','val3'])
bfdata=pd.DataFrame(Bf_Add_list, columns = ['makb','BPB','TPB', 'amtb','val4','val5','val6'])


nfdata['makb']=bfdata['makb']
nfdata['BPB']=bfdata['BPB']
nfdata['TPB']=bfdata['TPB']
nfdata['amtb']=bfdata['amtb']
nfdata['val4']=bfdata['val4']
nfdata['val5']=bfdata['val5']
nfdata['val6']=bfdata['val6']







def get_existing_file(drive_service, folder_id, file_name):
    results = drive_service.files().list(q=f"'{folder_id}' in parents and name='{file_name}'").execute()
    files = results.get('files', [])

    if len(files) > 0:
        file_id = files[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        existing_file = io.BytesIO()
        downloader = MediaIoBaseDownload(existing_file, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

        return file_id, existing_file.getvalue().decode('utf-8')
    else:
        return None, None
    

def _process_data(data,header,df):
        data = filter(lambda x: len(x.split(',')) == len(header), data)
        data = map(lambda x: x.split(','), data)
        data = list(data)

        new_row = df.iloc[0].tolist()
        data.append(new_row)

        # Convert items to numbers
        data = [[float(item) for item in sublist] for sublist in data]

        return data

def _getFileName():
    # Get the current date
    current_date = datetime.now()

    # Format the current date as dd-mm-yyyy
    formatted_date = current_date.strftime("%d-%m-%Y")
    filename = 'data-report' + '_' + formatted_date + '.csv'
    return filename


def generate_report(df):
    # Replace 'FOLDER_ID' with the actual ID of your 'reports' folder in Google Drive
    # folder_id = '1fPo6lRq6dIlhKhElGE1xqjj-3P9S-h5l'

    # Choose the desired filename for the CSV file
    file_name = _getFileName()

    # Authenticate with Google Drive API using service account credentials
    gauth = GoogleAuth()
    gauth.credentials = SERVICE_ACCOUNT_CREDENTIALS

    # Create a new session to refresh the access token if necessary
    credentials.refresh(Request())

    # Build the Google Drive service
    drive_service = build('drive', 'v3', credentials=credentials)

    # Get folder ID
    # folder_id = get_folder_id(drive_service,'reports')
    folder_id = FOLDER_ID

    # Check if the file already exists in the folder
    file_id, existing_content = get_existing_file(drive_service, folder_id, file_name)
    

    if file_id is not None:
        arr = existing_content.split('\n')
        header = arr[0].split(',')
        data_io = StringIO(existing_content)
        df1 = pd.read_csv(data_io)
        
        
        #dadding data
        comdata=pd.concat([df1, df], ignore_index=True)

    
        #updated_data = _process_data(arr[1:],header,df)
        
        #updated_df = pd.DataFrame(updated_data, columns=header)
        print('***** updated_df *****')

        updated_csv_content =  comdata.to_csv(index=False)

        # Upload the updated content to the file
        media = MediaIoBaseUpload(io.BytesIO(updated_csv_content.encode('utf-8')), mimetype='text/csv')
        drive_service.files().update(fileId=file_id, media_body=media).execute()
        print(f"File '{file_name}' updated in Google Drive successfully!")

    else:
        # If the file doesn't exist, create a new one and upload the CSV content
        file_metadata = {
            'name': file_name,
            'parents': [folder_id],
            'mimeType': 'text/csv'
        }
        csv_content = df.to_csv(index=False)
        media = MediaIoBaseUpload(io.BytesIO(csv_content.encode('utf-8')), mimetype='text/csv', resumable=True)
        drive_service.files().create(body=file_metadata, media_body=media).execute()
        print(f"File '{file_name}' uploaded to Google Drive successfully!")


if __name__ == "__main__":
    rec=nfdata.copy()
    print(rec)
    try:
        if rec is not None:
            if MODE != 'prod':
                print('[info]: This is running in dev mode, report will not be uploded')
            else:
                generate_report(rec)
            
    except:
        print('NO RECORD CREATED!')




