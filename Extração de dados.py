import logging
import pandas as pd
import datetime
from datetime import date, datetime
import upload_azure_gen2 as uag2

nm_arq_param = "raw/dados_externos/sgs/indicador_macroeconomico"


def processData():
    
    df_master = pd.DataFrame()

    with open('series.txt','r', encoding="utf8" ) as f:
        lines = f.read().splitlines()
        for line in lines:
            df_data = request(line)
            df_master = df_master.append(df_data)
    
    fileName = 'rw_sgs_'+datetime.now().strftime("%Y%m%d_%H_%M")+'.csv'
    csvContent = df_master.to_csv(index=False)
    #df_master.to_csv(fileName,index=False)
    uag2.uploadFile(fileName, csvContent, nm_arq_param)
    logging.info("Processed")



def request(param):
    columns_ndf = ['DATA', 'VALOR', 'DS_INDIC_MACRECON','CD_TIPO_DADO_INDIC_MACRECON',]
    param = str(param).replace('[','').replace(']','').replace("'","").split(',')
    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.%s/dados/ultimos/20000?formato=json" %(str(param[0]))
    try:
        df = pd.read_json(url)
    except:
        df= pd.DataFrame()
    for i in range(5):
        if len(df.columns) == 0:
            try:
                df = pd.read_json(url)
            except:
                df= pd.DataFrame()
    if len(df.columns) == 0:
        raise ValueError('Erro ao extrair indicador ' + url)
    ndf_matrix = []
    for index, row in df.iterrows():
        for x in df.columns:
            ndf_matrix.append([row['data'], row['valor'],param[1], param[2]])
    ndf = pd.DataFrame(ndf_matrix, columns=columns_ndf)  
            
    return ndf

processData()