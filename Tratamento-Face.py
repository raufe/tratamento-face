# Programa para tratamento de arquivos vindos de campanhas no Facebook para criação de um 
# arquivo único para importação em um CRM proprietário

# Leitura de todos os arquivos

import os
import glob
import pandas as pd
import numpy as np

#set working directory
os.chdir("/Users/rauferibeiro/CargaFB")

# Verificando se os arquivos tratados existem e deletando caso existam

if(os.path.exists ('Carga_FB.csv')): 
    os.unlink ('Carga_FB.csv') 
    
if(os.path.exists ('Carga_FB_Final.csv')): 
    os.unlink ('Carga_FB_Final.csv') 

#find all csv files in the folder
#use glob pattern matching -> extension = 'csv'
#save result in list -> all_filenames
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
# print(all_filenames)

# Geração de um único arquivo combinado

combined_csv = pd.concat([pd.read_csv(f, encoding='UTF-16',sep='\t') for f in all_filenames ])
#export to csv
combined_csv.to_csv( "Carga_FB.csv", index=False, encoding='UTF-16')

# Criação de um Data Frame com os arquivos somados

df = pd.read_csv("Carga_FB.csv", encoding='UTF-16',sep=',')

# df.head(10)

# Tratamento de um Data Frame intermediário para próximo do layout do arquivo de saída

df.drop('created_time', inplace=True, axis=1)
df.drop('ad_id', inplace=True, axis=1)
df.drop('ad_name', inplace=True, axis=1)
df.drop('adset_id', inplace=True, axis=1)
df.drop('adset_name', inplace=True, axis=1)
df.drop('campaign_id', inplace=True, axis=1)
df.drop('campaign_name', inplace=True, axis=1)
df.drop('form_id', inplace=True, axis=1)
df.drop('id', inplace=True, axis=1)
df.drop('is_organic', inplace=True, axis=1)
df.drop('platform', inplace=True, axis=1)
df['ORIGEM'] = 'REDES_SOCIAIS'
df['ABORDAGEM'] = 'FACEBOOK'
df['phonestr']=df.phone_number.apply(str)
df['CELDDD'] = df.phonestr.str[5:7]
df['CELULAR'] = df.phonestr.str[7::]
        
# df.head(5)
        
# Função para tratar o campo telefone

def tratamento_tel(tel):
    if len(tel) == 9:
        tel_trat = tel
        return tel_trat
    else:
        tel_trat = '9' + tel
        return tel_trat
        
# Tratamento do um Data Frame final para ficar no layout do arquivo de saída
        
df2 = df.copy()
df2.drop('phone_number', inplace=True, axis=1)
df2.drop('phonestr', inplace=True, axis=1)
df2['CELULAR'] = df2.CELULAR.apply(tratamento_tel)
df2['FLAG2'] = 'FB0511'
df2['GRUPO'] = 'Grupo 1'
df2['TELDDD'] = np.nan
df2['TELEFONE'] = np.nan
df2['TIPOCURSO'] = np.nan
        
columnsTitles = ['FLAG2', 'ORIGEM', 'ABORDAGEM', 'full_name', 'email', 'CELDDD', 'CELULAR', 'TELDDD', 'TELEFONE',\
                 'GRUPO', 'TIPOCURSO', 'form_name']
df3 = df2.reindex(columns=columnsTitles)
df3.rename(columns={'full_name': 'FULLNAME', 'email': 'EMAIL', 'form_name':'CURSO'}, inplace=True)

# Criação do arquivo final

df3.to_csv( "Carga_FB_Final.csv", index=False, encoding='UTF-16')