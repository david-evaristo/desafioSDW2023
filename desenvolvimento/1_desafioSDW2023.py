from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import os

inicio = time.time()
pd.options.mode.chained_assignment = None
pd.options.display.float_format = '{:.2f}'.format

url = "https://www.fundamentus.com.br/fii_resultado.php"

tamanho = 10
MaxValorCotacao = 100
MinValorCotacao = 10
tamanhoLista = 2

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

def soupHtml(url):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")
    return soup

def buscaAtivos():
    soup = soupHtml(url)
    tabela = soup.find('table', {"id": "tabelaResultado"})
    dfDetalhes = pd.read_html(str(tabela), decimal='.',thousands='')[0]
    dfDetalhes.fillna(0, inplace=True)
    # dfDetalhes[dfDetalhes.columns[2]] = dfDetalhes[dfDetalhes.columns[2]].str.replace('.', '', regex=True).str.replace(',', '.', regex=True)#.astype(float)
    dfDetalhes[dfDetalhes.columns[2]] = dfDetalhes[dfDetalhes.columns[2]].str.replace('.', '').str.replace(',', '.').astype(float)
    dfDetalhes[dfDetalhes.columns[2]] = dfDetalhes[dfDetalhes.columns[2]].astype(float)
    dfDetalhes = dfDetalhes.sort_values(by='Cotação', ascending=False)
    dfDetalhes = dfDetalhes.loc[(dfDetalhes['Cotação'] < MaxValorCotacao)&(dfDetalhes['Cotação'] > MinValorCotacao)&(dfDetalhes['Dividend Yield'] != '0,00%')&(dfDetalhes['Liquidez'] != '0')].reset_index(drop=True)
    dfDetalhes = dfDetalhes.iloc[:tamanhoLista,:].reset_index(drop=True) #################################################
    # dfDetalhes = dfDetalhes.loc[dfDetalhes['Papel'] == 'BVAR11'].reset_index(drop=True)###############################################
    print(f'A lista tem {len(dfDetalhes)} ativos!')
    return dfDetalhes

def detalhesAtivo():
    dataFrameDetalhes = buscaAtivos()
    urlDet = 'https://www.fundamentus.com.br/detalhes.php?papel='
    urlRend = 'https://www.fundamentus.com.br/fii_proventos.php?papel='

    infoNumCota = []
    infoRenDist =[]
    listaUrl = []
    listaDfRendimento = []
    for a, ativo in enumerate(dataFrameDetalhes['Papel'].tolist(),1):
        urlDetalhes = urlDet + ativo
        urlRendimentos = urlRend + ativo

        soupDetalhes = soupHtml(urlDetalhes)
        soupRendimento = soupHtml(urlRendimentos)

        tabelaDetalhes = soupDetalhes.find("div",{"class":"conteudo clearfix"}).find_all('table',{"class":"w728"})
        tabelaRendimentos = soupRendimento.find("table",{"id":"resultado"}) 

        dfRendimento = pd.read_html(str(tabelaRendimentos), decimal='.',thousands='')[0].head(tamanho-1) if tabelaRendimentos is not None else pd.DataFrame()
        dfDetalhes = pd.concat([pd.read_html(str(tabela), decimal='.', thousands='')[0] for tabela in tabelaDetalhes[1:3]], ignore_index=True)

        listaDfRendimento.append(dfRendimento)

        infoNumCota.append(float(dfDetalhes.iloc[0,3].replace(".","")))
        infoRenDist.append(float(dfDetalhes.iloc[11,3].replace(".","")))
        listaUrl.append(urlDetalhes)
        
        print(a,ativo)
    infoDetalhes = [listaDfRendimento, infoNumCota, infoRenDist, listaUrl,dataFrameDetalhes]
    return infoDetalhes

def rendimentosAtivo():
    listaDf = detalhesAtivo()
    listaDfRendimento = listaDf[0]

    listaData = []
    colData = {}
    current_date = datetime.now()
    for i in range(tamanho):
        colData[current_date.strftime("%m/%Y")] = []
        listaData.append(current_date.strftime("%m/%Y"))
        current_date = current_date - timedelta(days=current_date.day)

    for rend in listaDfRendimento:
        indexMesRendimento = 0
        soma = {}
        if not rend.empty:
            remove_tres_primeiros = lambda x: x[3:]
            rend["Última Data Com"] = rend["Última Data Com"].apply(remove_tres_primeiros)
            # rend[rend.columns[3]] = rend[rend.columns[3]].str.replace('.', '',regex= True).str.replace(',', '.',regex= True).astype(float)
            rend[rend.columns[3]] = rend[rend.columns[3]].str.replace('.', '').str.replace(',', '.').astype(float)
            mesRendimento = rend['Última Data Com'].to_list()
            tipoRendimento = rend['Tipo'].to_list()
            valorRendimento = rend['Valor'].to_list()
            for interadorRendimento, dataRendimento in enumerate(mesRendimento):

                if dataRendimento in soma:
                    soma[dataRendimento] += 0#valorRendimento[interadorRendimento]
                elif tipoRendimento[interadorRendimento] != 'Amortização':
                    soma[dataRendimento] = valorRendimento[interadorRendimento]

            for key in colData:
                if key not in soma:
                    colData[key].append(0)
                else:
                    colData[key].append(float(soma[key]))
                    indexMesRendimento += 1
        else:
            dicionario_zero = {chave: valor + [0] for chave, valor in colData.items()}
            colData.update(dicionario_zero)
    return [colData, listaDf]

def unirDataframe():
    colData = rendimentosAtivo()
    # print(colData[1])
    infoNumCota = colData[1][1]
    infoRenDist = colData[1][2]
    urlDetalhes = colData[1][3]
    dataFrameDetalhes = colData[1][4]
    colData = pd.DataFrame(colData[0])
    
    dataFrameDetalhes["Num Cota"] = infoNumCota
    dataFrameDetalhes["Rend Distribuido"] = infoRenDist
    dataFrameDetalhes["URL"] = urlDetalhes

    # dataFrameDetalhes[dataFrameDetalhes.columns[[5,6,7,9,10]]] = dataFrameDetalhes[dataFrameDetalhes.columns[[5,6,7,9,10]]].apply(lambda x: x.str.replace('.', '',regex= True).str.replace(',', '.',regex= True)).astype(float)
    dataFrameDetalhes[dataFrameDetalhes.columns[[5,6,7,9,10]]] = dataFrameDetalhes[dataFrameDetalhes.columns[[5,6,7,9,10]]].apply(lambda x: x.str.replace('.', '').str.replace(',', '.')).astype(float)
    dfFinal = pd.concat([dataFrameDetalhes, colData], axis=1)
    dfFinal.fillna(0, inplace=True)
    dfFinal = dfFinal.drop(columns=['Preço do m2', 'Aluguel por m2', 'Cap Rate'])
    dfFinal.insert(loc=13, column='Media Dividendos', value=round(dfFinal.iloc[:,13:].mean(axis=1),4))
    dfFinal.insert(loc=13, column='Cotação x Med Div', value=round(dfFinal.apply(lambda row: (100 * row['Media Dividendos'])/row['Cotação'], axis=1),4))
    dfFinal['Cotação x Med Div'] = dfFinal['Cotação x Med Div'].apply(lambda x: str(round(x, 2)) + '%')
    dfFinal = dfFinal.sort_values(by='Cotação x Med Div', ascending=False).reset_index(drop=True)
    dfFinal.to_csv('dados.csv', index=False)
    return dfFinal 


unirDataframeFuncao = unirDataframe()

print(unirDataframeFuncao)
fim = time.time()
print("Tempo de execução " + str(round(((fim - inicio) /60),2)).replace('.',':'))