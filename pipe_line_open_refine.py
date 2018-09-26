# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
CreatO on Fri Jul 07 16:40:05 2017
@author: 
igor.marques
flavio.oliveira
luan.lisboa
leonardo.ribeiro

Overview:
Este é o programa responsável por realizar a clusterização de nomes de produtos utilizando a biblioteca Open Refine criada pelo Google.

Workflow:
Para cada código de NCM existente na base, obtem-se todos os nomes de produtos daquele NCM.
A lista de nomes de produto é então enviada para o Open Refine, que clusteriza e retorna a lista de clusteres.
Essa lista é então inserida dentro da tabela de clusteres.
O Open Refine não clusteriza palavras que possuem os caracteres idênticos.
Portanto ao fim do loop, nós inserimos na tabela de clusteres todos os produtos com nomes idênticos.
"""

from google.refine import refine
import sys
import os
from cx_Oracle import DatabaseError
from cx_Oracle import connect
import time
import datetime
import pandas as pd

#==============================================================================
# Classe de Projeto
#==============================================================================
PATH_TO_TEST_DATA = os.path.join(os.path.dirname(__file__), 'data')

# USA
class MyRefineProject():
    def __init__(self, project_file):
        self.project_file = project_file
        self.project_format = 'text/line-based/*sv'
        self.project_options = {}
        self.project = None

    def project_path(self):
        return os.path.join(PATH_TO_TEST_DATA, self.project_file)

    def set_up(self):
        self.server = refine.RefineServer()
        self.refine = refine.Refine(self.server)
        if self.project_file:
            self.project = self.refine.new_project(project_file=self.project_path(), project_format=self.project_format,separator='|' , **self.project_options)

    def tear_down(self):
        if self.project:
            self.project.delete()
            self.project = None
            os.remove('data/'+self.project_file)

    def get_cluster(self, column_name):
        params = {'ngram-size':1}
        return self.project.compute_clusters(column_name, 'binning', 'ngram-fingerprint',params)

#==============================================================================
# Realiza a conexao a banco de dados
#==============================================================================
def conecta(): 
    username = '' #nome do usuario
    password = '' #senha
    databaseName = '' # Nome do Banco de Dados
    connection = connect(username,password,databaseName)
    return connection

#==============================================================================
# Imprime os erro de insert no banco de dados
#==============================================================================
def printf (format,*args):
    sys.stdout.write (format % args)

def printException (exception):
    error, = exception.args
    printf ('Error code = %s\n', error.code); # Codigo do Erro
    printf ('Error message = %s\n', error.message); # Mensagem do Erro

#==============================================================================
# Verficar a conexao executando
#==============================================================================
def verificar(sql):
    try:
        cursor = connection.cursor() # Realiza a conexao com o BD
        cursor.execute(sql)
    except DatabaseError as exception:
        printf ('Failed to execute: '+str(sql)+'\n')
        printException (exception)

    return cursor

#==============================================================================
# Inserir dados
#==============================================================================
def batch_insert(lista):
    sql = "INSERT INTO SUATABELA(CLUSTER_PAI,CLUSTER_FILHO,MODO_INSERCAO)\
    VALUES(dim_palavra_cluster_seq.nextval,:1, :2, :3)"
    try:
        cursor = connection.cursor()
        cursor.prepare(sql)
        cursor.executemany(None, lista)

    except DatabaseError as exception:
        printf ('Failed to do batch insert!')
        printException (exception)

    cursor.close()
    connection.commit()

#==============================================================================
# Inserir log
#==============================================================================
def insert_log(ncm, total_linhas, linhas_clusterizadas):

    sql = "INSERT INTO SUA_TABELA DE LOG(COD_NCM, QTD_TOTAL_LINHAS, QTD_LINHAS_CLUSTERIZADAS, QTD_DIFERENCA) \
    VALUES('"+str(ncm)+"', "+str(total_linhas)+", "+str(linhas_clusterizadas)+", "+str(0)+")"

    try:
        cursor = connection.cursor()
        cursor.execute(sql)

    except DatabaseError as exception:
        printf ('Failed to insert row')
        printException (exception)

    cursor.close()
    connection.commit()

#==============================================================================
# Fechar conexão
#==============================================================================
def closeDB():
    connection.close()

#==============================================================================
# Busca dados no Banco e gera projeto do OpenRefine
#==============================================================================
def get_qtd_ncm():
    sql="COM ESSA SQL VOCÊ DEVE TRAZER QUANTOS NCM EXISTEM NA SUA BASE"
    cursor = verificar(sql)
    row = cursor.fetchone()
    cursor.close()
    return row[0]


# PEGA OS NCMS EXISTENTES NA BASE
def get_lista_ncm():
    sql = "COM ESSA SQL VOCÊ DEVE TRAZER OS NCMS EXISTENTES NA SUA BASE"
    cursor = verificar(sql)
    i = 0
    qtd = get_qtd_ncm()
    lista = []
    while i < qtd:
        row = cursor.fetchone()
        lista.append(row[0])
        i += 1
    return lista


def get_qtd_titulo_cluster(cod_ncm):
    sql = "ESTA QUERY DEVE TRAZER A QUANTIDADE DE PRODUTOS CONTEM O NCM =" + str(cod_ncm)
    cursor = verificar(sql)
    row = cursor.fetchone()
    cursor.close()
    print(row[0])
    return row[0]

def get_lista_titulo_cluster(cod_ncm):
    sql = "ESTA QUERY DEVE TRAZER TODOS OS NOMES DE PRODUTOS QUE SE DESEJA CLUSTERIZAR DENTRO DE UM NCM=" + str(cod_ncm)
    cursor = verificar(sql)
    i = 0
    qtd = get_qtd_titulo_cluster(cod_ncm)
    lista = []
    while i < qtd:
        r = cursor.fetchone()
        lista.append(r[0])
        i += 1
    return lista


# ESTA FUNÇÃO RECUPERA TODOS OS NOMES DE PRODUTOS DE UM NCM E OS TRANSFORMA EM CSV PARA DAR ENTRADA NO REFINE
def get_arquivo_lista(ncm):
    produtos = get_lista_titulo_cluster(ncm)

    if not produtos:
        return 'empty.txt', 0

    lista_prod = ['NOME']
    lista_prod.extend(produtos)

    df = pd.DataFrame(lista_prod)

    file_name = str(ncm)+'.csv'
    df.to_csv('data/'+file_name, header=False, index=False, index_label=False)
    return file_name, len(produtos)


#===============================================================================
start = time.time()
qtd_li = 0
connection = conecta()
lista_ncm = get_lista_ncm()
for ncm in lista_ncm:

    start_ncm = time.time()
    print('ncm: '+ str(ncm))

    file_name, qtd_li = get_arquivo_lista(ncm)

    if qtd_li:

        start_project = time.time()
        try:
            project = MyRefineProject(file_name)
            project.set_up()
        except:
            print("Falha ao criar projeto: "+file_name+" - "+str(datetime.datetime.now()))
            text = [file_name+"|create_project_fail|"+str(datetime.datetime.now())]
            df = pd.DataFrame(text)
            with open('log/log.txt', 'a') as f:
                df.to_csv(f, header=False, index=False, index_label=False)

        end_project = time.time()

        start_cluster = time.time()
        clusters = project.get_cluster('NOME')
        end_cluster = time.time()
        
        start_insert = time.time()
        qtd_oc = 0
        tuple_list = []
        for cluster in clusters:
            cluster_pai = cluster[0]['value']
            for register in cluster:
                qtd_oc = qtd_oc + register['count']
                row_tuple = (cluster_pai, register['value'],'CLUSTER INICIAL')
                tuple_list.append(row_tuple)

        batch_insert(tuple_list)

        insert_log(ncm, qtd_li, qtd_oc)
        end_insert = time.time()

    end_ncm = time.time()
    print('tempo total do ncm: ' + str(end_ncm-start_ncm)+" - "+str(datetime.datetime.now()))

closeDB()
end = time.time()
print('tempo total: ' + str(end-start)+" - "+str(datetime.datetime.now()))