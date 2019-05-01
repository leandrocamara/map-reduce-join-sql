# -*- coding: utf-8 -*-
import mincemeat
import csv
import glob

PATH = '/home/leandro/Documents/puc/06-solutions/activities/map-reduce-join-sql/'
text_files = glob.glob(PATH + 'data/*')

# Retorna o conteúdo do arquivo.
def file_contents (file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

def mapfn_TEST(k, v):
    print 'map ' + k
    for line in v.splitlines():
        columns = line.split(';')
        yield columns[0], 1

# Lê cada linha do arquivo e transforma as palavras em uma estrutura "chave/valor".
def mapfn (k, v):
    print 'map ' + k
    PATH = '/home/leandro/Documents/puc/06-solutions/activities/map-reduce-join-sql/'
    for line in v.splitlines():
        columns = line.split(';')
        if k == (PATH + 'data/2.2-vendas.csv'):
            yield columns[0], 'Vendas' + ':' + columns[5]
        if k == (PATH + 'data/2.2-filiais.csv'):
            yield columns[0], 'Filial' + ':' + columns[1]

# Soma a quantidade de vendas por filial.
def reducefn (k, v):
    print 'reduce ' + k
    total = 0
    for index, item in enumerate(v):
        columns = item.split(':')
        if columns[0] == 'Vendas':
            total += int(columns[1])
        if columns[0] == 'Filial':
            NomeFilial = columns[1]
    L = list()
    L.append(NomeFilial + ' , ' + str(total))
    return L

# Transforma todos os arquivos em uma estrutura de "chave/valor" (file_name/file_content).
source = dict( (file_name, file_contents(file_name)) for file_name in text_files )

s = mincemeat.Server()

s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

# Apresenta o resultado em um arquivo CSV.
w = csv.writer( open(PATH + 'result.csv', 'w') )

for k, v in results.items():
    w.writerow([k, str(v).replace('[', '').replace(']', '').replace("'", '').replace(' ', '')])

# Name Node
# python join-sql.py

# Data Nodes
# python mincemeat.py -p changeme localhost