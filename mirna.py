import pandas as pd
import re
import synapseclient
from synapseclient import File
syn = synapseclient.login()
import json
import yaml
import logging
import mimetypes
from pson_functions import storePSON
import argparse
 
parser = argparse.ArgumentParser("Link miRNA files")
parser.add_argument("-info","--information" , help="file path for the input file containing file information",type=str)
parser.add_argument("-folder", "--folderInfo", help="yaml file containing folder-synapseID information",type=str)

args = parser.parse_args()

f = pd.read_csv(args.information)

# Get fileName
f['fileName'] = f['filePath'].apply(lambda x: x.split('/')[-1])

# Get fileFormat
f['fileFormat'] = f['fileName'].apply(lambda x: x.split('.')[-1])
f.replace({'fileFormat':{'fq':'fastq'}},inplace=True)

# Get cellLine
def getCellLine(filePath):
    if re.search(r'NCI-PBCF',filePath):
        arr = filePath.split('/')[6]
        cellLineArr = arr.split('_')
        cellLine = '.'.join(cellLineArr[:len(cellLineArr)-2])
        return cellLine
    else:
        return None

f['cellLine'] = f['filePath'].apply(lambda x: getCellLine(x))

# Get parentFolder
def getFileFolder(fileName):
    if fileName.endswith(('.fastq','.trimmed.fq')):
        return "raw"
    if fileName.endswith(('.bam','.bai','.unmapped.fq')):
        return "aligned"
    if fileName.endswith(('.readsLength.freq','.txt','.16_25nt.fq','.miRanalyzer.zip')) or re.search(r'_vs_',fileName):
        return "analyzed"
    return "miRNA"

f['fileFolder'] = f['fileName'].apply(lambda x: getFileFolder(x))

# Get the folder synapse Ids
with open(args.folderInfo) as info:
    folderIds = yaml.load(info)

# Link files
for index,row in f.iterrows():
    entityFile = File(parentId = folderIds[row['fileFolder']],name=row['fileName'])
    entityFile.annotations = dict(assay = row['assay'],
                                 fileFormat = row['fileFormat'],
                                 cellLine = row['cellLine'])
    newEntity = storePSON(entityFile,row['filePath'],contentSize=row['fileSize'],md5=row['md5'],syn=syn)





