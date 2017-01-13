import pandas as pd
import synapseclient
from synapseclient import File
import yaml
from pson_functions import storePSON

import argparse

parser = argparse.ArgumentParser("Link files")
parser.add_argument("-file","--fileInfo" , help="file path for the input file containing file information",type=str)
parser.add_argument("-folder", "--folderInfo", help="yaml file containing folder-synapseID information",type=str)
parser.add_argument("-assay", "--assay", help="assay annotation",type=str)
parser.add_argument("-study", "--study", help="study annotation",type=str)

args = parser.parse_args()

syn = synapseclient.login()

f = pd.read_csv(args.fileInfo,sep="\t")

# Get the folder synapse Ids
with open(args.folderInfo) as info:
    folderIds = yaml.load(info)

# Link files
for index,row in f.iterrows():
    entityFile = File(parentId = folderIds[row['fileFolder']],name=row['fileName'])
    annotations = dict(assay = args.assay,
    		           fileFormat = row['fileFormat'],
    		           dataType = row['dataType'],
    		           dataSubtype = row['dataSubtype'],
    		           consortium = "PSON",
    		           fundingAgency = "National Cancer Institute",
    		           isCellLine = True,
    		           species = "Human",
    		           tissue = "Not Applicable",
                       organ = row['organ'],
                       diagnosis = row['diagnosis'],
                       cellType = row['cellType'],
                       tumorType = row['tumorType'],
    		           cellLine = row['cellLine'],
    		           catelogNumber = row['catelogNumber'],
    		           study = args.study,
    		           experimentalCondition = row['experimentalCondition'])
    entityFile.annotations = dict((k,v) for k,v in annotations.iteritems() if v != 'None' and v != 'nan')
    newEntity = storePSON(entityFile,row['filePath'],contentSize=row['fileSize'],md5=row['md5'],syn=syn)
    print newEntity.id
