import pandas as pd
import re
import synapseclient
from synapseclient import File
syn = synapseclient.login()
import yaml
from pson_functions import storePSON
import argparse
 
parser = argparse.ArgumentParser("Link miRNA files")
parser.add_argument("-info","--information" , help="file path for the input file containing file information",type=str)
parser.add_argument("-folder", "--folderInfo", help="yaml file containing folder-synapseID information",type=str)

args = parser.parse_args()

f = pd.read_csv(args.information) #'file_size_md5/cellLineGenomics/exomeSeq_file_size_md5.csv'

# Get catelogNumber
def getCatelogNumber(filePath):
	arr = filePath.split('/')
	if re.search(r'NCI-PBCF',filePath):
		if len(arr) > 7:
			return arr[6]
		else:
			return re.sub('\.zip','', arr[6])
	elif re.search(r'gene_fusions',filePath):
		if len(arr) > 8:
			return 'NCI-PBCF-'+arr[-1].split('_')[0]
	return None

f['catelogNumber'] = f['filePath'].apply(lambda x: getCatelogNumber(x))


# Get cellLine
catNumCellLine = pd.read_csv("catelogNumber_cellLine_mapping.csv")
f = pd.merge(f,catNumCellLine,how="left",on="catelogNumber")


# Get fileName
f['fileName'] = f['filePath'].apply(lambda x: x.split('/')[-1])

# Get fileFormat
f['fileFormat'] = f['fileName'].apply(lambda x: x.split('.')[-1])
f.replace({'fileFormat':{'fa':'fasta'}},inplace=True)

# # Get parentFolder
def getFileFolder(filePath):
	if filePath.endswith(('.fq.gz','.fq_fastqc.zip','.fastq.gz','fastqc.zip')):
		return "raw"
	if filePath.endswith(('.bam','.bai')):
		return "aligned"
	if len(filePath.split('/')) == 7:
		return "exome"
	return "analyzed"

f['fileFolder'] = f['filePath'].apply(lambda x: getFileFolder(x))

f.fillna('None', inplace=True)

# Get the folder synapse Ids
with open(args.folderInfo) as info:
    folderIds = yaml.load(info)

# Link files
for index,row in f.iterrows():
    entityFile = File(parentId = folderIds[row['fileFolder']],name=row['fileName'])
    annotations = dict(assay = row['assay'],
    		           fileFormat = row['fileFormat'],
    		           cellLine = row['cellLine'],
    		           catelogNumber = row['catelogNumber'],
    		           dataType = "geneExpression",
    		           consortium = "PSON")
    entityFile.annotations = dict((k,v) for k,v in annotations.iteritems() if v != 'None')
    newEntity = storePSON(entityFile,row['filePath'],contentSize=row['fileSize'],md5=row['md5'],syn=syn)



