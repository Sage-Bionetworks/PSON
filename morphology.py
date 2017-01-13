import pandas as pd
import re
import synapseclient
from synapseclient import File
syn = synapseclient.login()
import yaml
from pson_functions import storePSON
import argparse
 
parser = argparse.ArgumentParser("Link morphology files")
parser.add_argument("-info","--information" , help="file path for the input file containing file information",type=str)
parser.add_argument("-cond","--condition" , help="file path for the input file containing file condition",type=str)

args = parser.parse_args()

f = pd.read_csv(args.information) #'file_size_md5/physicalCharacterization/Morphology_file_size_md5.csv'

conditions = pd.read_csv(args.condition, sep="\t",names=["filePath","catelogNumber","experimentalCondition"]) #"morphology_file_cellLine_condition.txt"

# Get catelogNumber and experimentalCondition
conditions['filePath'] = conditions['filePath'].apply(lambda x: 'ftp://caftpd.nci.nih.gov/psondcc/PhysicalCharacterization/'+ x)
f = pd.merge(f,conditions,how="left",on="filePath")

def completeCatelogNumber(row):
	filePath = row['filePath']
	catelogNumber = row['catelogNumber']
	if re.search(r'NCI-PBCF',filePath) and pd.isnull(catelogNumber):
		return filePath.split('/')[-1].split('.')[0]
	elif catelogNumber == "VARIOUS_CELLLINEs":
		return None
	else:
		return catelogNumber

f['catelogNumber'] = f.apply(completeCatelogNumber,axis=1)
f['experimentalCondition'] = f['experimentalCondition'].apply(lambda x: str(x).replace("VARIOUS_SUBSTRATES","Various Substrates"))

# Get cellLine
catNumCellLine = pd.read_csv("cellLine_catalogNumber_metadata.tsv", sep="\t")
f = pd.merge(f,catNumCellLine,how="left",on="catelogNumber")

# Get parentFolder
def getFileFolder(filePath):
	if re.search(r'Morphology_Data/',filePath):
		return "data"
	if re.search(r'Morphology_Images/',filePath):
		return "images"
	return "morphology"

f['fileFolder'] = f['filePath'].apply(lambda x: getFileFolder(x))

# Get fileName
def getFileName(filePath):
	arr = filePath.split('/')
	if re.search(r'time_',filePath) or arr[-1] == "README.txt":
		return arr[-2]+"_"+arr[-1]
	else:
		return arr[-1]

f['fileName'] = f['filePath'].apply(lambda x: getFileName(x))

# Get fileFormat
f['fileFormat'] = f['fileName'].apply(lambda x: x.split('.')[-1].lower())
f.replace({'fileFormat':{'xlsx':'excel'}},inplace=True)

# Get dataType
def getType(fileFolder,type1,type2):
	if fileFolder == 'images':
		return type1
	else:
		return type2

f['dataType'] = f['fileFolder'].apply(lambda x: getType(x,"image","cellularPhysiology"))

# Get dataSubtype
f['dataSubtype'] = f['fileFolder'].apply(lambda x: getType(x,"raw","processed"))	

f.fillna('None', inplace=True)

f.to_csv('morphology_data_manifest.tsv',sep='\t',index=False)
