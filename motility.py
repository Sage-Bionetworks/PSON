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
parser.add_argument("-folder", "--folderInfo", help="yaml file containing folder-synapseID information",type=str)

args = parser.parse_args()

f = pd.read_csv(args.information) #'file_size_md5/physicalCharacterization/Motility_file_size_md5.csv'

conditions = pd.read_csv(args.condition, sep="\t",names=["filePath","catelogNumber","experimentalCondition"]) #"motility_file_cellLine_condition.txt"

# Get catelogNumber and experimentalCondition
conditions['filePath'] = conditions['filePath'].apply(lambda x: 'ftp://caftpd.nci.nih.gov/psondcc/PhysicalCharacterization/'+ x)
f = pd.merge(f,conditions,how="left",on="filePath")

def completeCatalogNumber(row):
	filePath = row['filePath']
	catalogNumber = row['catalogNumber']
	if re.search(r'NCI-PBCF',filePath) and pd.isnull(catelogNumber):
		return filePath.split('/')[-1].split('.')[0]
	elif catalogNumber == "VARIOUS_CELLLINEs":
		return None
	else:
		return catalogNumber

f['catalogNumber'] = f.apply(completeCatalogNumber,axis=1)
f['experimentalCondition'] = f['experimentalCondition'].apply(lambda x: str(x).replace("VARIOUS_SUBSTRATES","Various Substrates"))

# Get cellLine
catNumCellLine = pd.read_csv("cellLine_catalogNumber_metadata.tsv", sep="\t")
f = pd.merge(f,catNumCellLine,how="left",on="catalogNumber")

# Get parentFolder
def getFileFolder(filePath):
	if re.search(r'Motility_Data/',filePath):
		return "data"
	if re.search(r'Raw_Motility_Images/',filePath):
		return "images"
	return "motility"

f['fileFolder'] = f['filePath'].apply(lambda x: getFileFolder(x))

# Get fileName
def getFileName(row):
	arr = row['filePath'].split('/')
	fileName = arr[-1]
	if row['fileFolder'] == 'data' and fileName.startswith(('Motility','Results')):
		return arr[-2]+"_"+fileName
	elif row['fileFolder'] == 'images':
		if row['catelogNumber'] == 'NCI-PBCF-CCL256':
			return fileName.replace('NCI_H2087','NCI_H2126')
		elif fileName.startswith('LNCap') and arr[-2] == "plate_2_timelapse":
			return fileName.replace('plate_1','plate_2')
		elif fileName.startswith('A375'):
			return fileName.replace('A375','DU145')
	return fileName

f['fileName'] = f.apply(getFileName,axis=1)

# Get fileFormat
f['fileFormat'] = f['fileName'].apply(lambda x: x.split('.')[-1].lower())

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

f.to_csv('motility_data_manifest.csv',index=False)
