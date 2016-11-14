import synapseclient
syn = synapseclient.login()
import mimetypes
import json

def storePSON(ent,path,contentSize=None,md5="test",executed=None,used=None,syn="syn"):
	filetype = mimetypes.guess_type(ent.name,strict=False)[0]
	if filetype is None:
		filetype = ""
	fileHandle={}
	fileHandle['externalURL'] = path
	fileHandle["fileName"] = ent.name
	fileHandle["contentType"] = filetype
	fileHandle["contentMd5"] = md5
	fileHandle["contentSize"] = contentSize
	fileHandle["concreteType"] = "org.sagebionetworks.repo.model.file.ExternalFileHandle"
	fileHandle = syn.restPOST('/externalFileHandle', json.dumps(fileHandle), syn.fileHandleEndpoint)
	ent.dataFileHandleId = fileHandle['id']
	ent = syn.store(ent,executed=executed,used=used)
	return(ent)