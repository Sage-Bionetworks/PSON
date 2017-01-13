import pandas as pd
import synapseclient

import argparse

parser = argparse.ArgumentParser("Update genomics data file annotations")
parser.add_argument("-id","--synapseId" , help="folder ID",type=str)
parser.add_argument("-type", "--dataSubtype", help="dataSubtype annotation",type=str)

args = parser.parse_args()

syn = synapseclient.login()

result = syn.chunkedQuery('SELECT id FROM file WHERE parentId == \"%s\"' % args.synapseId)
f = pd.DataFrame(result['results'])
ids = list(f['file.id'])

for synId in ids:
    ent = syn.get(synId, downloadFile=False)
    newAnnotations = dict(dataSubtype = args.dataSubtype,
                          fundingAgency = "National Cancer Institute",
                          isCellLine = True,
                          species = "Human",
                          tissue = "Not Applicable")
    ent.annotations.update(newAnnotations)
    ent = syn.store(ent,forceVersion=False)
    print synId