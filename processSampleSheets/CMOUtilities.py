from collections import OrderedDict

def formatFullName(x):
    if "," in x:
        return(x)
    else:
        nm = x.split()
        return(", ".join([nm[-1]," ".join(nm[:-1])])) 

def normalizeSampleNames(x):
    x=str(x)
    return x.strip().replace("-","_").replace(" ","_").replace("/","_").replace(".","_")

def relabelSampleNames(x,relabels):
    '''
    Normalize sample names (remove special chars).
    Store correct names for samples whos FASTQ and manifest label need to be fixed
    '''
    nx=normalizeSampleNames(x)
    if nx in relabels:
        return relabels[nx]
    return nx

def isNormalPool(x):
    xx=x.upper()
    if xx.find("POOL")>-1 and xx.find("NORMAL")>-1:
        return True
    return False

def sampleOutFormat(s):
    if not s.lower() == 'na':
        return "s_"+s
    return s

def fixOutput(s):
    return s.replace(" ","_").replace("-","_")

def makeGroupLabel(groupNo):
    return "Group_%03d" % groupNo

def makeRosettaStone(kvp):
    stone=OrderedDict()
    for rec in kvp.strip().split("\n"):
        (key,value)=rec.strip().split("=")
        stone[key]=value
    return stone
