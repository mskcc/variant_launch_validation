import urllib
import urllib2
import sys
import re
import base64
from urlparse import urlparse
import json
from CMOUtilities import *

###TEMP
username = 'bicboss'
password = 'deepestbluestsharksfin'

def getServiceResults(service_url):

    req = urllib2.Request(service_url)
    try:
        handle = urllib2.urlopen(req)
    except IOError, e:
        # here we *want* to fail
        pass
    else:
        # If we don't fail then the page isn't protected
        print "LIMS_REST_ERROR: This page isn't protected by authentication."
        return None

    if not hasattr(e, 'code') or e.code != 401:
        # we got an error - but not a 401 error
        print "LIMS_REST_ERROR: This page isn't protected by authentication. But we failed for another reason."
        return None

    authline = e.headers['www-authenticate']
    # this gets the www-authenticate line from the headers
    # which has the authentication scheme and realm in it

    authobj = re.compile(r'''(?:\s*www-authenticate\s*:)?\s*(\w*)\s+realm=['"]([^'"]+)['"]''',re.IGNORECASE)
    # this regular expression is used to extract scheme and realm
    matchobj = authobj.match(authline)

    if not matchobj:
        # if the authline isn't matched by the regular expression
        # then something is wrong
        print 'LIMS_REST_ERROR: The authentication header is badly formed.'
        print authline
        return None

    scheme = matchobj.group(1)
    realm = matchobj.group(2)
    # here we've extracted the scheme
    # and the realm from the header
    if scheme.lower() != 'basic':
        print 'LIMS_REST_ERROR: This example only works with BASIC authentication.'
        return None

    base64string = base64.encodestring(
                    '%s:%s' % (username, password))[:-1]
    authheader =  "Basic %s" % base64string
    req.add_header("Authorization", authheader)
    try:
        handle = urllib2.urlopen(req)
    except IOError, e:
        # here we shouldn't fail if the username/password is right
        print "LIMS_REST_ERROR: It looks like the username or password is wrong."

    thepage = handle.read()

    return thepage


def getPassedSamples(project_id,service_user):
    '''Get info on all samples that have passed BOTH Picard (Nathalie's) QC AND pipeline QC
    :rtype: object
    '''

    service_url = "https://igo.mskcc.org:8443/LimsRest/getPassingSamplesForProject?project=" + project_id + "&user=" + service_user
    results = json.loads(getServiceResults(service_url))

    if results and results['restStatus'] == 'SUCCESS':
        return results

    return None

def getRunsPerSample(project_id,service_user):
    '''Parse passed samples to get two things:
       1) a mapping of cmo sample ID to sample IDs from the pipeline
       2) a list of sequencing runs for each sample
       Return a dictionary where key = pipeline_id, val = {"cmo_id":x,"runs":[r1,r2]}
    ''' 

    sample_runs = {}

    passed_samples = getPassedSamples(project_id,service_user)
    if passed_samples:
        for s in passed_samples['samples']:
            cmo_id = s['cmoId']
            pipeline_id = sampleOutFormat(normalizeSampleNames(cmo_id)) 
            sample_runs[pipeline_id] = {'cmo_id':cmo_id,'runs':[]}
            for qc in s['basicQcs']:
                sample_runs[pipeline_id]['runs'].append(qc['run'])

    return sample_runs

def setPostQc(project_id,cmo_sample_id,run_id,analyst,status,reason,service_user,testing):
    '''
    Set a post-sequencing QC status for a sample+run pair
    '''
    qcType = "Post"
    if not status in ["Passed","Failed","Redacted"]:
        return "ERROR: LIMS status not updated for sample "+cmo_sample_id+" in run "+run_id+". Invalid status given. Must be 'Passed', 'Failed', or 'Redacted'"

    service_root = "https://igo.cbio.mskcc.org:8443/LimsRest/setQcStatus?"
    if testing:
        service_root = "https://toro.cbio.mskcc.org:8443/LimsRest/setQcStatus?"

    service_url = service_root + \
                   "status=" + status + \
                   "&project=" + project_id + \
                   "&sample=" + cmo_sample_id + \
                   "&run=" + run_id + \
                   "&qcType=" + qcType
    if analyst: 
        service_url += "&analyst=" + analyst
    if reason:
        service_url += "&note=" + urllib.quote_plus(reason)
    if service_user:
        service_url += "&user=" + service_user

    ## parse response and get restStatus
    results = getServiceResults(service_url)
    expected = 'NewStatus:'+status
    if not results == expected:
        return "ERROR: LIMS status not updated for sample "+cmo_sample_id+" in run "+run_id+".\n\nResults from setQcStatus service:\n"+results+"\n\n"
    return "success"


def projectExists(projID, service_user):
    service_url = "https://igo.mskcc.org:8443/LimsRest/getProjectDetailed?project=" + projID + "&user=" + service_user
    results = json.loads(getServiceResults(service_url))

    if results["projectId"] == "UNKNOWN":
        return False
    return True