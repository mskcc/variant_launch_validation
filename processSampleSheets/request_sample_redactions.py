#!/opt/python2.7.2/bin/python2.7

from CMOProject import *
import CMOUtilities
import argparse
import smtplib
import shutil
import os
from email.mime.text import MIMEText
import subprocess
import lims_rest

def getParsedArgs():
    parser=argparse.ArgumentParser()
    parser.add_argument("-p","--projectNum",nargs=1,help="IGO Project ID")
    parser.add_argument("--manager",help="Project manager")
    parser.add_argument("--sampleRedactions",nargs=1,help=".txt file containing samples to be redacted")
    parser.add_argument("--redactionReasons",nargs=1,help=".txt file containing two columns: 1) sampleID and 2) reason for redaction")
    parser.add_argument("--analyst",nargs=1,help="Analyst who determined sample(s) should be redacted")
    parser.add_argument("--testing",action="store_true",help="run this script as a test")
    parser.add_argument("--serviceUser",nargs=1,help="Who is running this script? e.g.: ['qcWebsite'|'validator'|'Nick']") 
    return parser.parse_args()

def updateLims(proj_id, redaction_file, analyst, service_user, testing):
    results = []

    ## If proj_id ends with _Zp, it is a Z project (project with dmp samples added after LIMs) and will need to be removed 
    ## in order to correctly query the LIMs. Some errors and warnings should have original project ids
    ## ALSO if project id does not have prepended zero, prepend it for the LIMs proj id
    lims_proj_id=proj_id
    if not proj_id.startswith("0"):
        lims_proj_id="0" + proj_id
    lims_proj_id=re.sub('_p[0-9]$','',lim_proj)
    if proj_id.endswith("_Zp"):
        lims_proj_id=re.sub('_Zp$', '', proj_id)

    proj_samples_and_runs = lims_rest.getRunsPerSample(lims_proj_id, service_user)
    if not proj_samples_and_runs:
        results.append("LIMS_ERROR: Could not find samples and runs for project " + lims_proj_id + " in LIMS. QC status for redacted samples have NOT been updated.")
        return results

    status = "Failed" ## status Failed means "there is something wrong with the sample causing the pipeline results to be bad"
                      ## this is as opposed to status "Redacted" which means that a sample is removed for some other reason
                      ## such as patient consent withdrawn, etc. i.e., the sample could potentially be used in the future if "unredacted"

    with open(redaction_file,'rU') as f:
        for line in f:
            sample,reason = line.strip("\n").split("\t")
            if not sample in proj_samples_and_runs:
                results.append("LIMS_ERROR: Could not find sample " + sample + " for project " + proj_id + " in LIMS")
                continue
            cmo_sample_id = proj_samples_and_runs[sample]['cmo_id']
            runs = proj_samples_and_runs[sample]['runs']
            for run_id in runs:
                result = lims_rest.setPostQc(lims_proj_id,cmo_sample_id,run_id,analyst,status,reason,service_user,testing) 
                if not result:
                    results.append("LIMS_ERROR: Could not set QC status for sample "+cmo_sample_id+" from run " + run_id +". Details unknown. ")
                elif not result == 'success':
                    results.append(result)
    return results

def redactSamples(args):

    cmoproj = CMOProject(projID=args.projectNum,projManager=args.manager,testing=args.testing,overwrite=True)
    if not cmoproj.projectExists():
        print "\n"
        print "***** REQUEST FAILED *****\n"
        print "PM_ERROR: Could not find existing project with ID "+args.projectNum[0]
        print "\n**************************"
        sys.exit(1)

    cmoproj.createProjectFolder()
    cmoproj.saveAdditionalFiles(args.sampleRedactions)

    ### run Krista's script ###
    redFile = args.sampleRedactions[0]
    projDir = cmoproj.FINAL_PROJECT_ROOT
    configFile = cmoproj.FINAL_PROJECT_ROOT + "/" + cmoproj.analysisID + "_portal_conf_latest.txt"
    outDir = cmoproj.pipelineRunDir

    cmd = ["/bin/sh","/home/shiny/portal_automation/redactSamples.sh",
           "-s",redFile,
           "-c",configFile,
           "-o",outDir,
           "-v"]  

    submissionSuccess = False ## submissionSuccess indicates that everything on PM's side is correct
    bicErrors = []            ## any errors that can not be fixed by PM  
    out = None
    try:
        out = subprocess.check_output(cmd,stderr=subprocess.STDOUT).split("\n")
    except subprocess.CalledProcessError, e:
        out = e.output.split('\n')

    for line in out:
        if "PM_ERROR" in line:
           cmoproj.errors.append(line[line.index("PM_ERROR"):])
        elif "ERROR" in line:
            bicErrors.append(line[line.index("ERROR"):])
    if not cmoproj.errors or len(cmoproj.errors) == 0:
        submissionSuccess = True

    ## for project log
    completeSuccessResult = "revised project results files generated; ready for BIC to submit to portal"
    PMSuccessBicFailResult = "request received; waiting on BIC to generate files for portal submission"
    PMFailBicSuccessResult = "; ".join(cmoproj.errors)
    PMFailBicFailResult = "error on BIC's end; waiting on BIC to fix the issue"

    ## email constants
    subj = '[cmo-project-start] A sample redaction request has been submitted for %s' %cmoproj.analysisID
    successEmailText = "".join(["A request for sample redaction(s) for ",cmoproj.analysisID," has been received.\n\n",
                                "BIC has been notified and will update this project in the cBio portal ASAP."])
    

    if submissionSuccess:
        ## set result for project log and text for email notification
        if not bicErrors:
            result = completeSuccessResult
            ### update LIMS
            analyst = None
            if args.analyst:
                analyst = args.analyst[0]
            lims_results = updateLims(args.projectNum[0], args.redactionReasons[0], analyst, args.serviceUser[0], args.testing)
            if len(lims_results) == 0:
                result = completeSuccessResult
            else:
                result = PMSuccessBicFailResult
                failEmailBody = "There were errors in updating the LIMS:\n" + "\n".join(lims_results)
                cmoproj.sendEmail(subject=subj,body=failEmailBody)
        else:
            result = PMSuccessBicFailResult
        if not args.testing:
            cmoproj.sendEmail(recipients=[cmoproj.projManagerEmails[cmoproj.projManager]],subject=subj,body=successEmailText)
        else:
            cmoproj.sendEmail(subject=subj,body=successEmailText)
        print "Sample redaction request was successful."
        print "\n"
        print "BIC has been notified and will submit to cBio portal ASAP."
    else:
        ## something failed on PM's end
        ## set result for project log with errors and print message for user
        if not bicErrors:
            result = PMFailBicSuccessResult
            print "There were errors during this Sample redaction request."
            print "\n"
            print "BIC has NOT been notified. Please review, refresh, and resubmit."
            print "\n".join(cmoproj.errors)
        ## something failed on our end. PMs part may or may not be OK, but either way they will have to resubmit
        ## once we resolve the issue
        else:
            result = PMFailBicFailResult
            print "ERROR: An error has occurred on our end."
            print "\n"
            print "We have been notified and will send email when the issue has been resolved."

    ## compile all info BIC needs to fix any problems
    if bicErrors:
        text = 'Errors occurred during the generation of portal files for SAMPLE redaction.'
        text += "\n\nSUBMISSION COMMAND:"
        text += "\n\n\t" + " ".join(sys.argv)
        text += "\n\nREDACTION COMMAND:"
        text += "\n\n\t" + " ".join(cmd)
        text += "\n\nERROR(S):\n\n" + "\n".join(bicErrors)
        cmoproj.sendEmail(subject=subj,body=text)

    ## update log
    with open(args.redactionReasons[0],'rU') as rr:
        for line in rr:
            reason = "redact sample: "+line.strip().replace("\t"," - ")
            action = "Project Review"
            cmoproj.updateProjectStartLog(person=cmoproj.projManager,action=action,reason=reason,result=result)

    if submissionSuccess:
        sys.exit(0)
    sys.exit(1)

if __name__ == '__main__':
    redactSamples(getParsedArgs())
