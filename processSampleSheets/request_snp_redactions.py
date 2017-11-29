#!/opt/python2.7.2/bin/python2.7

from CMOProject import *
import CMOUtilities
import argparse
import smtplib
import shutil
import os
from email.mime.text import MIMEText
import subprocess

def getParsedArgs():
    parser=argparse.ArgumentParser()
    parser.add_argument("-p","--projectNum",nargs="+",help="IGO Project ID(s)")
    parser.add_argument("--manager",help="Project manager")
    parser.add_argument("--snpRedactions",nargs=1,help=".xlsx file containing snps to be redacted")
    parser.add_argument("--testing",action="store_true",help="run this script as a test")
    return parser.parse_args()

def redactSNPs(args):
    MASTER_REDACT_FILE = "/ifs/projects/CMO/archive/redactions/master_redact_file.txt"

    cmoproj = CMOProject(projID=args.projectNum,projManager=args.manager,testing=args.testing,overwrite=True)
    if not cmoproj.projectExists():
        print "\n"
        print "***** REQUEST FAILED *****\n"
        print "PM_ERROR: Could not find existing project with ID "+args.projectNum[0]
        print "\n**************************"
        sys.exit(1)

    cmoproj.createProjectFolder()
    cmoproj.saveAdditionalFiles(args.snpRedactions)

    ### run Krista's script ###
    reqFile = args.snpRedactions[0]
    projDir = cmoproj.FINAL_PROJECT_ROOT
    configFile = cmoproj.FINAL_PROJECT_ROOT + "/" + cmoproj.analysisID + "_portal_conf_latest.txt"
    ## test

    if not os.path.isdir(cmoproj.FINAL_PROJECT_ROOT): 
        oldID = cmoproj.analysisID.replace("Proj_0","Proj_")
        oldFinalDir = "/".join([cmoproj.CMO_PROJECTS_ROOT,oldID])
        if os.path.isdir(oldFinalDir):
            configFile = oldFinalDir + "/" + oldID + "_portal_conf_latest.txt"

    outDir = cmoproj.pipelineRunDir

    cmd = ["/opt/python2.7.2/bin/python2.7","/ifs/work/kristakaz/pipelines/portal_automation/MAF_redact/redactMutations.py",
           "-r",reqFile,
           "-c",configFile,
           "--mainOutDir",outDir,
           "--masterRedactFile",MASTER_REDACT_FILE,
           "-v"]  

    submissionSuccess = False ## submissionSuccess indicates that everything on PM's side is correct
    bicErrors = []            ## any errors that can not be fixed by PM  
    try:
        out = subprocess.check_output(cmd,stderr=subprocess.STDOUT).split("\n")
        for line in out:
            if "PM_ERROR" in line:
                cmoproj.errors.append(line[line.index("PM_ERROR"):])
            elif "ERROR" in line:
                bicErrors.append(line[line.index("ERROR"):])
        if not cmoproj.errors or len(cmoproj.errors) == 0:
            submissionSuccess = True
    except subprocess.CalledProcessError, e:
        bicErrors.append(e.output)

    ## for project log
    #action = "requested SNP redaction"
    action = "Project Review"
    reason = "SNP redaction"
    completeSuccessResult = "revised MAF and portal submission files generated; ready for BIC to submit to portal"
    PMSuccessBicFailResult = "request received; waiting on BIC to generate files for portal submission"
    PMFailBicSuccessResult = "; ".join(cmoproj.errors)
    PMFailBicFailResult = "error on BIC's end; waiting on BIC to fix the issue"

    ## email constants
    subj = '[cmo-project-start] A SNP redaction request has been submitted for %s' %cmoproj.analysisID
    successEmailText = "".join(["A request for SNP redactions for ",cmoproj.analysisID," has been received.\n\n",
                                "BIC has been notified and will update this project in the cBio portal ASAP."])
    

    if submissionSuccess:
        ## set result for project log and text for email notification
        if not bicErrors:
            result = completeSuccessResult
        else:
            result = PMSuccessBicFailResult
        if not args.testing:
            cmoproj.sendEmail(recipients=[cmoproj.projManagerEmails[cmoproj.projManager]],subject=subj,body=successEmailText)
        else:
            cmoproj.sendEmail(subject=subj,body=successEmailText)
        print "SNP redaction request was successful."
        print "\n\n"
        print "BIC has been notified and will submit to cBio portal ASAP."
    else:
        ## something failed on PM's end
        ## set result for project log with errors and print message for user
        if not bicErrors:
            result = PMFailBicSuccessResult
            print "There were errors during this SNP redaction request."
            print "\n\n"
            print "BIC has NOT been notified. Please review, refresh, and resubmit."
            print "\n".join(cmoproj.errors)
        ## something failed on our end. PMs part may or may not be OK, but either way they will have to resubmit
        ## once we resolve the issue
        else:
            result = PMFailBicFailResult
            print "ERROR: An error has occurred on our end. "
            print "\n\n"
            print "We have been notified and will send email when the issue has been resolved."

    ## compile all info BIC needs to fix any problems
    if bicErrors:
        text = 'Errors occurred during the generation of portal files for SNP redaction.'
        text += "\n\nSUBMISSION COMMAND:"
        text += "\n\n\t" + " ".join(sys.argv)
        text += "\n\nREDACTION COMMAND:"
        text += "\n\n\t" + " ".join(cmd)
        text += "\n\nERROR(S):\n\n" + "\n".join(bicErrors)
        cmoproj.sendEmail(subject=subj,body=text)

    ## update log
    cmoproj.updateProjectStartLog(person=cmoproj.projManager,action=action,reason=reason,result=result)

    if submissionSuccess:
        sys.exit(0)
    sys.exit(1)

if __name__ == '__main__':
    redactSNPs(getParsedArgs())
