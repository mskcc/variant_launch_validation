from CMOProject import *
import CMOUtilities
import argparse
import smtplib
import shutil
import os
from email.mime.text import MIMEText

def getParsedArgs():
    parser=argparse.ArgumentParser()
    parser.add_argument("-p","--projectNum",nargs="+",help="IGO Project ID(s)")
    parser.add_argument("--overwrite",action="store_true",help="this exact analysis has been run before")
    parser.add_argument("--testing",action="store_true",help="This is being run as a test. Write project files to testing directory")
    parser.add_argument("--deliverTo",nargs="+",help="MSKCC emails of those to whom results should be delivered, in addition to lab head and requester")
    parser.add_argument("--maf",nargs=1,help="new maf to be uploaded to the cBio portal")
    parser.add_argument("--cna",nargs=1,help="new CNA file to be uploaded to the cBio portal")
    parser.add_argument("--cna_seg",nargs=1,help="new CNA Seg file to be uploaded to the cBio portal")
    parser.add_argument("--addFile",nargs="+",help="additional file(s) to put in project folder")
    parser.add_argument("--freeze",action="store_true",help="FREEZE a project; project not to be touched without contacting Nick or Barry")
    parser.add_argument("--comment",nargs=1,help="comment describing any non-standard updates being made")
    parser.add_argument("--portalKeyVal",nargs='+',help="line to replace or add in portal config file; e.g., maf_desc=\"this is my description\"")
    parser.add_argument("--updater",nargs=1,help="name of person making updates to this project")
    return parser.parse_args()

def updateExistingProject(args):
    cmoproj = CMOProject(projID=args.projectNum,testing=args.testing,overwrite=args.overwrite)
    if not cmoproj.projectExists():
        print "**PROJECT HAS NOT BEEN UPDATED**"
        print "ERROR: Could not find existing project with ID "+args.projectNum[0]
        sys.exit(1)
    cmoproj.createProjectFolder()

    ## save all uploaded files to new project subfolder
    filesToSave = []
    if args.addFile:
        filesToSave += args.addFile

    ## if any portal fields are to be updated, write new portal file
    ## if any portal files are uploaded, save them as well
    if args.portalKeyVal or args.maf or args.cna or args.cna_seg:
        currentFile = "/".join([cmoproj.FINAL_PROJECT_ROOT,cmoproj.analysisID +"_portal_conf_redacted.txt"])
        if not os.path.isfile(currentFile):
            currentFile = currentFile.replace("_redacted.txt",".txt")
        if not os.path.isfile(currentFile):
            cmoproj.errors.append("ERROR: Could not find current portal config file in "+cmoproj.CMO_PROJECTS_ROOT+"!")

        portalKeyVals = {}
        for kvstr in args.portalKeyVal:
            k,v = kvstr.strip().split("=")
            portalKeyVals[k] = v
        if args.maf:
            filesToSave.append(args.maf[0])
            finalMafPath = "/".join([cmoproj.FINAL_PROJECT_ROOT,args.maf[0].split("/")[-1]])
            portalKeyVals['maf'] = finalMafPath
        if args.cna:
            filesToSave.append(args.cna[0])
            finalCnaPath = "/".join([cmoproj.FINAL_PROJECT_ROOT,args.cna[0].split("/")[-1]])
            portalKeyVals['cna'] = finalCnaPath
        if args.cna_seg:
            filesToSave.append(args.cna_seg[0])
            finalCnaSegPath = "/".join([cmoproj.FINAL_PROJECT_ROOT,args.cna_seg[0].split("/")[-1]])
            portalKeyVals['cna_seg'] = finalCnaSegPath
        cmoproj.writePortalConfigFile(currentFile=currentFile,**portalKeyVals)

    ## save any uploaded files
    if len(filesToSave) > 0:
        cmoproj.saveAdditionalFiles(filesToSave)

    ## if project is frozen, write empty FROZEN file
    if args.freeze:
        cmoproj.freezeProject()

    ## if a comment is given, write a new README with date
    if args.comment:
        cmoproj.writeUpdateReadmeFile(args.comment[0],args.updater[0])

    ## update project log
    person=args.updater[0]
    action="project updated"
    reason = args.comment[0] if args.comment else "Not specified."
    result = "PROJECT FROZEN. Notifying BIC." if args.freeze else "Notifying BIC of update."
    cmoproj.updateProjectStartLog(person=person,action=action,reason=reason,result=result)

    if len(cmoproj.errors) == 0:
        ## send email notification
        c = args.comment[0] if args.comment else ''
        subj = "[cmo-project-start] %s: updates have been made to project" %cmoproj.analysisID
        text = "Updates have been made to %s.\n\n" %cmoproj.analysisID
        if args.freeze:
            text += "PROJECT HAS BEEN FROZEN\n\n"
        if c:
            text += "COMMENT:\n\t"+c+"\n\n"
        text += "See files in seq:%s for further details."%cmoproj.pipelineRunDir
        cmoproj.sendEmail(subject=subj,body=text)
        print "Project successfully updated."
        print "\n".join(cmoproj.warnings)
    else:
        print "There were errors during this update attempt. Please review, refresh, and resubmit."
        print "\n".join(cmoproj.errors)
        print "\n".join(cmoproj.warnings)
        sys.exit(-1)
    sys.exit(0)

if __name__ == '__main__':
    updateExistingProject(getParsedArgs())
