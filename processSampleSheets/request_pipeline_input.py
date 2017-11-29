#!/opt/python2.7.2/bin/python2.7

import argparse

from CMOProject import *


def getParsedArgs():
    parser = argparse.ArgumentParser()

    req = parser.add_argument_group('required')

    req.add_argument("-p", "--projectNum", nargs="+", help="IGO Project ID(s)")
    req.add_argument("-n", "--projectTitle", help="Final Project Title")
    req.add_argument("-c", "--projectCmoId", help="CMO Project ID")
    req.add_argument("-s", "--species", help="species")
    req.add_argument("-l", "--labHead", help="Full name of PI or Lab Head")
    req.add_argument("-le", "--labHeadEmail", help="PI or Lab Head's MSKCC email")
    req.add_argument("-r", "--requester", help="Full name of requester")
    req.add_argument("-re", "--requesterEmail", help="Investigator's MSKCC email")
    req.add_argument("--brief", help="Brief project description")
    req.add_argument("--manager", help="Project manager")
    req.add_argument("-P", "--pairingSheet", help="Sample Pairing File")
    req.add_argument("-M", "--manifestFiles", nargs="+", help="Sample Manifest Excels")
    req.add_argument("-R", "--readme", help="Readme File From Project Managers")

    opt = parser.add_argument_group('optional')

    opt.add_argument("-a", "--analyst", help="Full name of data anlyst")
    opt.add_argument("-ae", "--analystEmail", help="Data analyst's MSKCC email")
    opt.add_argument("--rerun", action="store_true",
                     help="number of times the project has previously been run through the pipeline")
    opt.add_argument("--rerunReason", help="explanation for why this project needs to go through the pipeline again")
    opt.add_argument("--rerunDestination", help="[triage|delivery] to indicate where results of rerun should be sent")
    opt.add_argument("--noPipelineRun", action="store_true",
                     help="note that this project does NOT need to be run through the pipeline")
    opt.add_argument("--merge", nargs="+", help="IGO Project IDs of projects with which this one should be merged")
    opt.add_argument("--manifestSource", help="describe source of sample information (e.g., 'PM manual upload')")
    opt.add_argument("--overwrite", action="store_true", help="this exact analysis has been run before")
    opt.add_argument("--ignoreWarnings", action="store_true", help="accept and ignore warnings")
    opt.add_argument("--validateOnly", action="store_true",
                     help="Do NOT write any project files. Only run validation and print results")
    opt.add_argument("--noPortal", action="store_true",
                     help="This project is NOT to be imported to the cBio portal (e.g., non-cmo projects")
    opt.add_argument("--testing", action="store_true",
                     help="This is being run as a test. Write project files to testing directory")
    opt.add_argument("--deliverTo", nargs="+",
                     help="MSKCC emails of those to whom results should be delivered, in addition to lab head and requester")
    opt.add_argument("--exome", action="store_true", help="This is a WES project")
    opt.add_argument("--innovation", action="store_true", help="This is an innovation project (for validation only)")
    return parser.parse_args()


def printSuccess():
    print """
        ************************************************************************************

           Success! Project files have been written and BIC has been notified.            
                                        
           If anything needs to be changed, please email cmo-project-start@cbio.mskcc.org
           immediately.
    
        ************************************************************************************

        """
    return


def printFail(cmoproj):
    print "**************************************************************************\n"
    print "PROJECT FILES HAVE NOT BEEN WRITTEN."
    print "\n"
    if cmoproj.errors:
        print "Please fix the following errors:"
        print "\n"
        print "\n".join(sorted(cmoproj.errors))
        print "\n"
    else:
        print "No errors were found!"
        print "\n"
    if cmoproj.warnings:
        print "Please review the following warnings before submitting this project."
        print "If changes are made, validation must be run again before final submission."
        print "\n"
        print "\n".join(sorted(cmoproj.warnings))
        print "\n"
    print "**************************************************************************\n"
    return


def generateProjectInput(args):
    success = False
    newProj = CMOProject(projID=args.projectNum, \
                         species=args.species, \
                         pi=args.labHead, \
                         piEmail=args.labHeadEmail, \
                         requester=args.requester, \
                         requesterEmail=args.requesterEmail, \
                         analyst=args.analyst, \
                         analystEmail=args.analystEmail, \
                         projDesc=args.brief, \
                         projTitle=args.projectTitle, \
                         projCmoId=args.projectCmoId, \
                         overwrite=args.overwrite, \
                         merge=args.merge, \
                         noPipelineRun=args.noPipelineRun, \
                         rerun=args.rerun, \
                         rerunReason=args.rerunReason, \
                         rerunDestination=args.rerunDestination, \
                         readmeFile=args.readme, \
                         manifestFiles=args.manifestFiles, \
                         pairingFile=args.pairingSheet, \
                         projManager=args.manager, \
                         ignoreWarnings=args.ignoreWarnings, \
                         noPortal=args.noPortal, \
                         testing=args.testing, \
                         deliverTo=args.deliverTo, \
                         exome=args.exome, \
                         innovation=args.innovation, \
                         manifestSource=args.manifestSource)
    newProj.validateProject()

    ## attempt to write project files if and only if there are zero
    ## errors or warnings, or there are zero errors and the user 
    ## has set the ignoreWarnings flag
    if newProj.isValid():
        if (len(newProj.warnings) == 0 or args.ignoreWarnings) and not args.validateOnly:
            success = newProj.writeProjectFiles()

    if success:
        ## save pairing file, manifest files, and readme
        ## uploaded by project manager
        newProj.savePMFiles()

        ## send email to Caitlin and Krista notifying them of new project submission
        subj = "[cmo-project-start] %s: pipeline input files have been generated" % newProj.analysisID
        text = "".join(["Pipeline input files for ", newProj.analysisID, " are ready on seq.\n\n", \
                        "Please check and copy necessary files from \n\n\tluna:", newProj.projDir, "\n\n", \
                        "to\n\n\tluna:", newProj.CMO_PROJECTS_ROOT, "/", newProj.analysisID, "\n\n", \
                        "and notify Nick and Mono.\n\n", \
                        "Contact ", newProj.projManager, " if there are any issues with this project."])
        newProj.sendEmail(subject=subj, body=text)

        ## send email to PM saying we received the submission
        if not args.testing:
            recipients = [newProj.projManagerEmails[newProj.projManager]]
            subj = "[cmo-project-start] %s: pipeline input files have been generated" % newProj.analysisID
            text = "".join(["Pipeline input files for ", newProj.analysisID, " are ready.\n\n", \
                            "BIC has been notified and will begin the pipeline shortly.\n\n", \
                            "Reply to this email if there are any issues in the meantime."])
            newProj.sendEmail(recipients=recipients, subject=subj, body=text)

        ## finally, print success message
        printSuccess()

    else:
        ## print out errors and warnings
        printFail(newProj)

    ## log every run of the validator    
    action = "Pipeline Prep"
    if newProj.ignoreWarnings:
        reason = "manifest validation PASSED"
        result = "project submitted; waiting for BIC to check files generated"
    else:
        if len(newProj.errors + newProj.warnings) == 0:
            result = "no errors or warnings; waiting for project to be submitted"
            reason = "manifest validation PASSED"
        elif len(newProj.errors) > 0:
            result = "waiting for re-validation"
            reason = "; ".join(["manifest validation FAILED"] + newProj.errors + newProj.warnings)
        elif len(newProj.warnings) > 0:
            result = "no errors; waiting for project to be submitted"
            reason = "; ".join(["manifest validation PASSED with warnings"] + newProj.warnings)
    newProj.updateProjectStartLog(person=newProj.projManager, action=action, reason=reason, result=result)

    ## keep track of runs/reruns
    ## make sure this is the very last thing done 
    ## as it is used to mark project as 
    ## "ready for analysis"
    if success:
        time.sleep(1)
        newProj.updatePipelineRunLog()
        newProj.updateRunLog()
        if newProj.errors:
            print newProj.errors
        sys.exit(0)
    sys.exit(-1)


if __name__ == '__main__':
    args = getParsedArgs()

    ## if min required info for new project is given, validate project
    if args.projectNum and args.species and args.labHead and \
            args.labHeadEmail and args.requester and args.requesterEmail and \
            args.brief and args.projectTitle and args.projectCmoId and \
            args.readme and args.manifestFiles and args.pairingSheet and \
            args.manager:
        generateProjectInput(args)
    else:
        print "ERROR: Required info missing."
        sys.exit(-1)
