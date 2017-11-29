#! /opt/bin/python
import os
import re
import shutil
import smtplib
import sys
import time
from collections import namedtuple
from datetime import datetime
from email.mime.text import MIMEText
from os import listdir
from subprocess import Popen, PIPE
from urllib import urlopen

import lib.xlsx
import lims_rest
from CMOUtilities import *
from PORTAL_CONSTANTS import *


class CMOProject():
    def __init__(self, projID=None, pi=None, piEmail="NA", requester=None, \
                 requesterEmail="NA", analyst="NA", analystEmail="NA", \
                 platform=None, overwrite=False, \
                 projTitle=None, projCmoId=None, projDesc=None, merge=None, \
                 species=None, readmeFile=None, manifestFiles=None, \
                 pairingFile=None, projManager=None, analysisID=None, \
                 ignoreWarnings=False, noPortal=False, rerun=False, rerunReason=None, \
                 rerunDestination=None, testing=False, addFiles=None, \
                 freeze=False, deliverTo=None, noPipelineRun=False, \
                 exome=False, innovation=False, manifestSource="LIMS"):

        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tInitializing project..."

        self.projID = projID[0]
        if not analysisID:
            self.analysisID = "Proj_" + self.projID
        else:
            self.analysisID = analysisID
        self.projStartLog = self.analysisID + "_cmo_project_start.log"
        self.pipelineRunDir = None
        self.pipelineRunLog = self.analysisID + "_pipeline_run.log"
        self.newProjLog = self.analysisID + "_cmo_project.log"
        self.newRunLog = self.analysisID + "_runs.log"

        self.pi = pi
        self.piEmail = piEmail.lower() if not piEmail == "NA" else piEmail
        self.requester = requester
        self.requesterEmail = requesterEmail.lower() if not requesterEmail == "NA" else requesterEmail
        self.analyst = analyst
        self.analystEmail = analystEmail.lower() if analystEmail and not analystEmail == "NA" else analystEmail
        self.platform = platform
        self.species = species
        self.projTitle = projTitle
        self.projCmoId = projCmoId
        self.projDesc = projDesc
        self.overwrite = overwrite
        self.runNum = 1
        self.runNumGuessed = False
        self.rerun = rerun
        self.rerunReason = rerunReason
        self.rerunDestination = rerunDestination
        self.merge = merge
        self.readme = readmeFile
        self.manifests = manifestFiles
        self.pairingFile = pairingFile
        self.projManager = formatFullName(projManager)
        self.ignoreWarnings = ignoreWarnings
        self.noPortal = noPortal
        self.testing = testing
        self.addFiles = addFiles
        self.freeze = freeze
        self.deliverTo = deliverTo
        self.noPipelineRun = noPipelineRun
        self.exome = exome
        self.innovation = innovation
        self.manifestSource = manifestSource

        self.unmatchedNormals = set()
        self.errors = []
        self.warnings = []
        self.sampleInfo = None
        self.relabels = None
        self.sampleMap = None
        self.samplePairing = None

        self.projManagerEmails = {'Caitlin Bourque': 'bourquec@mskcc.org', \
                                  'Bourque, Caitlin': 'bourquec@mskcc.org', \
                                  'Duygu Selcuklu': 'selcukls@mskcc.org', \
                                  'Ravichandran, Krithika': 'ravichak@mskcc.org', \
                                  'Krithika Ravichandran': 'ravichak@mskcc.org', \
                                  'Vanness, Katelynd': 'vannessk@mskcc.org', \
                                  'Katelynd Vanness': 'vannessk@mskcc.org', \
                                  'Selcuklu, S. Duygu': 'selcukls@mskcc.org', \
                                  'Selcuklu, Duygu': 'selcukls@mskcc.org', \
                                  'Caitlin Jones': 'byrnec@mskcc.org', \
                                  'Amy Webber': 'webbera@mskcc.org', \
                                  'Webber, Amy': 'webbera@mskcc.org', \
                                  'Nick Socci': 'soccin@mskcc.org', \
                                  'Socci, Nicholas': 'soccin@mskcc.org', \
                                  'Socci, Nick': 'soccin@mskcc.org'}

        self.CMO_PROJECTS_ROOT = "/ifs/projects/CMO"
        self.FINAL_PROJECT_ROOT = "/".join([self.CMO_PROJECTS_ROOT, self.analysisID])
        self.OLD_PROJECT_ROOT = None
        self.emailChange = False
        if self.projID[0] == '0' and re.match(r'^Proj_\d{4}\D', self.projID):
            self.OLD_PROJECT_ROOT = "/".join([self.CMO_PROJECTS_ROOT, self.analysisID.replace("Proj_0", "Proj_")])
        if self.piEmail and self.requesterEmail:
            self.FINAL_DELIVERY_DIR = "/".join(
                ["/ifs/solres/seq", self.piEmail.split("@")[0], self.requesterEmail.split("@")[0], self.analysisID])
            self.OLD_DELIVERY_DIR = self.setOldDeliveredDir()
            self.setRunNum()

    def projectExists(self):
        if not os.path.isdir(self.FINAL_PROJECT_ROOT) and self.OLD_PROJECT_ROOT and not os.path.isdir(
                self.OLD_PROJECT_ROOT):
            return False
        return True

    def addFileExistsError(self, file):
        self.errors.append("".join(["ERROR: file ", file, " exists. Not overwriting. ", \
                                    "Use '--overwrite' to overwrite existing files."]))

    def makeFileName(self, projectNo, filename):
        return "Proj_%s_sample_%s.txt" % (projectNo, filename)

    def setRunNum(self):
        lastRun = 0
        contents = []

        if self.OLD_PROJECT_ROOT:
            oldAnalysisID = self.analysisID.replace("Proj_0", "Proj_")
        try:
            if not os.path.isdir(self.FINAL_DELIVERY_DIR) and self.OLD_PROJECT_ROOT:
                if not self.OLD_DELIVERY_DIR:
                    if self.rerunReason and not self.rerunReason.strip() == "":
                        self.warnings.append("".join(["WARNING: A 'reason for rerun' was given, but ", \
                                                      "we have no record of any previous pipeline runs being delivered."]))
                elif os.path.isdir(self.OLD_DELIVERY_DIR):
                    contents.extend(os.listdir(self.OLD_DELIVERY_DIR))
                    if oldAnalysisID in self.OLD_DELIVERY_DIR:
                        self.warnings.append(
                            "".join(["WARNING: Run number is being set based on delivery directory with ID ", \
                                     oldAnalysisID, ". If this is incorrect email cmo-project-start@cbio.mskcc.org."]))
            else:
                contents.extend(os.listdir(self.FINAL_DELIVERY_DIR))
            if contents:
                for file in contents:
                    if file.startswith('r_'):
                        try:
                            thisRun = int(file.replace('r_', '').lstrip('0'))
                            if thisRun > lastRun:
                                lastRun = thisRun
                        except:
                            continue
        except OSError as e:
            self.warnings.append(
                "".join(["WARNING: Could not access delivery directory to determine run number: ", e.strerror]))

        if lastRun == 0:
            self.runNumGuessed = True
            if not contents:  ## project directory is empty
                self.warnings.append(
                    "".join(["WARNING: Could not determine PIPELINE RUN NUMBER from delivery directory. Setting to: 1. " \
                             "If this is incorrect, email cmo-project-start@cbio.mskcc.org"]))
            else:
                self.warnings.append(
                    "".join(["WARNING: Could not determine exact PIPELINE RUN NUMBER from delivery directory, " \
                             "but there ARE results there. Setting PIPELINE RUN NUMBER to: 2. If this is incorrect, " \
                             "email cmo-project-start@cbio.mskcc.org"]))
                self.runNum = 2
        elif not self.noPipelineRun:
            self.runNum = lastRun + 1
        else:
            self.runNum = lastRun

        if self.runNum > 1:
            if not self.rerunReason:
                self.errors.append("ERROR: This project has been run before. Must give a reason for rerun!")
            if not self.rerunDestination in ["triage", "delivery"]:
                self.errors.append("ERROR: Invalid rerun desintation. Valid values are 'triage' and 'delivery'")

        return

    def writeRequestFile(self):
        print>> sys.stderr, "Writing request file..."

        file = "/".join([self.pipelineRunDir, "_".join([self.analysisID, "request.txt"])])
        comments = []

        if not os.path.exists(file) or self.overwrite:
            with open(file, 'w') as fp:
                print>> fp, "PI:", self.piEmail.split("@")[0]
                print>> fp, "PI_Name:", self.pi
                print>> fp, "PI_E-mail:", self.piEmail.lower()
                print>> fp, "Investigator:", self.requesterEmail.split("@")[0]
                print>> fp, "Investigator_Name:", self.requester
                print>> fp, "Investigator_E-mail:", self.requesterEmail.lower()
                print>> fp, "Data_Analyst:", self.analyst
                print>> fp, "Data_Analyst_E-mail:", self.analystEmail
                print>> fp, "ProjectID:", self.analysisID
                print>> fp, "ProjectName:", self.projCmoId
                print>> fp, "ProjectTitle:", self.projTitle
                print>> fp, "ProjectDesc:", self.projDesc
                print>> fp, "Project_Manager:", self.projManager
                if self.projManager in self.projManagerEmails:
                    print>> fp, "Project_Manager_Email:", self.projManagerEmails[self.projManager]
                else:
                    print>> fp, "Project_Manager_Email:", "unknown"
                print>> fp, "Institution:", "cmo"
                print>> fp, "Species:", self.species
                print>> fp, "RunID:", ",".join(self.sampleMap.runIDset)
                print>> fp, "NumberOfSamples:", len(self.sampleInfo.sampleRecs)
                print>> fp, "TumorType:", self.sampleInfo.portalTumorType
                print>> fp, "Assay:", self.sampleInfo.bait_version
                if self.exome:
                    print>> fp, "AssayPath:", self.sampleInfo.designFile
                else:
                    print>> fp, "DesignFile:", self.sampleInfo.designFile
                print>> fp, "SpikeinDesignFile:", self.sampleInfo.spikeinDesignFile
                pl = 'variants' if self.exome else 'dmp'
                print>> fp, "Pipelines:", pl
                print>> fp, "Run_Pipeline:", pl
                if self.noPipelineRun:
                    print>> fp, "Note: No pipeline run needed."
                print>> fp, "RunNumber:", self.runNum
                if int(self.runNum) > 1 or self.rerunReason:
                    if self.rerunReason:
                        print>> fp, "Reason_for_rerun:", self.rerunReason
                    else:
                        print>> fp, "Reason_for_rerun:", "Unknown. Contact", self.projManager
                    if self.rerunDestination == "triage":
                        comments.append("Send results to TRIAGE")
                    else:
                        comments.append("Send results OUT FOR DELIVERY")
                dt = ",".join(self.deliverTo) if self.deliverTo else "NA"
                print>> fp, "DeliverTo:", dt
                if self.exome:
                    comments.append("Send results to Nicholas Socci")
                if self.noPortal:
                    comments.append("This project is NOT to be imported to the cBio Portal")
                print>> fp, "Comments:", "; ".join(comments)
                print>> fp, "DateOfLastUpdate:", str(datetime.now()).split()[0]
                print>> fp, "AmplificationTypes:", "NA"
                print>> fp, "LibraryTypes:", "NA"
                print>> fp, "Strand:", "NA"
                if self.emailChange:
                    print>> fp, "WARNING: PI or Requester email has changed. Older project runs must be moved to the new location."
                if self.runNumGuessed:
                    print>> fp, "WARNING: RUN NUMBER MAY NOT BE ACCURATE"
                if self.OLD_PROJECT_ROOT and os.path.isdir(self.OLD_PROJECT_ROOT):
                    print>> fp, "WARNING: This project appears to already exist under a different ID (see " + self.OLD_PROJECT_ROOT + ")"
                if not self.manifestSource == "LIMS":
                    print>> fp, "WARNING: Manifest was uploaded manually. Sample information may differ between manifest and LIMS"
        else:
            self.addFileExistsError(file)

        return

    def writePairingFile(self):
        print>> sys.stderr, "Writing pairing file..."

        file = "/".join([self.pipelineRunDir, self.makeFileName(self.projID, "pairing")])
        if not os.path.exists(file) or self.overwrite:
            with open(file, 'w') as fp:
                for t in sorted(self.samplePairing.pairingTable.keys()):
                    try:
                        print>> fp, "\t".join([sampleOutFormat(self.samplePairing.pairingTable[t]), sampleOutFormat(t)])
                    except:
                        if self.exome:
                            for n in self.samplePairing.pairingTable[t]:
                                print>> fp, "\t".join([sampleOutFormat(n), sampleOutFormat(t)])
                if self.exome:
                    for t in sorted(self.unmatchedNormals):
                        print>> fp, "\t".join([sampleOutFormat(t), "na"])
        else:
            self.addFileExistsError(file)

        return

    def writeMappingFile(self):
        print>> sys.stderr, "Writing mapping file..."
        file = "/".join([self.pipelineRunDir, self.makeFileName(self.projID, "mapping")])
        if not os.path.exists(file) or self.overwrite:
            pathDb = self.sampleMap.pathDb
            with open(file, 'w') as fp:
                #
                # Regular (non-pool) samples and unmatched normals
                #
                if not self.exome:
                    samples = set(self.samplePairing.pairingTable.keys() + \
                                  self.samplePairing.pairingTable.values() + \
                                  list(self.unmatchedNormals))
                else:
                    samples = set(self.samplePairing.pairingTable.keys() + \
                                  list(self.unmatchedNormals))
                    for nSets in self.samplePairing.pairingTable.values():
                        if isinstance(nSets, set):
                            samples.update(set(nSets))
                        else:
                            samples.add(nSets)
                for si in samples:
                    if si.lower() == "na":
                        continue
                    try:
                        seqDirs = sorted(self.sampleMap.sampleSequenceDirs[si])
                        if len(seqDirs) == 0:
                            self.errors.append(
                                "ERROR: No sequence data for sample" + si)  ## THIS SHOULDN'T HAPPEN AT THIS POINT
                            continue
                        for pi in seqDirs:
                            if pi in self.sampleMap.relocatedSeqs:
                                finalDir = self.sampleMap.relocatedSeqs[pi]
                            else:
                                finalDir = pi
                            print>> fp, "\t".join(
                                ["_1", sampleOutFormat(si), pathDb[pi].runID, finalDir, pathDb[pi].PE])
                    except KeyError, e:  ## normals are in this dict, but can just be skipped
                        continue

                samples = self.sampleMap.normalPools
                for ni in samples:
                    sampleName = sampleOutFormat(ni.poolName)
                    pi = ni.path
                    ## check to see if a temp directory of fastqs has been
                    ## created for this sample; this would happen when
                    ## multiple barcodes were used and we selected one of them
                    ## to include in analysis
                    if ni.path in self.sampleMap.relocatedSeqs:
                        pi = self.sampleMap.relocatedSeqs[ni.path]
                    print>> fp, "\t".join(["_1", sampleName, pathDb[ni.path].runID, pi, pathDb[ni.path].PE])

        else:
            self.addFileExistsError(file)

        return

    def writeGroupingFile(self):
        print>> sys.stderr, "Writing grouping file..."
        file = "/".join([self.pipelineRunDir, self.makeFileName(self.projID, "grouping")])
        if not os.path.exists(file) or self.overwrite:
            with open(file, 'w') as fp:
                for (groupNo, pID) in enumerate(sorted(self.sampleInfo.patientDb)):
                    for si in self.sampleInfo.patientDb[pID]:
                        print>> fp, "\t".join([si.SampleNameOutFormat, makeGroupLabel(groupNo)])
                maxGroupNo = groupNo
                #
                # Normal pools in their own groups
                #             
                for ui in self.sampleMap.normalPools:
                    sampleName = sampleOutFormat(ui.poolName)
                    maxGroupNo += 1
                    print>> fp, "\t".join([sampleName, makeGroupLabel(maxGroupNo)])

        else:
            self.addFileExistsError(file)

        return

    def writePortalConfigFile(self, currentFile=None, **kwargs):
        print>> sys.stderr, "Writing portal config file..."
        ## initialize all standard portal config keys
        pc = {'project': '""', \
              'name': '""', \
              'desc': '""', \
              'invest_name': '""', \
              'invest': '""', \
              'tumor_type': '""', \
              'assay_type': '""', \
              'inst': '"cmo"', \
              'groups': '"COMPONC"', \
              'data_clinical': '""', \
              'maf_desc': '""', \
              'maf': '""', \
              'cna': '""', \
              'cna_seg': '""', \
              'cna_seg_desc': '""', \
              'date_of_last_update': '""'
              }

        qw = ['"', '"']
        ## if updating a file that already exists, populate pc with 
        ## existing values first, then update with kwargs
        if currentFile:
            try:
                with open(currentFile, 'r') as cpcf:
                    for line in cpcf:
                        key = line.strip().split('=')[0]
                        val = line.strip()[line.strip().index("=") + 1:]
                        if not key == 'inst':  ## we always want inst=cmo, so don't change this
                            pc[key] = val
            except IOError:
                self.errors.append("ERROR: Couldn't read file: " + currentFile)
            if len(kwargs) > 0:
                for key in kwargs:
                    pc[key] = kwargs[key].lstrip('"').rstrip('"').join(qw)
        else:
            ## if files are being generated for reason other than a run of the pipeline,
            ## check to see if a portal config already exists for this project. Use values
            ## in the existing config in order to propagate any previously run redactions 
            if self.projectExists():
                epcf = None

                if os.path.isfile(os.path.join(self.FINAL_PROJECT_ROOT, self.analysisID + "_portal_conf_latest.txt")):
                    epcf = os.path.join(self.FINAL_PROJECT_ROOT, self.analysisID + "_portal_conf_latest.txt")
                elif self.OLD_PROJECT_ROOT and os.path.isfile(os.path.join(self.OLD_PROJECT_ROOT,
                                                                           self.analysisID.replace("Proj_0",
                                                                                                   "Proj_") + "_portal_conf_latest.txt")):
                    epcf = os.path.join(self.OLD_PROJECT_ROOT,
                                        self.analysisID.replace("Proj_0", "Proj_") + "_portal_conf_latest.txt")
                if epcf:
                    if self.noPipelineRun:
                        try:
                            with open(epcf, 'r') as cpcf:
                                for line in cpcf:
                                    key = line.strip().split('=')[0]
                                    val = line.strip()[line.strip().index("=") + 1:]
                                    if not key == 'inst':  ## we always want inst=cmo, so don't change this
                                        pc[key] = val
                        except IOError:
                            self.warnings.append("".join(["WARNING: Found existing portal config, ", epcf, \
                                                          ", but could not read it. THIS MEANS ANY REDACTIONS", \
                                                          " PREVIOUSLY SUBMITTED MAY BE UNDONE."]))

            ## update any values that could have changed via the manifests
            ## or other PM input
            groups = "COMPONC;" + self.piEmail.split("@")[0].upper()
            newmaf_desc = self.sampleInfo.bait_version + " " + maf_desc  ## from PORTAL_CONSTANTS.py
            pc['project'] = self.projID.join(qw)
            pc['name'] = self.projTitle.join(qw)
            pc['desc'] = self.projDesc.join(qw)
            pc['invest_name'] = self.pi.join(qw)
            pc['invest'] = self.piEmail.split("@")[0].join(qw)
            pc['groups'] = groups.join(qw)
            pc['tumor_type'] = self.sampleInfo.portalTumorType.join(qw)
            pc['assay_type'] = self.sampleInfo.bait_version.join(qw)
            pc['data_clinical'] = "/".join(
                [self.FINAL_PROJECT_ROOT, self.makeFileName(self.projID, "data_clinical")]).join(qw)
            if pc['maf_desc'] == '""':
                pc['maf_desc'] = newmaf_desc.join(qw)
            if pc['cna_seg_desc'] == '""':
                pc['cna_seg_desc'] = cna_seg_desc.join(qw)  ## from PORTAL_CONSTANTS.py

        file = "/".join([self.pipelineRunDir, "_".join([self.analysisID, "portal_conf.txt"])])
        pc['date_of_last_update'] = str(datetime.now()).split()[0].join(qw)

        if not os.path.exists(file) or self.overwrite:
            try:
                with open(file, 'w') as fp:
                    for key, val in pc.iteritems():
                        print>> fp, "=".join([key, val])
            except IOError, e:
                print e
        else:
            self.addFileExistsError(file)

    def writePatientSampleFile(self):
        print>> sys.stderr, "Writing patient sample file..."
        samplePatientHeader = "\t".join(["Pool", "Sample_ID", "Collab_ID", "Patient_ID", "Class", \
                                         "Sample_type", "Input_ng", "Library_yield", "Pool_input", \
                                         "Bait_version", "Sex"])
        file = "/".join([self.pipelineRunDir, self.makeFileName(self.projID, "patient")])
        if not os.path.exists(file) or self.overwrite:
            with open(file, 'w') as fp:
                print>> fp, samplePatientHeader
                for si in sorted(self.sampleInfo.sampleRecs):
                    out = []
                    samp = self.sampleInfo.sampleRecs[si]
                    out.append(fixOutput(self.analysisID))
                    out.append(fixOutput(samp.SampleNameOutFormat))
                    out.append(fixOutput(sampleOutFormat(normalizeSampleNames(samp.INVESTIGATOR_SAMPLE_ID))))
                    out.append(fixOutput(samp.CMO_PATIENT_ID))
                    out.append(fixOutput("Normal" if samp.SAMPLE_CLASS.upper() == "NORMAL" else "Tumor"))
                    out.append(fixOutput(samp.SPECIMEN_PRESERVATION_TYPE))
                    out.append(getattr(samp, "LIBRARY_INPUT[ng]"))
                    out.append(getattr(samp, "LIBRARY_YIELD[ng]"))
                    out.append(getattr(samp, "CAPTURE_INPUT[ng]"))
                    if samp.SEX.lower() == "unknown" or samp.SEX == "":
                        sex = "na"
                    else:
                        sex = samp.SEX
                    print>> fp, "\t".join(out) + "\t" + self.sampleInfo.bait_version + "\t" + sex

                for ui in self.sampleMap.normalPools:
                    out = []
                    sampleName = sampleOutFormat(ui.poolName)
                    samp = self.sampleInfo.normalPoolRecs[ui.poolName]
                    out.append(fixOutput(self.analysisID))
                    out.append(fixOutput(sampleName))
                    out.append(fixOutput(sampleName))
                    out.append(fixOutput(sampleName))
                    out.append("PoolNormal")
                    out.append(fixOutput(samp.SPECIMEN_PRESERVATION_TYPE))
                    # out.append("FFPE" if sampleName.find("FFPE")>-1 else "Frozen")
                    out.append(getattr(samp, "LIBRARY_INPUT[ng]"))
                    out.append(getattr(samp, "LIBRARY_YIELD[ng]"))
                    out.append(getattr(samp, "CAPTURE_INPUT[ng]"))
                    print>> fp, "\t".join(out) + "\t" + self.sampleInfo.bait_version + "\t" + "na"

        else:
            self.addFileExistsError(file)

        return

    def writeClinicalFile(self):
        print>> sys.stderr, "Writing data clinical file..."
        ##CLINICAL_FILE_HEADER=MANIFEST_FILE_HEADER##
        dataClinicalRosetta = """
        SAMPLE_ID=SampleNameOutFormat
        PATIENT_ID=CMO_PATIENT_ID
        COLLAB_ID=INVESTIGATOR_SAMPLE_ID
        SAMPLE_TYPE=SAMPLE_CLASS
        GENE_PANEL=CAPTURE_BAIT_SET
        ONCOTREE_CODE=ONCOTREE_CODE
        SAMPLE_CLASS=SAMPLE_TYPE
        SPECIMEN_PRESERVATION_TYPE=SPECIMEN_PRESERVATION_TYPE
        SEX=SEX
        TISSUE_SITE=TISSUE_SITE
        """

        dataClinicalStone = makeRosettaStone(dataClinicalRosetta)

        file = "/".join([self.pipelineRunDir, self.makeFileName(self.projID, "data_clinical")])
        if not os.path.exists(file) or self.overwrite:
            with open(file, 'w') as fp:
                print>> fp, "\t".join(dataClinicalStone)
                for ti in sorted(self.sampleInfo.tumors):
                    trec = self.sampleInfo.sampleRecs[ti]
                    if trec.SEX.lower() == "unknown":
                        trec.SEX = "na"
                    rec = map(fixOutput, [trec[dataClinicalStone[x]] for x in dataClinicalStone])
                    print>> fp, "\t".join(rec)
        else:
            self.addFileExistsError(file)

        return

    def writeReadmeFile(self):
        print>> sys.stderr, "Writing readme file..."
        file = "/".join([self.pipelineRunDir, self.analysisID + "_README.txt"])
        if not os.path.exists(file) or self.overwrite:
            with open(file, 'w') as fp:
                with open(self.readme, 'r') as rm:
                    for line in rm:
                        try:
                            print>> fp, line.strip().decode('utf-8').replace(u'\u201c', '"').replace(u'\u201d',
                                                                                                     '"').replace(
                                u'\u2019', "'").replace(u"\u2018", "'")
                        except UnicodeEncodeError:
                            self.errors.append(
                                "".join(["ERROR: ReadMe file contains bad character. Please flatten your readme file"]))
                print>> fp, "\n"
                print>> fp, "***** Additional info from project validator *****"
                if self.noPortal:
                    print>> fp, "DO NOT IMPORT PROJECT TO CBIO PORTAL"
                print>> fp, "Platform:", self.sampleInfo.bait_version
                print>> fp, "Project desc:", self.projDesc
                print>> fp, "Project manager:", self.projManager
                if self.merge:
                    print>> fp, "Merge with:", ",".join(["Proj_" + x for x in self.merge])
                print>> fp, "\n"
                if self.warnings:
                    print>> fp, "Warnings accepted/ignored by project manager:"
                    print>> fp, "\n".join(sorted(self.warnings))
                    print>> fp, "\n"

        else:
            self.addFileExistsError(file)

        return

    def validatePM(self):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tValidating Project Manager..."
        if not self.projManager in self.projManagerEmails:
            self.errors.append(
                "".join(["ERROR: Project Manager not in list of known Project Managers: ", self.projManager]))
        return

    def validatePI(self):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), "\tValidating PI..."
        ## if not, append warning to self.errors
        try:
            piNames = listdir("/ifs/solres/seq/")
            if not self.piEmail.split("@")[0] in piNames:
                self.warnings.append("".join(["WARNING: PI ", self.pi, " with MSKCC email ", \
                                              self.piEmail, " is not recognized. " \
                                                            "Please double check spelling."]))
                return False
        except OSError:
            self.warnings.append("WARNING: Could not validate PI ID")
        return True

    def crossCheckSamples(self):
        '''
        Check that all samples in pairing file exist in sampleRecs object;
        '''
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tCross checking samples in pairing file and in manifest..."
        sampsInPairingFile = set()
        try:
            sampsInPairingFile = set(self.samplePairing.pairingTable.keys() + self.samplePairing.pairingTable.values())
        except:
            if self.exome:
                sampsInPairingFile = set(self.samplePairing.pairingTable.keys())
                for nSets in self.samplePairing.pairingTable.values():
                    if isinstance(nSets, set):
                        sampsInPairingFile.update(set(nSets))
                    else:
                        sampsInPairingFile.add(nSets)
        for samp in self.sampleInfo.sampleRecs.keys():
            if samp not in sampsInPairingFile:
                if self.sampleInfo.sampleRecs[samp].SAMPLE_CLASS.lower() == "normal":
                    self.unmatchedNormals.add(samp)
                    if not self.exome:
                        self.warnings.append("".join(["WARNING: ", samp, \
                                                      " is a normal in the Sample Manifest but not the Pairing file.", \
                                                      " It will not go through somatic mutation analysis. Are you sure", \
                                                      " it shouldn't be in the pairing file? "]))
                else:
                    self.errors.append("".join(["ERROR: Sample ", samp, \
                                                " is in the Sample Manifest but not in the pairing file. Either ", \
                                                "Add this to the pairing file, or add a status to the Sample in ", \
                                                "the sample manifest. "]))
        for samp in sampsInPairingFile:
            ## still use isNormalPool here because the name of the normal pool samp has been changed
            ## to definitely include 'FFPE' or 'FROZEN', while the original name is what is in 
            ## self.sampleInfo.normalPoolRecs
            if not isNormalPool(samp) and not samp.lower() == 'na' and samp not in self.sampleInfo.sampleRecs.keys():
                self.errors.append("".join(["ERROR: Sample ", samp, \
                                            " is in the pairing file but is NOT in the Sample Manifest."]))

        return

    # def validateRerunNumber(self,pipelineRunLog):
    #    try:
    #        with open(pipelineRunLog,'r') as fp:
    #            lastRerun = fp.readlines()[-1].split("\t")[2]
    #    except IOError:
    #        print>>sys.stderr, "Error: Could not open file for reading: ",pipelineRunLog
    #    try:
    #        if not int(lastRerun) == int(self.rerun)-1:
    #            self.warnings.append("".join(["WARNING: Rerun number is incorrect according to our records.",\
    #                                 " Our last rerun number is: ",lastRerun]))
    #    except ValueError:
    #        return
    #    return


    def createProjectFolder(self):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tCreating new project folder..."
        # if self.testing:
        #    root = "/home/shiny/CMO/projects/testing"
        # else:
        #    root = "/home/shiny/CMO/projects"

        root = "/ifs/projects/CMO/archive"
        if self.testing:
            root += "/testing"
        if self.innovation:
            root += "/innovation"

        aDir = self.analysisID
        dDir = datetime.strftime(datetime.now(), "%Y%m%d")
        self.projDir = "/".join([root, aDir])
        self.pipelineRunDir = "/".join([self.projDir, dDir])

        try:
            if not os.path.exists(self.projDir):
                os.makedirs(self.projDir, 0775)
                os.chown(self.projDir, -1, 2104)
            if not os.path.exists(self.pipelineRunDir):
                os.makedirs(self.pipelineRunDir, 0775)
                os.chown(self.pipelineRunDir, -1, 2104)

            ## initialize project logs if they do not exist already
            ## by printing headers to file; this way downstream we
            ## can always assume we are appending to these files, without
            ## having to check every time whether they exist
            projStartLog = "/".join([self.projDir, self.projStartLog])
            pipelineRunLog = "/".join([self.projDir, self.pipelineRunLog])
            if not os.path.exists(projStartLog):
                with open(projStartLog, 'w') as fp:
                    print>> fp, "\t".join(["Date", "Time", "Name", "Action", "Reason_For_Action", "Result"])
            if not os.path.exists(pipelineRunLog):
                with open(pipelineRunLog, 'w') as fp:
                    print>> fp, "\t".join(["Date", "Time", "Rerun_Number", "Reason_For_Rerun"])
            # else:
            #    self.validateRerunNumber(pipelineRunLog)

            newProjLog = "/".join([self.projDir, self.newProjLog])
            newRunLog = "/".join([self.projDir, self.newRunLog])
            if not os.path.exists(newProjLog):
                with open(newProjLog, 'w') as fp:
                    print>> fp, "\t".join(["Date", "Time", "Name", "Category", "Detail", "Result"])
            if not os.path.exists(newRunLog):
                with open(newRunLog, 'w') as fp:
                    print>> fp, "\t".join(["Date", "Time", "Run_Number", "Reason_For_Rerun"])


        except OSError:
            self.errors.append(
                "ERROR: Problem either creating or setting permisions for directory: " + self.pipelineRunDir)

        return

    def sendEmail(self, recipients=[], subject=None, body=None):
        # recipients += ['byrne@cbio.mskcc.org','kristakaz@cbio.mskcc.org']
        recipients += ['gabow@cbio.mskcc.org', 'rezae@mskcc.org']

        if not self.testing:
            recipients += ['mpirun@cbio.mskcc.org', 'bic-request@cbio.mskcc.org']

        msgFile = 'notifyBIC.txt'
        msgFrom = 'cmo-project-start@cbio.mskcc.org'
        msgTo = ', '.join(set(recipients))

        if self.testing:
            subject = "TEST " + subject
            body = " ** THIS IS A TEST ** \n\n" + body

        ## write message body to file
        with open(msgFile, 'w') as b:
            print>> b, body

        ## construct message
        m = open(msgFile, 'rb')
        msg = MIMEText(m.read())
        m.close()
        msg['Subject'] = subject
        msg['From'] = msgFrom
        msg['To'] = msgTo

        ## send message
        s = smtplib.SMTP('localhost')
        s.sendmail(msgFrom, recipients, msg.as_string())
        s.quit()

        return

    def savePMFiles(self):
        try:
            dir = self.pipelineRunDir + "/filesUsed"
            os.mkdir(dir)

            shutil.copy(self.pairingFile, dir)
            shutil.copy(self.readme, dir)
            for man in self.manifests:
                shutil.copy(man, dir)
        except:
            self.warnings.append("WARNING: Uploaded files may not have been saved properly. \
                                 Please let us know by emailing cmo-project-start@cbio.mskcc.org")

        return

    def updatePipelineRunLog(self):
        d = str(datetime.now()).split()[0]
        t = time.strftime("%H:%M:%S")
        # r = self.rerun
        r = 'X'
        if not self.rerunReason:
            rr = "NA"
        else:
            rr = self.rerunReason
        log = "/".join([self.projDir, self.pipelineRunLog])
        try:
            with open(log, 'a') as l:
                print>> l, "\t".join([d, t, r, rr])
        except IOError:
            self.errors.append("ERROR: Could not open pipeline run log file")
        return

    def updateRunLog(self):
        d = str(datetime.now()).split()[0]
        t = time.strftime("%H:%M:%S")
        r = str(self.runNum)
        if not self.rerunReason:
            rr = "NA"
        else:
            rr = self.rerunReason
        log = "/".join([self.projDir, self.newRunLog])
        try:
            with open(log, 'a') as l:
                print>> l, "\t".join([d, t, r, rr])
        except IOError:
            self.errors.append("ERROR: Could not open pipeline run log file")
        return

    def updateProjectStartLog(self, person=None, action=None, reason=None, result=None):
        d = str(datetime.now()).split()[0]
        t = time.strftime("%H:%M:%S")
        log = "/".join([self.projDir, self.projStartLog])
        newlog = "/".join([self.projDir, self.newProjLog])
        try:
            with open(log, 'a') as l:
                print>> l, "\t".join([d, t, person, action, reason, result])
        except IOError:
            self.errors.append("ERROR: Could not open project start log")
        try:
            with open(newlog, 'a') as l:
                print>> l, "\t".join([d, t, person, action, reason, result])
        except IOError:
            self.errors.append("ERROR: Could not open NEW project start log")
        return

    def saveAdditionalFiles(self, filesToSave):
        for f in filesToSave:
            try:
                shutil.copy(f, self.pipelineRunDir)
            except IOError, e:
                self.errors.append("ERROR: Could not save file " + f)
        return

    def freezeProject(self):
        print>> sys.stderr, "Freezing project..."
        dt = datetime.strftime(datetime.now(), "%Y%m%d")
        file = "/".join([self.pipelineRunDir, self.analysisID + "_FROZEN_" + dt])
        ## create empty file
        with open(file, 'w') as fp:
            self.warnings.append("WARNING: Project FROZEN!!")
        return

    def writeUpdateReadmeFile(self, comment, updater):
        print>> sys.stderr, "Writing update readme file..."
        dt = datetime.strftime(datetime.now(), "%Y%m%d")
        file = "/".join([self.pipelineRunDir, self.analysisID + "_" + dt + "_UPDATE_README.txt"])
        if not os.path.exists(file) or self.overwrite:
            with open(file, 'w') as fp:
                print>> fp, "\t".join(["Updater", "Comment"])
                print>> fp, "\t".join([updater, comment])
        else:
            self.warnings.append("WARNING: Did not write UPDATE README file")
        return

    def checkRequired(self):
        if not self.projID:
            self.errors.append("ERROR: Required info missing - IGO project ID(s)")
        if not self.pi:
            self.errors.append("ERROR: Required info missing - PI name")
        if not self.piEmail:
            self.errors.append("ERROR: Required info missing - PI email")
        if not self.requester:
            self.errors.append("ERROR: Required info missing - Requester name")
        if not self.requesterEmail:
            self.errors.append("ERROR: Required info missing - Requester email")
        if not self.projTitle:
            self.errors.append("ERROR: Required info missing - Final Project Title")
        if not self.projDesc:
            self.errors.append("ERROR: Required info missing - Project description")
        if not self.readme:
            self.errors.append("ERROR: Required info missing - Readme.txt file")
        if not self.manifests:
            self.errors.append("ERROR: Required info missing - Sample manifest file(s)")
        if not self.pairingFile:
            self.errors.append("ERROR: Required info missing - Sample pairing file")
        if not self.projManager:
            self.errors.append("ERROR: Required info missing - Project manager")
        return

    def checkProjectExistence(self):
        if self.projectExists():
            if not self.noPipelineRun and not self.noPortal:
                if os.path.isfile(os.path.join(self.FINAL_PROJECT_ROOT, self.analysisID + "_portal_conf_latest.txt")):
                    self.warnings.append("".join(["WARNING: Any SNP or CNV redactions previously run for this", \
                                                  " project will NOT be propagated into the portal after this rerun."]))
        if os.path.isfile(os.path.join(self.pipelineRunDir, self.analysisID + "_portal_conf.txt")):
            if not self.noPipelineRun and not self.noPortal:
                self.warnings.append("".join(["WARNING: A portal config from earlier today has been found. ", \
                                              "If your previous submission included a request for SNP or CNV redactions, ", \
                                              " clicking submit may undo that request. Email cmo-project-start@cbio.mskcc.org to check."]))

        return

    def getLatestDataClinicalFile(self):
        newest = None
        dir = None
        if self.projectExists():
            if os.path.isdir(self.FINAL_PROJECT_ROOT):
                dir = self.FINAL_PROJECT_ROOT
            elif self.OLD_PROJECT_ROOT and os.path.isdir(self.OLD_PROJECT_ROOT):
                dir = self.OLD_PROJECT_ROOT
            if not dir:
                return
            # print>>sys.stderr,"Searching %s for data clinical file..." %dir
            mtime = lambda f: os.stat(os.path.join(dir, f)).st_mtime
            sorted_files = list(sorted(os.listdir(dir), key=mtime))
            # print sys.stderr,sorted_files
            for f in sorted_files:
                if "_sample_data_clinical" in f:
                    newest = os.path.join(dir, f)
        # print>>sys.stderr,"latest data clinical file: %s" %newest
        return newest

    def getLatestRequestFile(self):
        newest = None
        dir = None
        if self.projectExists():
            if os.path.isdir(self.FINAL_PROJECT_ROOT):
                dir = self.FINAL_PROJECT_ROOT
            elif self.OLD_PROJECT_ROOT and os.path.isdir(self.OLD_PROJECT_ROOT):
                dir = self.OLD_PROJECT_ROOT
            if not dir:
                return
            # print>>sys.stderr,"Searching %s for data clinical file..." %dir
            mtime = lambda f: os.stat(os.path.join(dir, f)).st_mtime
            sorted_files = list(sorted(os.listdir(dir), key=mtime))
            # print sys.stderr,sorted_files
            for f in sorted_files:
                if "_request.txt" in f:
                    newest = os.path.join(dir, f)
        # print>>sys.stderr,"latest data clinical file: %s" %newest
        return newest

    def setOldDeliveredDir(self):
        # Here, check old project pi & requester name and email. If they don't match put a warning. 
        # Then use these values to create OLD Delivery directory
        oldDir = self.FINAL_DELIVERY_DIR.replace("Proj_0", "Proj_")

        # If PI was not given to this script, return empty.
        if not self.pi:
            return None

        if self.projectExists():
            oldProjID = oldPiName = oldInvName = oldInv = oldPi = ""
            erf = self.getLatestRequestFile()
            if not erf:
                return None
            try:
                with open(erf, 'r') as rf:
                    for line in rf:
                        line = line.rstrip()
                        if line.startswith("PI_Name:"):
                            oldPiName = line.split(": ", 1)[1];
                            newPI = self.pi
                            if "," in newPI and "," not in oldPiName:
                                newPI = newPI.split(", ")[1] + " " + newPI.split(", ")[0]
                            if newPI.lower() != oldPiName.lower():
                                self.warnings.append(
                                    "WARNING: PI from older project and new project do not match. Old: " + oldPiName + " New: " + newPI)
                        if line.startswith("ProjectID:"):
                            oldProjID = line.split(": ", 1)[1]

                        if line.startswith("PI:"):
                            oldPi = line.split(": ", 1)[1];
                            if self.piEmail.split("@")[0].lower() != oldPi.lower():
                                self.emailChange = True
                                self.warnings.append(
                                    "WARNING: PI E-mail from older project and new project do not match. Old: " + oldPi + " New: " +
                                    self.piEmail.split("@")[0])
                        if line.startswith("Investigator_Name:"):
                            oldInvName = line.split(": ", 1)[1];
                            newInv = self.requester
                            if "," in newInv and "," not in oldInvName:
                                newInv = newInv.split(", ")[1] + " " + newInv.split(", ")[0]
                            if newInv.lower() != oldInvName.lower():
                                self.warnings.append(
                                    "WARNING: Requester from older project and new project do not match. Old: " + oldInvName + " New: " + newInv)
                        if line.startswith("Investigator:"):
                            oldInv = line.split(": ", 1)[1];
                            if self.requesterEmail.split("@")[0].lower() != oldInv.lower():
                                self.emailChange = True
                                self.warnings.append(
                                    "WARNING: Requester E-mail from older project and new project do not match. Old: " + oldInv + " New: " +
                                    self.requesterEmail.split("@")[0])
            except IOError:
                self.errors.append(
                    "ERROR: could not open existing request file. Please email cmo-project-start@cbio.mskcc.org")
        if self.emailChange and oldProjID:
            oldDir = "/".join(["/ifs/solres/seq", oldPi, oldInv, oldProjID])
            if not os.path.isdir(oldDir):
                # Cannot find old directory based on the e-mail changes.
                self.warnings.append(
                    "WARNING: PI/Requester e-mail has changed, but I cannot find old delivered directory to get correct run number")
                return None

        return oldDir

    def compareToExistingProject(self):
        if self.projectExists() and self.noPipelineRun:
            print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
                '%Y-%m-%d %H:%M:%S'), "\tComparing new project to old project files..."
            ## compare list of tumors in new manifest to that in old data clinical file
            # edcf = os.path.join(self.FINAL_PROJECT_ROOT,self.analysisID+"_sample_data_clinical.txt")
            # if not os.path.isfile(edcf):
            #    edcf = os.path.join(self.OLD_PROJECT_ROOT,self.analysisID.replace("Proj_0","Proj_")+"_sample_data_clinical.txt")
            edcf = self.getLatestDataClinicalFile()

            if not edcf:
                self.errors.append("ERROR: Could not find existing data clinical file")
                return
            try:
                with open(edcf, 'r') as edc:
                    edc.readline()
                    existingTumors = [line.split("\t")[0] for line in edc]
                existingTumors.sort()
                newTumors = [sampleOutFormat(x) for x in self.sampleInfo.tumors]
                newTumors.sort()
                if not existingTumors == newTumors:
                    inNewNotOld = list(set(newTumors) - set(existingTumors))
                    inOldNotNew = list(set(existingTumors) - set(newTumors))
                    if len(inNewNotOld) > 0:
                        self.errors.append(
                            "ERROR: the following tumors are in the new manifest but are not in the existing data clinical file: " + str(
                                inNewNotOld) + \
                            " Resolve this by entering statuses for these samples in the manifest.")
                    if len(inOldNotNew) > 0:
                        self.errors.append(
                            "ERROR: the following tumors are in the existing data clinical file but are missing from the new manifest: " + str(
                                inOldNotNew) + "file: " + edcf)
            except IOError:
                self.errors.append(
                    "ERROR: could not open existing data clinical file. Please email cmo-project-start@cbio.mskcc.org")

        return

    def validateSpecies(self):
        if self.sampleInfo.includesXenografts and not self.species.lower() == 'xenograft':
            self.species = "Xenograft"
        return

    def validateProject(self):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tValidating manifest and pairing file..."
        self.createProjectFolder()

        ## validate command line input
        self.checkRequired()
        self.checkProjectExistence()
        self.validatePI()
        self.validatePM()

        ## validate project files
        self.sampleInfo = SampleInfo(self.manifests, self.exome)
        self.validateSpecies()

        if self.sampleInfo.isValid():
            self.warnings += self.sampleInfo.warnings
            self.sampleMap = SampleMap(self.sampleInfo, self.projID, self.piEmail, self.pipelineRunDir, self.exome,
                                       self.sampleInfo.relabels)
            if self.sampleMap.isValid():
                self.warnings += self.sampleMap.warnings
                ## I have to assing sampleMap.sampleInfo to self.sampleInfo because the pooled normal names get changed
                ## and it needs to be relected in the sampleInfo dict (just because I wanted it to be correct
                self.sampleInfo = self.sampleMap.sampleInfo
                self.samplePairing = SamplePairing(self.pairingFile, self.sampleInfo, self.sampleMap, self.exome)
                if self.samplePairing.isValid():
                    self.warnings += self.samplePairing.warnings
                    self.crossCheckSamples()
                    if self.projectExists():
                        self.compareToExistingProject()
                else:
                    self.errors += self.samplePairing.errors
                    self.warnings += self.samplePairing.warnings
            else:
                self.errors += self.sampleMap.errors
                self.warnings += self.sampleMap.warnings
        else:
            self.errors += self.sampleInfo.errors
            self.warnings += self.sampleInfo.warnings

        self.errors = list(set(self.errors))
        self.warnings = list(set(self.warnings))

        if len(self.errors) > 0:
            print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
                '%Y-%m-%d %H:%M:%S'), "\tDone validating. Project is INVALID."
            return False
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tDone validating. Project is VALID."
        return True

    def isValid(self):
        '''
        A project is considered valid if there are no errors.
        Warnings do NOT render a project invalid.
        '''
        if len(self.errors) == 0:
            return True
        return False

    def writeProjectFiles(self):
        '''
        Only write project files if there are NO errors and
        any warnings have been checked and accepted
        '''
        if self.isValid() and (len(self.warnings) == 0 or self.ignoreWarnings):
            self.writePairingFile()
            self.writeRequestFile()
            self.writeMappingFile()
            self.writeGroupingFile()
            self.writeClinicalFile()
            self.writePatientSampleFile()
            self.writeReadmeFile()
            if not self.noPortal:
                self.writePortalConfigFile()
            if self.isValid():
                return 1
        return 0


class SampleInfo():
    def __init__(self, manifests, exome):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tParsing manifest(s)..."
        self.projectIds = set()
        self.manifests = manifests
        self.exome = exome
        self.sampleRecs = {}
        self.normalPoolRecs = {}
        self.relabels = {}
        self.fixSampleNameMap = {}
        self.tumors = []
        self.portalTumorType = ''
        self.patientDb = {}
        self.impactPLUS = ''
        self.bait_version = ''
        self.exonsFile = ''
        self.baitsIntervalFile = ''
        self.tilingFile = ""
        self.fullBedFile = ""
        self.designFile = ''
        self.spikeinDesignFile = ''
        self.includesXenografts = False
        self.DESIGN_DIR = "/ifs/projects/CMO/targets/designs"
        self.errors = []
        self.warnings = []
        self.validateManifests()

    def verifySampleRenameHeaders(self, xlsFile, headerList):
        filename = os.path.basename(xlsFile)
        TrueHeaderList = ["OldName", "NewName"]
        for item in TrueHeaderList:
            if item not in headerList:
                self.errors.append("".join(["ERROR: Column '", item, "' is either misspelled or missing from ", \
                                            "the SampleRenames spreadsheet of the SampleManifest file ", filename,
                                            "."]))
        return

    def captureBaitSetFound(self, cbs):
        # print>>sys.stderr,"Searching for capture bait set in design directory..."
        for root, dirs, files in os.walk(self.DESIGN_DIR):
            if cbs in dirs:
                return True
        return False

    def storeRelabels(self):
        '''
        Store renames from ALL manifests together
        '''
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tStoring sample relabels..."
        for (fileNum, xlfile) in enumerate(self.manifests):
            for rec in lib.xlsx.DictReader(xlfile, sheetName="SampleRenames"):
                self.verifySampleRenameHeaders(xlfile, rec._MetaStruct__fields)
                if (rec.OldName != "" and rec.NewName == "") or (rec.OldName == "" and rec.NewName != ""):
                    self.errors.append(
                        "ERROR: in SampleRenames sheet. Either there is no old name, or there is no new name.")
                else:
                    self.relabels[normalizeSampleNames(rec.OldName)] = normalizeSampleNames(rec.NewName)
        return

    def validateBaits(self, captureBaitSet, spikeInGenes, sampleName):
        '''
        Check that files exist for specified baits, and that baits 
        are the same for all samples in manifest.
        '''
        # print>>sys.stderr,"Validating baits..."
        bait_info = ''

        ## check that capture bait set specified is in the list of known bait sets
        if captureBaitSet in capture_bait_info or self.captureBaitSetFound(captureBaitSet):
            bait_info = captureBaitSet
        else:
            self.errors.append("".join(["ERROR: CaptureBaitSet ", captureBaitSet, " is not in the list of possible ", \
                                        "CaptureBaitSets. Possible bait sets = ", \
                                        str(capture_bait_info.keys()), \
                                        " Email cmo-project-start@cbio.mskcc.org to have an", \
                                        " addition made to the list if need be."]))

        ## add custom part of assay name if need be, check that it is consistent
        ## among all samples
        if not spikeInGenes.lower() == "na":
            if self.impactPLUS == "":
                self.impactPLUS = spikeInGenes
            elif not self.impactPLUS == spikeInGenes:
                self.errors.append("".join(["ERROR: SpikeInGenes are different between samples: ", \
                                            self.impactPLUS, " vs ", spikeInGenes, "."]))
            bait_info += "+" + spikeInGenes

        ## check that bait_version is consistent among all samples
        if len(self.bait_version) > 0:
            if not bait_info == self.bait_version:
                self.errors.append("".join(["ERROR: BAIT_VERSION differs between samples. ", \
                                            "Previous samples have this bait_version: ", \
                                            self.bait_version, ". ", sampleName, \
                                            " has this bait_version: ", bait_info]))
        else:
            self.bait_version = bait_info

        ## check that all required design files exist
        if not self.impactPLUS == "":
            si = captureBaitSet
        else:
            si = self.bait_version
        for root, dirs, files in os.walk(self.DESIGN_DIR):
            if self.exome:
                for dir in dirs:
                    if si == dir:
                        self.designFile = os.path.realpath(os.path.join(root, dir))
                        break
            else:
                for file in files:
                    if si in file and '_LATEST.berger' in file:
                        self.designFile = os.path.realpath(os.path.join(root, file))
                        break

        if self.impactPLUS == "":
            self.spikeinDesignFile = "NA"
        else:
            si = self.impactPLUS
            for root, dirs, files in os.walk(self.DESIGN_DIR):
                for file in files:
                    if si in file and 'LATEST.berger' in file:
                        self.spikeinDesignFile = os.path.realpath(os.path.join(root, file))
                        break

        if self.designFile == '':
            self.errors.append("".join(["ERROR: Design file(s) for ", captureBaitSet, " could not be found."]))

        if not self.impactPLUS == "" and self.spikeinDesignFile == '':
            self.errors.append(
                "".join(["ERROR: Design file(s) for spike in ", self.impactPLUS, " could not be found."]))
        return

    def isTumor(self, x):
        if x == "NORMAL":
            return True
        # oncoTreeURL="http://cmo.mskcc.org/oncotree/tumor_tree.txt"
        # oncoTreeURL="http://www.cbioportal.org/oncotree/tumor_tree.txt"
        # oncoTreeURL="http://www.cbioportal.org/oncotree/tumorType"
        oncoTreeURL = "http://www.cbioportal.org/oncotree/api/tumor_types.txt"
        try:
            response = urlopen(oncoTreeURL).read()
        except IOError:
            self.warnings.append("WARNING: We could not reach the ONCO_TREE file to validate tumor type!")
        if "404 Not Found" in response:
            self.warnings.append("WARNING: We could not reach the ONCO_TREE file to validate tumor type!")
        else:
            tumorTypes = list()
            data = response.split("\n")
            for line in data:
                line = line.split()
                for part in line:
                    if part.endswith(")") and part.startswith("("):
                        for moreParts in part.split("/"):
                            tumorTypes.append(moreParts.rstrip(")").lstrip("("))
            if not tumorTypes:
                self.warnings.append(
                    "WARNING: DID ONCO_TREE MOVE?!?!?! Please tell Krista there were no tumor types in the onco tree!!")
            if x not in tumorTypes:
                return False
            else:
                return True

    def validateTumorType(self, tumorType, metaStructFields):
        '''
        Assign tumor type '_UNKNOWN' if type is not specified, and warn if
        tumor type is not in the cbio portal's Onco Tree
        '''
        if "ONCOTREE_CODE" not in metaStructFields or tumorType == "":
            return "_UNKNOWN"
        else:
            if not self.isTumor(tumorType.upper()):
                self.warnings.append("".join(["WARNING: This tumor type is not on the portal's", \
                                              " Onco Tree: ", tumorType.upper(), \
                                              ". Are you sure this is the right tumor type??"]))
            return tumorType.upper()

    def findFixesInRelabels(self):
        '''
        Separate pre- and post-sequencing changes to sample labels, as they will be handled
        differently downstream.

        Corrections to pre-sequencing sample label swaps are stored in fixSampleNameMap
        Changes/corrections to post-sequencing sample labels remain in relabels
        '''
        for oldName in sorted(self.relabels):
            newName = self.relabels[oldName]
            if newName in self.relabels.keys() and oldName in self.relabels.values():
                self.warnings.append("".join(["WARNING: Verify that there is a swap (sample mixup) between ", \
                                              oldName, " and ", newName]))
                ## store sample name swaps separately
                self.fixSampleNameMap[oldName] = newName
            else:
                if not oldName.startswith("".join([newName, "_IGO_"])):
                    self.warnings.append("".join(["WARNING: Renaming sample: ", oldName, " to ", \
                                                  newName, ". Does the PatientID have to renamed too?"]))

        ## remove sample swaps from relabels
        for key in self.fixSampleNameMap.keys():
            self.relabels.pop(key, None)

        return

    def normalizeSex(self, sex, sample):
        if sex.lower() == "male":
            sex = "M"
        elif sex.lower() == "female":
            sex = "F"
        elif sex.lower() == "unknown":
            sex = "na"
        if sex not in ["F", "M", "na", "unknown"]:
            self.errors.append("".join(["ERROR: Gender, '", sex, "' for sample ", sample, " is invalid.", \
                                        "Valid genders are 'M', 'F', 'unknown', and 'na'."]))
        return sex

    def verifySampleInfoHeaders(self, xlsFile, headerList):
        ## verify that all headers exist in manifest
        filename = os.path.basename(xlsFile)

        TrueHeaderList = ["CMO_SAMPLE_ID", "CMO_PATIENT_ID", "INVESTIGATOR_SAMPLE_ID", \
                          "INVESTIGATOR_PATIENT_ID", "ONCOTREE_CODE", "SAMPLE_CLASS", \
                          "SAMPLE_TYPE", "SPECIMEN_PRESERVATION_TYPE", "SPECIMEN_COLLECTION_YEAR", \
                          "SEX", "BARCODE_ID", "BARCODE_INDEX", "LIBRARY_INPUT[ng]", \
                          "LIBRARY_YIELD[ng]", "CAPTURE_INPUT[ng]", "CAPTURE_NAME", \
                          "CAPTURE_CONCENTRATION[nM]", "CAPTURE_BAIT_SET", "SPIKE_IN_GENES", \
                          "STATUS", "INCLUDE_RUN_ID", "EXCLUDE_RUN_ID", "TISSUE_SITE"]
        good = True
        for item in TrueHeaderList:
            if item not in headerList:
                self.errors.append(
                    "".join(["ERROR: Column '", item, "' is either misspelled or missing from the SampleInfo ", \
                             "spreadsheet of the SampleManifest file ", filename, "."]))
                good = False
        return good

    def verifyValues(self, rec):

        # SAMPLE_TYPE_OPTIONS = ["Biopsy", "Plasma", "Resection", \
        #                       "Blood", "CellLine", "PDX", "Xenograft", \
        #                       "XenograftDerivedCellLine", "RapidAutopsy", \
        #                       "CerebralSpinalFluid","Urine","Unknown","Organoid","na"]

        SAMPLE_TYPE_OPTIONS = ["Biopsy", "Resection", "Blood", "cfDNA", "Fingernails", \
                               "CellLine", "PDX", "Xenograft", "XenograftDerivedCellLine", \
                               "RapidAutopsy", "Organoid", "Saliva", "other", "na"]

        SAMPLE_CLASS_OPTIONS = ["Unknown Tumor", "Primary", "Recurrence", "Metastasis", \
                                "Normal", "TumorPool", "PoolNormal", "NormalPool", "NTC", \
                                "AdjacentTissue"]

        SPECIMEN_PRESERVATION_TYPE_OPTIONS = ["Frozen", "FFPE", "Cytology", \
                                              "Blood", "EDTA-Streck", "Fresh", "OCT"]

        if rec.SAMPLE_TYPE not in SAMPLE_TYPE_OPTIONS:
            self.errors.append("".join(["ERROR: Sample ", rec.CMO_SAMPLE_ID, " has an invalid SAMPLE_TYPE.", \
                                        " Value Present: ", rec.SAMPLE_TYPE, " Valid values: ",
                                        ", ".join(SAMPLE_TYPE_OPTIONS)]))

        if rec.SAMPLE_CLASS not in SAMPLE_CLASS_OPTIONS:
            self.errors.append("".join(["ERROR: Sample ", rec.CMO_SAMPLE_ID, " has an invalid SAMPLE_CLASS.", \
                                        " Value Present: ", rec.SAMPLE_CLASS, " Valid values: ",
                                        ", ".join(SAMPLE_CLASS_OPTIONS)]))

        if rec.SPECIMEN_PRESERVATION_TYPE not in SPECIMEN_PRESERVATION_TYPE_OPTIONS:
            self.errors.append(
                "".join(["ERROR: Sample ", rec.CMO_SAMPLE_ID, " has an invalid SPECIMEN_PRESERVATION_TYPE.", \
                         " Value Present: ", rec.SPECIMEN_PRESERVATION_TYPE, " Possible Values: ",
                         ", ".join(SPECIMEN_PRESERVATION_TYPE_OPTIONS)]))
        if "cfDNA" in rec.SAMPLE_TYPE:
            rec.SPECIMEN_PRESERVATION_TYPE = "CFDNA"

        return

    def setPortalTumorType(self):
        tumorTypesSet = set([self.sampleRecs[ti].ONCOTREE_CODE for ti in self.tumors])
        tumorTypesSet = [x for x in tumorTypesSet if x.upper() != "NA"]
        if len(tumorTypesSet) > 1:
            self.portalTumorType = "mixed"
        elif len(tumorTypesSet) == 0:
            self.portalTumorType = "NA"
        else:
            self.portalTumorType = tumorTypesSet[0]
        return

    def validatePatientSex(self):
        '''
        Make sure sex is consistent across samples with the same patient ID
        '''
        for pt, recs in self.patientDb.items():
            sex = recs[0].SEX
            for rec in recs[1:]:
                if not rec.SEX == sex:
                    self.errors.append("".join(["ERROR: Sex of samples from patient ", pt, " are inconsistent"]))
        return

    def validateManifests(self):
        '''
        Modify each sample record as needed and store modified records from all
        manifests in one object
        '''

        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tValidating manifest(s)..."
        ## parse and store sample name relabels
        self.storeRelabels()
        ## separate type 1 and type 2 relabels
        self.findFixesInRelabels()

        for (fileNum, xlfile) in enumerate(self.manifests):
            for rec in lib.xlsx.DictReader(xlfile, sheetName="SampleInfo"):
                if not self.verifySampleInfoHeaders(xlfile, rec._MetaStruct__fields):
                    return

                if "STATUS" in rec._MetaStruct__fields and not rec.STATUS == "":
                    continue
                if rec.CMO_SAMPLE_ID == "":
                    continue

                projectId = self.getProjectId(rec)
                if projectId is not None:
                    self.projectIds.add(projectId)

                print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), "INFO: Project " \
                                                                                                       "id added "
                if rec.SAMPLE_CLASS.lower() == "tumor":
                    rec.SAMPLE_CLASS = "Unknown Tumor"
                    self.warnings.append("WARNING: Changing SAMPLE_CLASS 'Tumor' to 'Unknown Tumor'")

                self.verifyValues(rec)

                print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), "INFO: Values " \
                                                                                                       "verified "
                if rec.SAMPLE_TYPE in ['PDX', 'Xenograft', 'XenograftDerivedCellLine']:
                    self.includesXenografts = True

                sampleName = relabelSampleNames(rec.CMO_SAMPLE_ID, self.relabels)
                if sampleName in self.sampleRecs:
                    self.warnings.append(" ".join(["WARNING: Duplicate sample", sampleName, ". skipping."]))
                    continue

                rec.SampleNameOutFormat = sampleOutFormat(sampleName)
                rec.CMO_PATIENT_ID = "p_" + normalizeSampleNames(rec.CMO_PATIENT_ID)
                rec.BatchNumber = fileNum
                rec.ONCOTREE_CODE = self.validateTumorType(rec.ONCOTREE_CODE, rec._MetaStruct__fields)
                rec.SEX = self.normalizeSex(rec.SEX, rec.SampleNameOutFormat)

                self.validateBaits(rec.CAPTURE_BAIT_SET, rec.SPIKE_IN_GENES, rec.SampleNameOutFormat)

                print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), "INFO: Baits " \
                                                                                                       "verified "
                ## store modified sample records for all except pooled normals
                # if not isNormalPool(sampleName):
                if rec.SAMPLE_CLASS == 'NormalPool' or rec.SAMPLE_CLASS == 'PoolNormal':
                    self.normalPoolRecs[sampleName] = rec
                else:
                    self.sampleRecs[sampleName] = rec
                    ## index records by patient ID
                    if not rec.CMO_PATIENT_ID in self.patientDb:
                        self.patientDb[rec.CMO_PATIENT_ID] = []
                    self.patientDb[rec.CMO_PATIENT_ID].append(rec)
                    ## keep track of tumors
                    if not rec.SAMPLE_CLASS.upper() == "NORMAL":
                        self.tumors.append(sampleName)

                try:
                    float(rec["LIBRARY_INPUT[ng]"])
                except ValueError:
                    self.errors.append(
                        "ERROR: Invalid value for " + sampleName + " LIBRARY_INPUT[ng]: " + rec["LIBRARY_INPUT[ng]"])
                    continue
                try:
                    float(rec["LIBRARY_YIELD[ng]"])
                except ValueError:
                    self.errors.append(
                        "ERROR: Invalid value for " + sampleName + " LIBRARY_YIELD[ng]: " + rec["LIBRARY_YIELD[ng]"])
                    continue

        self.validatePatientSex()

        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), "INFO: Patient sex " \
                                                                                               "validated "
        self.setPortalTumorType()

        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), "INFO: tumor type set "

        return

    def getProjectId(self, rec):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tCmo sample id: " + rec.CMO_SAMPLE_ID

        matcher = re.search(".*IGO_(\d{5,}(_[A-Z]+)?)_.*", rec.CMO_SAMPLE_ID)
        if matcher:
            projectId = matcher.group(1)
            print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
                '%Y-%m-%d %H:%M:%S'), "\tProject id found: " + projectId

            return projectId
        return None

    def isValid(self):
        if len(self.errors) == 0:
            return True
        return False


class SampleMap():
    def __init__(self, sampleInfo, projID, piEmail, pipelineRunDir, exome, relabels):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tCreating sample map..."
        self.DELIVERED_ROOT = '/srv/www/html/share'
        self.FASTQ_ROOTS = ['/ifs/archive/GCL/hiseq/FASTQ', \
                            '/ifs/input/GCL/hiseq/FASTQ', \
                            '/ifs/assets/socci/Cache/IGO']
        self.projID = projID
        self.piEmail = piEmail
        self.exome = exome
        self.relabels = relabels
        self.sampleInfo = sampleInfo
        self.pipelineRunDir = pipelineRunDir
        self.sampleSequenceDirs = {}
        self.relocatedSeqs = {}
        self.runIDset = set()  ## set of strings
        self.normalPools = []  ## list of namedtuples, each containing sampName,path,poolType,chosen
        self.pathDb = {}  ## dict of sample info keyed by path to fastq dir
        self.errors = []
        self.warnings = []
        self.limsProject = self.isLimsProject(projID)

        self.cmoIdToCorrected = {}
        self.correctedToCmoId = {}

        self.initCmoToCorrectedSampleIdMappings()
        self.makeMap()

    def checkSampleSheet(self, sampleSheet, sampleName):
        result = 'valid'
        sname = relabelSampleNames(sampleName, self.sampleInfo.relabels)
        if self.sampleInfo.fixSampleNameMap and sname in self.sampleInfo.fixSampleNameMap:
            sname = self.sampleInfo.fixSampleNameMap[sname]
        if not os.path.exists(sampleSheet):
            self.errors.append("".join(["ERROR: Sample sheet ", sampleSheet, " does not exist. "]))
            return 'invalid'
        if not os.path.exists(sampleSheet):
            self.errors.append("".join(["ERROR: Sample sheet ", sampleSheet, " does not exist. "]))
            return 'invalid'
        if not os.path.exists(sampleSheet):
            self.errors.append("".join(["ERROR: Sample sheet ", sampleSheet, " does not exist. "]))
            return 'invalid'
        with open(sampleSheet, 'rU') as fp:
            headers = fp.readline().split(",")
            barcode = ''
            if headers[2] == "SampleID":
                barcode = fp.readline().split(',')[headers.index('Index')]
            elif headers[2] == sname:
                barcode == headers[4]
            else:
                self.errors.append("".join(["ERROR: The sample sheet for sample ", sname, \
                                            " has the wrong Sample name: ", headers[2], \
                                            ". Please alert the GCL. (Path = ", path]))
                result = 'invalid'

            try:
                limsBarcode = self.sampleInfo.sampleRecs[sname].BARCODE_INDEX
                ## check that sample sheet barcode matches lims barcode
                if not barcode == limsBarcode and not self.exome:
                    if not barcode.startswith(limsBarcode):
                        self.errors.append("".join(["ERROR: Barcode index for ", sampleName, " in sample sheet ", \
                                                    "[", barcode, "] does not match barcode in the manifest [",
                                                    limsBarcode, "]."]))
                        return 'invalid'

                    else:
                        self.warnings.append(
                            "".join(["WARNING: It looks like barcode for sample ", sampleName, " has been expanded ", \
                                     "by IGO for demultiplexing. LIMS has [", limsBarcode, "], but sample sheet has [", \
                                     barcode, "]. Reverting back to [", limsBarcode, "] for the pipeline."]))
                        result = 'expanded'
                        sampleName = relabelSampleNames(sampleName, self.sampleInfo.relabels)
                        if self.sampleInfo.fixSampleNameMap and sampleName in self.sampleInfo.fixSampleNameMap:
                            sampleName = self.sampleInfo.fixSampleNameMap[sampleName]
                        correctBarcode = self.sampleInfo.sampleRecs[sampleName].BARCODE_INDEX
                        # correctedPath = self.getModifiedSequenceDir(realPath,newBarcode=correctBarcode)
                        # return correctedPath
                        return correctBarcode
            except KeyError as e:
                print>> sys.stderr, e

        return 'valid'

    def isValidSeqDirectory(self, path):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tValidating sequence directory", path, "..."
        valid = True
        sampleSheetFound = False

        F = path.rstrip("/").split("/")
        sampleName = F[-1].replace("Sample_", "", 1)
        contents = os.listdir(path)

        ## check for fastq files
        r1Files = set([x for x in contents if x.find("_R1_") > -1])
        r2Files = set([x for x in contents if x.find("_R2_") > -1])
        if not len(r1Files) > 0:
            self.errors.append("ERROR: No R1 files found. Invalid FASTQ directory: " + path)
            valid = False
        if len(r2Files) > 0 and not set([x.replace("_R2_", "_R1_") for x in r2Files]) == r1Files:
            self.errors.append("ERROR: R2 files do not match R1 files. Invalid FASTQ directory: " + path)
            valid = False

        ## if a targeted project, check sample sheet
        if not self.exome:
            for file in contents:
                if file.startswith(sampleName) or file.startswith(sampleName.replace('_', '-')):
                    if not '/ifs/assets/socci/Cache/IGO' in path and 'tmp' not in file and time.time() - os.stat(
                                            path + "/" + file).st_mtime < 3600:
                        self.errors.append("".join(["ERROR: file ", file,
                                                    " was modified less than 1 hour ago and is possibly incomplete. Try again in an hour."]))
                        valid = False
                elif "samplesheet" in file.lower():
                    sampleSheetFound = True
                    sampleSheetStatus = self.checkSampleSheet(os.path.join(path, file), sampleName)
                    if sampleSheetStatus == 'invalid':
                        valid = False
                    elif not sampleSheetStatus == 'valid':
                        ## sampleSheetStatus is the corrected barcode
                        valid = self.getModifiedSequenceDir(path, newBarcode=sampleSheetStatus)
                else:
                    self.errors.append("".join(["ERROR: The sample fastqs for sample ", sampleName, \
                                                " are named with the incorrect sample name: ", file, \
                                                ". Please alert the GCL, and have them check the SampleSheet.csv file as well. (Path: ", \
                                                path]))
                    valid = False

            if not sampleSheetFound:
                self.errors.append("ERROR: No SampleSheet found in FASTQ directory: " + path)
                valid = False

        return valid

    def getModifiedSequenceDir(self, realPath, newBarcode=None, newSampleName=None):
        nameSampleDir = os.path.basename(realPath)
        newDir = realPath.replace("/ifs/archive/GCL/hiseq/FASTQ",
                                  os.path.join(self.pipelineRunDir, "modified_fastq_dirs"))

        try:
            print>> sys.stderr, "Creating new fastq dir", newDir, "..."
            if not os.path.isdir(newDir):
                os.makedirs(newDir)
            os.chown(newDir, 139, 2104)
            cmd = "".join(['ln -s ', realPath, '/*.fastq.gz ', newDir])
            output = Popen([cmd], stdout=PIPE, shell=True, stderr=PIPE).communicate()
            # print>>sys.stderr, cmd

            if newBarcode or newSampleName:
                oldSS = os.path.join(realPath, 'SampleSheet.csv')
                newSS = os.path.join(newDir, 'SampleSheet.csv')
                print>> sys.stderr, "Writing new sample sheet %s to replace %s" % (newSS, oldSS)

                with open(oldSS, 'rU') as old:
                    with open(newSS, 'w') as new:
                        x = old.readline().strip('\n').split(',')
                        if x[2] == 'SampleID' and x[4] == 'Index':
                            print>> new, ','.join(x)
                            x = old.readline().strip('\n').split(',')
                            if newSampleName:
                                x[2] = newSampleName
                            if newBarcode:
                                x[4] = newBarcode
                            print>> new, ','.join(x)
                        else:
                            self.errors.append(
                                ''.join(['ERROR: Determined that sample sheet must be modified to revert to 6pb ', \
                                         'barcode, but existing sample sheet is in an unknown format. Please ', \
                                         'email cmo-project-start@cbio.mskcc.org.']))
                            return None

        except OSError as e:
            print>> sys.stderr, e.strerror
            return None

        self.relocatedSeqs[newDir] = newDir  # .replace("/home/shiny/CMO/projects","/ifs/projects/CMO/archive")
        return newDir

    def fastqsDelivered(self):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tChecking that FASTQS have been properly delivered..."
        sampleDirs = set()
        patternsToSearchFor = []

        projID = self.projID

        ## Project FASTQ directory names we have seen include:
        ##    Project_1234
        ##    Project_01234
        ##    Project_1234_B
        ##    Project_01234_B
        ## Need to search for all of these possibilities

        patternsToSearchFor.append("Project_" + projID)
        patternsToSearchFor.append("Proj_" + projID)
        if len(projID.split("_")[0]) == 5 and projID[0] == '0':
            patternsToSearchFor.append("Project_" + projID[1:])
            patternsToSearchFor.append("Proj_" + projID[1:])
        if len(projID.split("_")[0]) == 4:
            patternsToSearchFor.append("Project_0" + projID)
            patternsToSearchFor.append("Proj_0" + projID)
        patternsToSearchFor.sort(
            reverse=True)  ## this puts patterns in order of likelihood (Project_1234, Project_01234, Proj_1234, Proj_01234)

        notArchived = False
        fastqsFound = False

        for pattern in patternsToSearchFor:
            findCMD = "find " + self.DELIVERED_ROOT + " -type d -name " + pattern
            projdir = Popen([findCMD], shell=True, stdout=PIPE).communicate()[0].strip()
            if not projdir or len(projdir) == 0:
                continue
            for pdir in projdir.split('\n'):
                print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
                    '%Y-%m-%d %H:%M:%S'), "  \tFound project dir:", pdir

                for p, ds, fs in os.walk(pdir, followlinks=False):
                    print>> sys.stderr, p
                    for d in ds:
                        print>> sys.stderr, "directory: ", d
                        realPath = os.path.join(p, d)
                        print>> sys.stderr, "Link: ", realPath
                        if os.path.islink(realPath):
                            realPath = os.readlink(os.path.join(p, d))
                            if not os.path.isabs(realPath):
                                realPath = os.path.normpath(os.path.join(p, realPath))
                            print>> sys.stderr, "Real path:", realPath
                            for fqr in self.FASTQ_ROOTS:
                                if fqr in realPath:
                                    print>> sys.stderr, fqr
                                    fastqsFound = True
                                    if '/ifs/input' in fqr:
                                        notArchived = True
                                        self.errors.append("".join(["ERROR: FASTQs were delivered from /ifs/input. ", \
                                                                    "Pipeline can not be run until FASTQs are archived. ", \
                                                                    "Please email sequencing techs."]))
                                    else:
                                        # sampleDirs.append(realPath)
                                        # if not self.isValidSeqDirectory(realPath):
                                        #    self.errors.append("ERROR: FASTQ directory "+realPath+" is invalid.")
                                        # break
                                        # sampleName=realPath.rstrip("/").split("/")[-1].replace("Sample_","", 1)
                                        # print>>sys.stderr,"Checking sample sheet status...",
                                        # sampleSheetStatus = self.checkSampleSheet(os.path.join(realPath,'SampleSheet.csv'),sampleName)
                                        # print>>sys.stderr,sampleSheetStatus
                                        # if sampleSheetStatus == 'expanded':
                                        #    sampleName = relabelSampleNames(sampleName,self.sampleInfo.relabels)
                                        #    if self.sampleInfo.fixSampleNameMap and sampleName in self.sampleInfo.fixSampleNameMap:
                                        #        sampleName = self.sampleInfo.fixSampleNameMap[sampleName]
                                        #    correctBarcode = self.sampleInfo.sampleRecs[sampleName].BARCODE_INDEX
                                        #    realPath = self.getModifiedSequenceDir(realPath,newBarcode=correctBarcode)
                                        sampleDirs.add(realPath)
                if fastqsFound:
                    break

        if len(sampleDirs) == 0 and not notArchived:
            self.errors.append("ERROR: No valid FASTQ delivery directories found for Project " + \
                               self.projID)

        return sampleDirs

    def isValidRunID(self, runID):
        if runID.find("_"):
            return True
        self.errors.append("".join(["ERROR: Run ID ", runID, " is invalid."]))
        return False

    def includeRun(self, runID, sampleName):
        runID_Abbrev = runID[:runID.find("_") + 5]

        if sampleName not in self.sampleInfo.sampleRecs:
            return False

        if runID_Abbrev in self.sampleInfo.sampleRecs[sampleName].EXCLUDE_RUN_ID:
            return False
        if runID_Abbrev not in self.sampleInfo.sampleRecs[sampleName].INCLUDE_RUN_ID:
            self.errors.append("".join(["ERROR: Run ID ", runID_Abbrev, " is not in the include " \
                                                                        "or exclude columns for sample ", sampleName,
                                        "."]))
            return False
        return True

    def allRunsHavePooledNormal(self, poolNormRunIDset):
        if len(list(self.runIDset - poolNormRunIDset)) > 0:
            self.warnings.append("".join(["WARNING: not all runs have pooled normal sequences. ", \
                                          str(self.runIDset - poolNormRunIDset), " are missing ", \
                                          "pooled normal sequences. Please email sequencing techs."]))
            return False
        return True

    def returnBarcodes(self, path, files):
        barcodes = set()
        for f in files:
            if f.endswith('.csv'):
                with open(os.path.join(path, f), 'r') as ss:
                    idx = ss.readline().split(',').index('Index')
                    for line in ss:
                        barcodes.add(line.split(',')[idx])
        return barcodes

    def checkForMultipleRuns(self):
        for sampleName in sorted(self.sampleSequenceDirs):
            list_o_dirs = self.sampleSequenceDirs[sampleName]
            barcodes = set()
            if len(list_o_dirs) == 0:
                return
            if len(list_o_dirs) > 1:
                runs = []
                for path in list_o_dirs:
                    # print>>sys.stderr,path
                    F = path.rstrip("/").split("/")
                    runID = F[-3]
                    runs.append(runID)
                    files = listdir(path)
                    barcodes = barcodes.union(self.returnBarcodes(path, files))
                self.warnings.append("".join(["WARNING: ", sampleName, " has multiple runs: ", str(runs)]))
            else:
                files = listdir(list_o_dirs[0])
                barcodes = self.returnBarcodes(list_o_dirs[0], files)
            if len(barcodes) > 1:
                self.warnings.append("".join(["WARNING: ", sampleName, " has multiple barcodes: ", \
                                              str(barcodes), ". This warning is for BIC."]))
        return

    def getSize(self, sa, bc):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tGetting size of directory..."
        if bc != "":
            path = "".join([sa.path, "/*", bc, "*"])
        else:
            path = sa.path
        cmd = "".join(["du -Lc ", path])
        out1 = Popen([cmd], shell=True, stdout=PIPE)
        output = Popen(["grep total"], stdout=PIPE, shell=True, stdin=out1.stdout).communicate()[0]
        size = output.split()[0].strip()

        return int(size)

    def linkFQ(self, fPath, bc, tPath):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), "\tLinking FASTQs..."
        cmd = "".join(['for file in $(ls ', fPath, '/*', bc, '* ); do ln -s -t ', tPath, ' $file ; done;'])
        output = Popen([cmd], stdout=PIPE, shell=True, stderr=PIPE).communicate()
        self.relocatedSeqs[tPath] = tPath  # .replace("/home/shiny/CMO/projects","/ifs/projects/CMO/archive")
        cmd2 = "".join(['head -n 1 ', fPath, '/SampleSheet.csv > ', tPath, '/SampleSheet.csv; grep ', bc, ' ', fPath,
                        '/SampleSheet.csv >> ', tPath, '/SampleSheet.csv'])
        output2 = Popen([cmd2], stdout=PIPE, shell=True, stderr=PIPE).communicate()

        return

    def select1PooledNormal(self):
        '''
        For each type of pooled normal (FFPE, FROZEN, UNKNOWN),
        find the sample with the most coverage (largest fastq
        file size). Indicate these samples as the 'chosen' samples
        in the normalPools tuple(?) by setting 'chosen' = 1 
        '''
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tSelecting pooled normal..."
        FFPE_num = 0
        FROZEN_num = 0
        UNK_num = 0
        FFPE_data = {}
        FROZEN_data = {}
        UNK_data = {}
        removeNames = set()
        Active_POOL_NORM = []

        for i in range(len(self.normalPools)):

            ## self.normalPools is a list of namedtuples, each containing
            ## fields: poolName path poolType chosen 

            files = listdir(self.normalPools[i].path)
            barcodes = self.returnBarcodes(self.normalPools[i].path, files)

            ## if multiple barcodes are found in one pooled normal sample,
            ## choose the one with the most coverage; create a temp
            ## directory with soft links to the fastq files with that barcode
            ## only; replace the path for that pooled normal with the new
            ## temp directory 
            if len(barcodes) > 1:
                bc = {}
                drname = self.pipelineRunDir
                tdir = "".join([drname, "/temp_fastq_file", "_", str(i)])
                if not os.path.isdir(tdir):
                    os.mkdir(tdir)
                os.chown(tdir, 139, 2104)  ## ownership: user 'shiny', group 'cmo'
                for b in barcodes:
                    bc[b] = self.getSize(self.normalPools[i], b)
                maxBC = max(bc, key=bc.get)
                self.linkFQ(self.normalPools[i].path, maxBC, tdir)
                self.pathDb[tdir] = self.pathDb[self.normalPools[i].path]
                del self.pathDb[self.normalPools[i].path]
                self.normalPools[i] = self.normalPools[i]._replace(path=tdir)

            originalPoolName = self.normalPools[i].poolName
            replacePoolName = originalPoolName
            ## standardize normal pool sample names and number them
            ## store the size of each normal pool sample
            if self.normalPools[i].poolType == "FFPE":
                FFPE_num += 1
                replacePoolName = "_".join(["Normal_Pooled_FFPE", str(FFPE_num)])
                self.normalPools[i] = self.normalPools[i]._replace(poolName=replacePoolName)
                ## I also need to change the sampleInfo recs.
                sampSize = self.getSize(self.normalPools[i], "")
                FFPE_data[self.normalPools[i].poolName] = int(sampSize)
            elif self.normalPools[i].poolType == "FROZEN":
                FROZEN_num += 1
                replacePoolName = "_".join(["Normal_Pooled_FROZEN", str(FROZEN_num)])
                self.normalPools[i] = self.normalPools[i]._replace(poolName=replacePoolName)
                sampSize = self.getSize(self.normalPools[i], "")
                FROZEN_data[self.normalPools[i].poolName] = int(sampSize)
            else:  ##elif self.normalPools[i].poolType == "UNK":
                UNK_num += 1
                replacePoolName = "_".join(["Normal_Pooled_UNK", str(UNK_num)])
                self.normalPools[i] = self.normalPools[i]._replace(poolName=replacePoolName)
                sampSize = self.getSize(self.normalPools[i], "")
                UNK_data[self.normalPools[i].poolName] = int(sampSize)

            if originalPoolName != replacePoolName:
                self.sampleInfo.normalPoolRecs[replacePoolName] = self.sampleInfo.normalPoolRecs[originalPoolName]
                removeNames.add(originalPoolName)

            ## find the largest of each kind of pooled normal
            if UNK_num > 0:
                Active_POOL_NORM.append(max(UNK_data, key=UNK_data.get))
            if FROZEN_num > 0:
                Active_POOL_NORM.append(max(FROZEN_data, key=FROZEN_data.get))
            if FFPE_num > 0:
                Active_POOL_NORM.append(max(FFPE_data, key=FFPE_data.get))

            ## flag 'chosen' pooled normal samples
            for g in range(len(self.normalPools)):
                if self.normalPools[g].poolName in Active_POOL_NORM:
                    self.normalPools[g] = self.normalPools[g]._replace(chosen=1)

        ## At the end of the for loop, remove self.sampleInfo.normalPoolRecs(removeNames[i])
        for oldName in removeNames:
            del self.sampleInfo.normalPoolRecs[oldName]

        return

    def isPairedEnd(self, path):
        files = listdir(path)
        r1Files = set([x for x in files if x.find("_R1_") > -1])
        r2Files = set([x for x in files if x.find("_R2_") > -1])
        if len(r2Files) == 0:
            return False
        if set([x.replace("_R2_", "_R1_") for x in r2Files]) == r1Files:
            return True

    def poolType(self, x):
        xx = x.upper()
        if xx.find("FFPE") > -1:
            return "FFPE"
        elif xx.find("FROZEN") > -1:
            return "FROZEN"
        else:
            self.errors.append(" ".join(["Unknown normal pool type:", x]))
            return "UNK"

    def initCmoToCorrectedSampleIdMappings(self):
        for projectId in self.sampleInfo.projectIds:
            self.initCmoToCorrectedForProject(projectId)

    def initCmoToCorrectedForProject(self, projectId):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tCreating mapping cmo id to corrected cmo id for project: " + projectId

        passedSamples = lims_rest.getPassedSamples(projectId, "BICValidator")
        for passedSample in passedSamples["samples"]:
            relabeledCmoId = relabelSampleNames(passedSample["cmoId"], self.relabels)

            if not "correctedCmoId" in passedSample:
                correctedCmoId = relabeledCmoId
            else:
                correctedCmoId = relabelSampleNames(passedSample["correctedCmoId"], self.relabels)

            self.cmoIdToCorrected[passedSample["cmoId"]] = correctedCmoId
            self.correctedToCmoId[correctedCmoId] = passedSample["cmoId"]

    def isDmpSample(self, sample):
        return sample.INCLUDE_RUN_ID == "DMPLibs"

    def allSamplesHaveFastqs(self):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tChecking that all samples have fastqs..."

        for correctedCmoId in self.sampleInfo.sampleRecs:
            if not self.sampleInfo.sampleRecs[correctedCmoId].INCLUDE_RUN_ID.strip() == "":
                if not correctedCmoId in self.sampleSequenceDirs:
                    self.errors.append("ERROR: No valid FASTQ directories found for sample " + correctedCmoId)
                else:
                    for iRun in self.sampleInfo.sampleRecs[correctedCmoId].INCLUDE_RUN_ID.strip().split(";"):
                        fastqsFound = False
                        for dir in self.sampleSequenceDirs[correctedCmoId]:
                            if iRun.strip() in dir:
                                fastqsFound = True
                        if not fastqsFound:
                            self.errors.append(
                                "ERROR: No valid FASTQ directories found from run " + iRun + " for sample " + correctedCmoId)
        return

    def makeMap(self):
        sampleDirs = list(self.fastqsDelivered())
        if not sampleDirs:
            return None
        else:
            DirectoryInfo = namedtuple("DInfo", "sampleName runID PE")
            PoolInfo = namedtuple("PInfo", "poolName path poolType chosen")
            poolNormRunIDset = set()

            for dir in sampleDirs:
                print >>sys.stderr,"sample dir: " + dir
                cmoId, runID = self.getCmoId(dir)
                correctedCmoId = cmoId
                if not cmoId in self.sampleInfo.normalPoolRecs:
                    if self.limsProject:
                        if not cmoId in self.cmoIdToCorrected:
                           continue

                        correctedCmoId = self.cmoIdToCorrected[cmoId]
                    else:
                        if runID == "DMPLibs" and cmoId in self.relabels:
                            correctedCmoId = self.relabels[cmoId]
                        if runID == "DMPLibs" and cmoId.replace("-","_") in self.relabels:
                            correctedCmoId = self.relabels[cmoId.replace("-","_")]
                        if runID != "DMPLibs" and "Project_POOLEDNORMALS" not in dir:
                             realProj  = dir.rstrip("/").split("/")[-2].replace("Project_", "").replace("Proj_", "")
                             if(len(realProj.split("_")[0]) == 4):
                                realProj = '0' + realProj
                             passedSamples = lims_rest.getPassedSamples(realProj, "BICValidator")
                             for passedSample in passedSamples["samples"]:
                                 if passedSample["cmoId"] == cmoId:
                                    correctedCmoId = passedSample["correctedCmoId"].replace("-","_")

                    if not self.isValidRunID(runID):
                        print>> sys.stderr, "runID", runID, "invalid!!!"
                        continue
                    if not self.includeRun(runID, correctedCmoId):
                        print>> sys.stderr, runID, "to be excluded for sample", correctedCmoId
                        continue
                    if not correctedCmoId in self.sampleSequenceDirs:
                        self.sampleSequenceDirs[correctedCmoId] = []
                    validDir = self.isValidSeqDirectory(dir)
                    if not validDir:
                        self.errors.append("ERROR: FASTQ directory " + dir + " is invalid.")
                        continue
                    elif not validDir == True:  ## validDir must be a corrected dir
                        dir = validDir
                    self.sampleSequenceDirs[correctedCmoId].append(dir)
                    print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
                        '%Y-%m-%d %H:%M:%S'), "\tSequence dir added: " + dir + " for sample: " + correctedCmoId

                self.pathDb[dir] = DirectoryInfo(correctedCmoId, runID, "PE" if self.isPairedEnd(dir) else "SE")
                self.runIDset.add(runID)
                if cmoId in self.sampleInfo.normalPoolRecs:
                    self.normalPools.append(PoolInfo(cmoId, dir, self.sampleInfo.normalPoolRecs[
                        cmoId].SPECIMEN_PRESERVATION_TYPE.upper(), 0))
                    poolNormRunIDset.add(self.pathDb[dir].runID)

            self.select1PooledNormal()
            self.allRunsHavePooledNormal(poolNormRunIDset)
            self.checkForMultipleRuns()
            self.allSamplesHaveFastqs()

        return

    def getCorrectedCmoId(self, cmoId):
        return self.cmoSampleIdToSample[cmoId]

    def isValid(self):
        if len(self.errors) > 0:
            return False
        return True

    def getCmoId(self, dir):
        F = dir.rstrip("/").split("/")
        print>> sys.stderr, F
        cmoIgoSampleId = F[-1].replace("Sample_", "", 1)
        runID = F[-3]
        print >>sys.stderr,"run id: " + runID
        cmoId = cmoIgoSampleId.split("_IGO")[0]

        return cmoId, runID

    def isLimsProject(self, projID):
        return lims_rest.projectExists(projID, "BICValidator")

class SamplePairing():
    def __init__(self, pairingFile, sampleInfo, sampleMap, exome):
        print>> sys.stderr, datetime.fromtimestamp(time.time()).strftime(
            '%Y-%m-%d %H:%M:%S'), "\tValidating pairing file..."
        self.sampleInfo = sampleInfo
        self.pairingFile = pairingFile
        self.exome = exome
        self.pairingTable = {}
        self.errors = []
        self.warnings = []
        self.validatePairingFile(sampleInfo, sampleMap)

    def verifyPairingInfoHeaders(self, headerList):
        filename = os.path.basename(self.pairingFile)
        TrueHeaderList = ["Tumor", "MatchedNormal", "SampleRename"]
        for item in TrueHeaderList:
            if item not in headerList:
                self.errors.append("".join(["Column '", item, \
                                            "' is either misspelled or missing from the PairingInfo spreadsheet of the Pairing file", \
                                            filename, "."]))
        return

    def pickPoolNormal(self, sType, nPools):

        chosen = dict()

        sType = sType.upper()

        if sType != "FROZEN" and sType != "FFPE":
            sType = "FROZEN"
        for samp in nPools:
            if samp.chosen == 1:
                chosen[samp.poolType.upper()] = samp.poolName
        if len(chosen) > 0:
            if sType in chosen:
                return chosen[sType]
            elif "UNK" in chosen:
                return chosen["UNK"]
            elif len(chosen) == 1:
                return chosen.values()[0]
        else:
            self.errors.append("ERROR: There are no POOLED NORMALS!?!")

        return

    def validatePairingFile(self, sampleInfo, sampleMap):
        sampleRecs = sampleInfo.sampleRecs
        relabels = sampleInfo.relabels
        normPools = sampleMap.normalPools

        if not os.path.isfile(self.pairingFile):
            self.errors.append("ERROR: Pairing file does not exist or is not a valid file. ")
            return

        for rec in lib.xlsx.DictReader(self.pairingFile, sheetName="PairingInfo"):
            self.verifyPairingInfoHeaders(rec._MetaStruct__fields)
            tumorName = relabelSampleNames(rec.Tumor, relabels)
            normalName = relabelSampleNames(rec.MatchedNormal, relabels)
            if normalName == "" or tumorName == "":
                continue
            if tumorName not in sampleRecs:
                if not tumorName.lower() == 'na':
                    self.errors.append("".join(["ERROR: The tumor sample: ", tumorName, \
                                                " is not in the sample manifests but is in the pairing file."]))
                continue
            if sampleRecs[tumorName].SAMPLE_CLASS.lower() == 'normal':
                self.warnings.append("".join(["ERROR: Sample ", tumorName, " is listed as a TUMOR in the pairing file " \
                                                                           "but as a NORMAL in the sample manifest."]))
            if normalName.lower() == "na":
                if not self.exome:
                    normalName = str(
                        self.pickPoolNormal(self.sampleInfo.sampleRecs[tumorName].SPECIMEN_PRESERVATION_TYPE,
                                            sampleMap.normalPools))
                    self.warnings.append("".join(["WARNING: Pairing unmatched tumor ", tumorName, \
                                                  " with pooled normal: ", normalName, "."]))
                else:
                    self.warnings.append("".join(["WARNING: Tumor ", tumorName,
                                                  " has no matched normal. Sample will NOT go through somatic analysis."]))
            elif normalName not in sampleRecs:
                self.errors.append("".join(["ERROR: The normal sample: ", normalName, \
                                            " is not in the sample manifests but is in the pairing file."]))
                continue
            elif not sampleRecs[normalName].SAMPLE_CLASS.lower() == 'normal':
                self.warnings.append(
                    "".join(["WARNING: Sample ", normalName, " is listed as a NORMAL in the pairing file " \
                                                             "but as a TUMOR in the sample manifest."]))
            elif sampleRecs[normalName].CMO_PATIENT_ID != sampleRecs[tumorName].CMO_PATIENT_ID:
                self.warnings.append("".join(["WARNING: Tumor and Normal have different CMO Patient ID's. Tumor: ", \
                                              sampleRecs[tumorName].CMO_PATIENT_ID, " Normal: ",
                                              sampleRecs[normalName].CMO_PATIENT_ID]))
            elif tumorName in self.pairingTable.keys():
                if self.exome:
                    temp = set([self.pairingTable[tumorName], normalName])
                    self.pairingTable[tumorName] = temp
                    continue
                else:
                    self.errors.append("".join(["ERROR: Tumor ", tumorName, \
                                                " already has a matched normal: ", self.pairingTable[tumorName], \
                                                " dmp pipeline does not allow tumor to be matched to multiple normals. "]))
                    continue
            self.pairingTable[tumorName] = normalName

        return

    def isValid(self):
        if len(self.errors) > 0:
            return False
        return True
