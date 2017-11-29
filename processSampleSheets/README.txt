General code overview:

New pipeline_info_creator code aims to collect any and all problems
with input files at one time instead of exiting after each problem
it finds. 

All validation must pass before project files can be written, so 
once those files exist we can assume they are correct. 

request_pipeline_input.py creates a CMOProject object based on command line
input. CMOProject.validateProject() runs all validation on manifest and
pairing file. 

During validation all errors are collected in CMOProject.errors, and warnings in
CMOProject.warnings. 

If no errors exist, CMOProject.isValid() will return True. NOTE: the existence
of warnings do NOT render the object invalid. Warnings will be written to the
project's readme file

Validation details:

    CMOProject.isValid() first validates PI based on the MSK email given.

    Then, sample manifests are validated by creating a SampleInfo object. 

    If and only if the SampleInfo object is valid, a SampleMap object will
    be created, validating FASTQ directories, run IDs, existence of pooled
    normals, etc. 

    If and only if the SampleMap object is valid, a SamplePairing object will
    be created to validate the pairing file.

    Only once all three of these objects have been validated can any project
    files be written. 

    As each object is validated, any errors are added to CMOProject.errors. 

    Once project files are written, automatically send email to
    cmo-project-start

        * files are written to 

             /home/shiny/CMO/projects/Proj_[id]

        * if --testing flag is set, files are written to

             /home/shiny/CMO/projects/testing/Proj_[id]

    Caitlin/Krista then manually move project folder to its final
    location

        * entire project folder is rsynced to

            /ifs/projects/CMO/archive

        * then most recent project files are rsynced to

            /ifs/projects/CMO/Proj_[id]



request_sample_redaction.py and request_snp_redaction.py also create
CMOProject objects and update project and run logs in addition to running
the appropriate redaction scripts. Details above regarding file locations, 
notifications and rsyincing procedure also applies to these scripts.

request_sample_redaction.py now also updates LIMs to mark redacted samples
Failed. Any errors in updating lims are emailed to Caitlin and Krista
separately from other notifications.


**** MORE VALIDATION DETAILS (THIS NEEDS TO BE UPDATED) ****

Sample manifest validation
    - relabels/swaps
    - check column headers in Excel file
    - validate baits
        - check that directory of capture bait set files exists
        - check that all required design files exist
        - check that bait version is consistent across all samples
    - validate tumor type against onco tree
    - check that samples are unique

Pairing file validation:
    - check column headers in Excel file
    - check that all samples in manifest are in pairing file
    - check that CMO patient IDs are correct

Sample map validation:
    - check that FASTQs are delivered for the project (and by doing so, validate project ID and PI email)
    - validate run IDs
    - validate Inclue/ExcludeRunID columns
    - check that all runs have a pooled normal
    - check for samples that have multiple runs and/or barcodes

Project validation:
    - check for existence of old project files if rerun flag not set
    - somehow validate super proj id?
    - validate platform?
    - validate sample manifests
    - validate sample map
    - validate sample pairing
    - cross check sample pairing with manifests
    
