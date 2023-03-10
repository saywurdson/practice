import pandas as pd
import os
from dotenv import load_dotenv
import ModelSyntheaPandas
import ModelOmopPandas5
import ModelOmopPandas6
import SyntheaToOmop5
import SyntheaToOmop6
import Utils
import sys

#------------------------------------------------------
# This scripts performs an ETL from Synthea to omop CDM.
# It uses pandas dataframes
#------------------------------------------------------

# ------------------------
# load env
# ------------------------
load_dotenv(verbose=True)

# -----------------------------------
# - Configuration
# -----------------------------------

# Edit your .env file to change which directories to use in the ETL process
# Path to the directory containing the downloaded SynPUF files
BASE_SYNTHEA_INPUT_DIRECTORY    = os.environ['BASE_SYNTHEA_INPUT_DIRECTORY']
# Path to the directory containing the OMOP Vocabulary v5 files (can be downloaded from http://www.ohdsi.org/web/athena/)
BASE_OMOP_INPUT_DIRECTORY       = os.environ['BASE_OMOP_INPUT_DIRECTORY']
# Path to the directory where CDM-compatible CSV files should be saved
BASE_OUTPUT_DIRECTORY           = os.environ['BASE_OUTPUT_DIRECTORY']
# cdm output version
CDM_VERSION                     = os.environ['CDM_VERSION']
# counter file name
COUNTER_FILE                    = os.environ['COUNTER_FILE']
# Synthea input file chunk size.
INPUT_CHUNK_SIZE = int(os.environ['INPUT_CHUNK_SIZE'])
# patients and encounters are first so that we can create dataframes to lookup ids
SYNTHEA_FILE_LIST =  ['patients','encounters','conditions','careplans','observations','procedures','immunizations','imaging_studies','organizations','providers','payer_transitions','allergies','medications']
# List of omop output files
OMOP_FILE_LIST = ['person','location','condition_occurrence','drug_exposure','observation','measurement','procedure_occurrence','observation_period','visit_occurrence','care_site','provider']
if (CDM_VERSION=='5'):
    OMOP_FILE_LIST.append('death')

#---------------------------------
# start of the program
#---------------------------------
if __name__ == '__main__':
    if not os.path.exists(BASE_OUTPUT_DIRECTORY): 
        os.makedirs(BASE_OUTPUT_DIRECTORY)
    else:
        # cleanup old files in output directory but dont delete directory
        filesToRemove = [f for f in os.listdir(BASE_OUTPUT_DIRECTORY)]
        for f in filesToRemove:
            os.remove(os.path.join(BASE_OUTPUT_DIRECTORY, f))

    print('BASE_SYNTHEA_INPUT_DIRECTORY     =' + BASE_SYNTHEA_INPUT_DIRECTORY)
    print('BASE_OUTPUT_DIRECTORY            =' + BASE_OUTPUT_DIRECTORY)
    print('CDM_VERSION                      =' + CDM_VERSION)
    print('COUNTER_FILE                     =' + COUNTER_FILE)

    # load utils
    util = Utils.Utils()

    # init the counter file if it does not exist and then read values into a dict
    if not os.path.exists(COUNTER_FILE):
        util.initCounterFile(OMOP_FILE_LIST, 1, COUNTER_FILE)
    c = util.getCounter(COUNTER_FILE)

    # check files look ok

    # load the conversion class for this CDM version
    model_synthea = ModelSyntheaPandas.ModelSyntheaPandas()
    if (CDM_VERSION == '5'):
        model_omop = ModelOmopPandas5.ModelOmopPandas5()
        convert = SyntheaToOmop5.SyntheaToOmop5(model_omop.model_schema, util)
    elif (CDM_VERSION == '6'):
        model_omop = ModelOmopPandas6.ModelOmopPandas6()
        convert = SyntheaToOmop6.SyntheaToOmop6(model_omop.model_schema, util)
    else:
        print("CDM version not supported")
        exit(1)

    # write the headers for the output files
    for initfile in OMOP_FILE_LIST:
        df = pd.DataFrame(columns=model_omop.model_schema[initfile].keys())
        initfile = initfile + ".csv"
        df.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,initfile), mode='w', header=True, index=False)

    # load the vocabulary into memory
    vocab_concept = util.loadConceptVocabulary(BASE_OMOP_INPUT_DIRECTORY, model_omop)

    # create source to standard and source to source mapping
    srctostdvm = util.sourceToStandardVocabMap(vocab_concept,model_omop) 
    srctosrcvm = util.sourceToSourceVocabMap(vocab_concept,model_omop)

    # create mapping for to lookup id's
    personmap = pd.DataFrame(columns=["person_id","synthea_patient_id"])
    visitmap = pd.DataFrame(columns=["visit_occurrence_id","synthea_encounter_id"])
    
    # we dont need a header when appending
    header = False
    mode='a'

    # start looping through the synthea files
    # we only need to consider one synthea input file at a time to make the mapping
    # so only put one in memory at a time and read in chunks to avoid memory issues
    for datatype in SYNTHEA_FILE_LIST:
        if (os.path.exists(os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,datatype + '.csv'))):
            inputfile = datatype + '.csv'
            compression=None
        elif (os.path.exists(os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,datatype + '.csv.gz'))):
            inputfile = datatype + '.csv.gz'
            compression='gzip'
        else:
            print("Error:  Could not find synthea file for " + datatype)
            exit(1)
        inputdata = os.path.join(BASE_SYNTHEA_INPUT_DIRECTORY,inputfile)
        output = os.path.join(BASE_OUTPUT_DIRECTORY,inputfile)
        print("")
        print(datatype),
        for df in pd.read_csv(inputdata, dtype=model_synthea.model_schema[datatype], chunksize=INPUT_CHUNK_SIZE, iterator=True, compression=compression):
            if (datatype == 'patients'):
                if 'person_id' not in c:
                    c['person_id'] = 0
                if 'location_id' not in c:
                    c['location_id'] = 0
                (person, location, death, personmap, c['person_id'], c['location_id']) = convert.patientsToOmop(df, personmap, c['person_id'], c['location_id'])
                person.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'person.csv'), mode=mode, header=header, index=False)
                location.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'location.csv'), mode=mode, header=header, index=False)
                if (CDM_VERSION=='5'): # death table is removed in cdm6 so only write it in cdm 5
                    death.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'death.csv'), mode=mode, header=header, index=False)
                personmap.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'personmap.csv'), mode='w', header=True, index=False)
            elif (datatype == 'conditions'):
                if 'condition_occurrence_id' not in c:
                    c['condition_occurrence_id'] = 0
                if 'drug_exposure_id' not in c:
                    c['drug_exposure_id'] = 0
                if 'observation_id' not in c:
                    c['observation_id'] = 0
                (condition_occurrence, drug_exposure, observation, c['condition_occurrence_id'], c['drug_exposure_id'], c['observation_id']) = convert.conditionsToOmop(df, srctostdvm, c['condition_occurrence_id'], c['drug_exposure_id'], c['observation_id'], personmap, visitmap)
                condition_occurrence.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'condition_occurrence.csv'), mode=mode, header=header, index=False)
                drug_exposure.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'drug_exposure.csv'), mode=mode, header=header, index=False)
                observation.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'observation.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'careplans'):
                pass
            elif (datatype == 'observations'):
                if 'measurement_id' not in c:
                    c['measurement_id'] = 0
                (measurement, c['measurement_id']) = convert.observationsToOmop(df, srctostdvm, srctosrcvm, c['measurement_id'], personmap, visitmap)
                measurement.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'measurement.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'procedures'):
                if 'procedure_occurrence_id' not in c:
                    c['procedure_occurrence_id'] = 0
                (procedure_occurrence, c['procedure_occurrence_id']) = convert.proceduresToOmop(df, srctosrcvm, c['procedure_occurrence_id'], personmap, visitmap)
                measurement.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'measurement.csv'), mode=mode, header=header, index=False)
                procedure_occurrence.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'procedure_occurrence.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'immunizations'):
                if 'drug_exposure_id' not in c:
                    c['drug_exposure_id'] = 0
                (drug_exposure, c['drug_exposure_id']) = convert.immunizationsToOmop(df, srctosrcvm, c['drug_exposure_id'], personmap, visitmap)
                drug_exposure.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'drug_exposure.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'imaging_studies'):
                pass
            elif (datatype == 'encounters'):
                if 'observation_period_id' not in c:
                    c['observation_period_id'] = 0
                if 'visit_occurrence_id' not in c:
                    c['visit_occurrence_id'] = 0
                (observation_period, visit_occurrence, c['observation_period_id'], c['visit_occurrence_id'], visitmap) = convert.encountersToOmop(df, c['observation_period_id'], c['visit_occurrence_id'], personmap, visitmap)
                observation_period.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'observation_period.csv'), mode=mode, header=header, index=False)
                visit_occurrence.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'visit_occurrence.csv'), mode=mode, header=header, index=False)
                visitmap.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'visitmap.csv'), mode='w', header=True, index=False)
            elif (datatype == 'organizations'):
                if 'care_site_id' not in c:
                    c['care_site_id'] = 0
                (care_site, c['care_site_id']) = convert.organizationsToOmop(df, c['care_site_id'])
                care_site.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'care_site.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'providers'):
                if 'provider_id' not in c:
                    c['provider_id'] = 0
                (provider, c['provider_id']) = convert.providersToOmop(df, c['provider_id'])
                provider.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'provider.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'payer_transitions'):
                pass
            elif (datatype == 'allergies'):
                if 'observation_id' not in c:
                    c['observation_id'] = 0
                (observation, c['observation_id']) = convert.allergiesToOmop(df, srctostdvm, c['observation_id'], personmap, visitmap)
                observation.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'observation.csv'), mode=mode, header=header, index=False)
            elif (datatype == 'medications'):
                if 'drug_exposure_id' not in c:
                    c['drug_exposure_id'] = 0
                (drug_exposure, c['drug_exposure_id']) = convert.medicationsToOmop(df, srctosrcvm, c['drug_exposure_id'], personmap)
                drug_exposure.to_csv(os.path.join(BASE_OUTPUT_DIRECTORY,'drug_exposure.csv'), mode=mode, header=header, index=False)
            else:
                print("Unknown input type: " + datatype)
            # no longer write header and append to file. write . so we know program is still running
            print('.'),
            sys.stdout.flush()
        # write the counter file
        util.writeCounter(c,COUNTER_FILE)