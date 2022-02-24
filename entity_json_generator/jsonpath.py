""" This file is use to give all json path as a constant which acn be import from indiviual python files as per requirements."""

import os

aegis_home = os.environ.get('AEGIS_HOME')

# define file path path for alljsons

dirname = os.path.dirname(__file__)

allJsonPath = str(os.path.abspath(os.path.join(dirname, os.pardir))) +'/jsonpath/'

print(f'Alljson path={allJsonPath}')

# define constant for complit path of AddressMatch.json
ADDRESS_MATCH_JSON = allJsonPath+'AddressMatch.json'

# define constant for complit path of CommonProperties.json
COMMON_PROPERTIES_JSON = allJsonPath+'CommonProperties.json'

# define constant for complit path of connectionInfo.json
CONNECTION_INFO_JSON = allJsonPath+'connectionInfo.json'

# define constant for complit path of CountryCodeList.json
COUNTRY_CODE_LIST_JSON = allJsonPath+'CountryCodeList.json'

# define constant for complit path of dataEntities.json
DATA_ENTITIES_JSON = allJsonPath+'dataEntities.json'

# define constant for complit path of dateFormats.json
DATE_FORMATS_JSON = allJsonPath+'dateFormats.json'

# define constant for complit path of dead_old.json
DEAD_OLD_JSON = allJsonPath+'dead_old.json'

# define constant for complit path of dead.json
DEAD_JSON = allJsonPath+'dead.json'

# define constant for complit path of dependency.json
DEPENDENCY_JSON = allJsonPath+'dependency.json'

ENGLISH_DICTIONARY_JSON =  allJsonPath+'englist_dictionary.json'

ENTITY_SYNONYMS_JSON_PATH = allJsonPath+'entity_synonyms.json'

# define constant for complit path of EntityMap.json
ENTITY_MAP_JSON = allJsonPath+'EntityMap.json'

# define constant for complit path of entityMetadata.json
ENTITY_METADATA_JSON = allJsonPath+'entityMetadata.json'

# define constant for complit path of entityNamesMap.json
ENTITY_NAMESMAP_JSON = allJsonPath+'entityNamesMap.json'

# define constant for complit path of GPEData.json
GPE_DATA_JSON = allJsonPath+'GPEData.json'

# define constant for complit path of hyperParams.json
HYPERPARAMS_JSON = allJsonPath+'hyperParams.json'

INDIRECT_IDENTIFIERS_JSON = allJsonPath+'indirect_identifiers.json'

# define constant for complit path of INRegularExpression.json
INREGULAR_EXPRESSION_JSON = allJsonPath+'INRegularExpression.json'

LABEL_JSON = allJsonPath+'label.json'
# define constant for complit path of pyfiles.json
PYFILES_JSON = allJsonPath+'pyfiles.json'

# define constant for complit path of output.json
OUTPUT_JSON = allJsonPath+'output.json'

# define constant for complit path of RegularExpression.json
REGULAR_EXPRESSION_JSON = allJsonPath+'RegularExpression.json'

# define constant for complit path of UKRegularExpression.json
UK_REGULAR_EXPRESSION_JSON = allJsonPath+'UKRegularExpression.json'

# define constant for complit path of USRegularExpression.json
US_REGULAR_EXPRESSION_JSON = allJsonPath+'USRegularExpression.json'

aegis_logs = os.environ.get('AEGIS_LOGS')
LOG_FILENAME = aegis_logs + "/python/aegis_python.log"
PYTHON_PATH = aegis_home + "/python"
