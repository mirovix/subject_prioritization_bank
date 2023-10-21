# ANTI-MONEY LAUNDERING ANOMALY DETECTION (Small part)

## Utilizzo

### Installazione
```
python.exe -m pip install link.whl
```

link -> http://sonar.netechgroup.local:8081/repository/kassandra/packages/...

## Esecuzione Kassandra
```
python.exe -m kassandra <codice_intermediario> <mese_riferimento>
```
### Esempio
```
python.exe -m kassandra 123456 062023
```

## Esecuzione del servizio Monitoring Kassandra
```
python.exe -m monitoring_kassandra <codice_intermediario>
```
### Esempio
```
python.exe -m monitoring_kassandra 123456
```

### Esempio dei vari moduli
#### Creazione e utilizzo di un oggetto Registry

```python
from kassandra.registry_management import Registry


# create a registry
engine_connection = 'oracle://user:password@host:port/sid'
registry = Registry(engine=engine_connection)

# create a table
registry.create_registry_table_index()

# load a registry
registry.load_on_registry(system_id="KASSANDRA",
                          control_code="0001",
                          intermediary_code="123456",
                          client_id="0000000000000001",
                          prediction=0.5,
                          model_name_date="20230927.joblib")


# get table object (sqlalchemy)
table = registry.get_table()

# execute a query
registry.execute_query(query="select * from REGISTRY_TEST")

# get table name
table_name = registry.get_table_name()

# get columns of the table (list of sqlalchemy columns)
columns_registry = registry.get_columns_registry()

# delete a registry from the database
registry.delete_registry()

# get registry from the database last prediction
last_predictions = registry.get_last_prediction(system_id="KASSANDRA",
                                                control_code="0001",
                                                intermediary_code="123456")
``` 

#### Creazione e utilizzo del censimento su Evaluation

```python
from kassandra.self_census import SelfCensus

# information for the self census
url_link_test = "http://192.168.1.25/Evaluation"
system_id_name_test = "Kassandra"
system_id_description_test = "Kassandra"

# create a self census and execute it
self_census = SelfCensus(url_link=url_link_test,
                         system_id_name=system_id_name_test,
                         system_id_description=system_id_description_test)()
```

#### Creazione e utilizzo della parametrizzazione

```python
# note: 'intermediary_code', control_code and 'system_id' must be registered in the database before running the code

from kassandra.parametrization import ParameterManagement
from kassandra.parametrization import Parameter

# create a parameter management
url_link_test = 'http://localhost:8080/DiscoveryApi'
system_id_test = 'Kassandra'
control_code_test = '01'

# create a parameter management
parameter_management = ParameterManagement(system_id=system_id_test,
                                           control_code=control_code_test)

# create a parameters
parameter = Parameter(place_holder='test', value="\"test\"", description='test', value_type='string')
parameter2 = Parameter(place_holder='test2', value="\"1\"", description='test2', value_type='numeric')
parameter3 = Parameter(place_holder='test3', value="[\"val1\", \"val2\"]", description='test3', value_type='string_list')

# get a parameter through api rest, if 'parameter_place_holder' is None, all parameters will be returned
# note: the api rest must be running (DisocveryApi) and the parameter must be registered in the database
# the 'engine_connection' is not necessary in this case
param_api = parameter_management.get_parameters_api(url_api=url_link_test, parameter_place_holder='test')
```

#### Estrazione dei dati da Evaluation e DWA 

```python
from kassandra.extraction import ExtractData
from kassandra.registry_management import Registry

# connections 
engine_evaluation = 'oracle://user:password@host:port/sid'
engine_dwa = 'oracle://user:password@host:port/sid'

# registry
engine = 'oracle://user:password@host:port/sid'
registry = Registry(engine=engine)

# data
system_id = 'KASSANDRA'
control_code = '00001'
intermediary_code = '123456'
ref_month = '062023'
registry_month_to_skip = 12
reported_other_systems = ['DISCOVERY', 'COMPORTAMENT']

# create a extraction
extract_data = ExtractData(engine_evaluation=engine_evaluation,
                           engine_dwa=engine_dwa,
                           registry=registry,
                           system_id=system_id,
                           control_code=control_code,
                           intermediary_code=intermediary_code,
                           ref_month=ref_month,
                           registry_month_to_skip=registry_month_to_skip,
                           reported_other_systems=reported_other_systems)

# get ndgs from dwa
ndgs = extract_data.get_ndgs()

# get operations from dwa
operations = extract_data.get_operations()

# get operation subjects from dwa
operation_subjects = extract_data.get_operations_subjects()

# get subjects information from dwa
subjects_information = extract_data.get_subjects()

# get target from evaluation
target = extract_data.get_target()

# get ndg in registry
ndg_registry = extract_data.get_ndgs_from_registry()

# get ndg from evaluated in other systems
ndg_other_systems = extract_data.get_ndgs_from_other_systems()
```

#### Features creation and selection

```python
import joblib
from kassandra.features_creation import BuildFeaturesProd
    
# load the model
model = joblib.load('...')

# features creation and selection for production (information are extracted from the database)
build_features_prod = BuildFeaturesProd(operations=operations,
                                        operation_subjects=operation_subjects,
                                        subjects=subjects_information,
                                        target_information=target,
                                        bank_months=12)
dataset_categorized = build_features_prod.get_transformed_prod()
shap_explainer = build_features_prod.get_shap_explainer(model_object=model)
```

#### Prediction and post processing

```python
import joblib
import pandas as pd
import shap
from kassandra.extraction import ExtractData
from kassandra.registry_management import Registry, RegistryLastPrediction
from kassandra.prediction_and_loading.features import FeatureCategories
from kassandra.prediction_and_loading import SubjectManagement
from ml_anomaly_gate import AnomalyGate

# connections 
engine = 'oracle://user:password@host:port/sid'

# registry
registry = Registry(engine=engine)
registry_last_prediction = RegistryLastPrediction(engine=engine)

# data
ref_month = '062023'
system_id = 'KASSANDRA'
control_code = '00001'
intermediary_code = '123456'
id_transition = '0001'
name_id = 'default'
description = "Kassandra01"

# object that manage each feature according to its category
features_name = FeatureCategories(description_features=...,
                                  boolean_features=...,
                                  numerical_features=...,
                                  currency_features=...,
                                  binary_features=...)

# create a anomaly gate and define base information
gate = AnomalyGate(system=system_id, id_transition=id_transition)  # create a new anomaly gate
gate.define_id(code=control_code, bank_code=intermediary_code, name=name_id)  # define id of the anomaly (optional)
gate.define_basic_information(code=control_code, bank_code=intermediary_code, description=description)  # define basic information of the anomaly

# subject data from dwa
extract_data = ExtractData(...)
subjects_information = extract_data.get_subjects()
ndg_registry = extract_data.get_ndgs_from_registry()
ndg_other_systems = extract_data.get_ndgs_from_other_systems()

# load the model and the explainer
model = joblib.load("...")
test_df = pd.read_csv("...")
explainer = shap.Explainer(model.predict, test_df, seed=1)

# get data from feature creation/selection process (the name is will be changed)
features_processed = feature_creation_selection(...)

subject_management = SubjectManagement(model_object=model, 
                                       model_date_name='model_name',
                                       shap_explainer=explainer, 
                                       registry_object=registry,
                                       registry_last_prediction_object=registry_last_prediction,
                                       evaluation_connection=engine, 
                                       anomaly_gate=gate,
                                       features_categories=features_name, 
                                       subjects_info=subjects_information,
                                       ref_month_anomaly=ref_month)

# predict the anomaly and postprocessing the results
subject_management(input_to_predict=features_processed,                                       
                   ndgs_from_registry=ndg_registry,
                   ndgs_from_other_systems=ndg_other_systems)

```

## Creazione del pacchetto
### Build del pacchetto

```
python.exe -m setup build --version ver sdist bdist_wheel
```
ver -> versione del pacchetto (es. 1.0.0)

### Caricamento del pacchetto sulla repository

```
python.exe -m twine upload --repository-url url dist/*
```

url -> http://sonar.netechgroup.local:8081/repository/kassandra/
