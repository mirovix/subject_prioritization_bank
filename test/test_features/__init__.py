import os

base_path = os.path.dirname(__file__)
data_path = os.path.join(base_path, 'data')

target_path = os.path.join(data_path, 'test_target.csv')
operation_subject_path = os.path.join(data_path, 'test_operations_subjects.csv')
operation_path = os.path.join(data_path, 'test_operations.csv')
subject_path = os.path.join(data_path, 'test_subjects.csv')

build_features_result_path = os.path.join(data_path, 'test_build_features_result.csv')
transformed_train_path = os.path.join(data_path, 'test_transformed_dataset.csv')

operations_result_path = os.path.join(data_path, 'test_operations_result.csv')