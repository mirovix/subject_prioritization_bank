"""

@Author: msalihi
@Date: 04/07/2023
@Version: 1.0
@Objective: testing the subject prediction train object.

"""

import os
from kassandra.config_module import app_config as apc
import unittest
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from train.subject_prediction_train import SubjectPrediction


class TestSubjectPrediction(unittest.TestCase):
    def setUp(self):
        # Create sample data for testing
        self.x = pd.DataFrame({'Feature1': [1, 2, 3], 'Feature2': [4, 5, 6]})
        self.y = pd.DataFrame({'STATUS': [0, 1, 0]})
        self.threshold = 0.5

        # Create a random forest classifier for testing
        self.model = RandomForestClassifier()
        self.model.fit(self.x, self.y['STATUS'])

        self.bins = 20
        self.classes = [0, 1]
        self.normalize = False

        # Create a SubjectPrediction instance for testing
        self.subject_prediction = SubjectPrediction(x_test=self.x, y_test=self.y, model=self.model,
                                                    threshold=self.threshold, model_name='test_model',
                                                    show_plots=False, save_plots=False)

    def test_get_confusion_matrix_parameters(self):
        self.subject_prediction()

        # Ensure the result files are created
        self.assertTrue(os.path.exists(f'{apc.result_directory}/predictions_result.csv'))

    def tearDown(self):
        result_files = [f'{apc.result_directory}/predictions_result.csv']
        for file_path in result_files:
            if os.path.exists(file_path):
                os.remove(file_path)


if __name__ == '__main__':
    unittest.main()
