#!/usr/bin/env python3

"""
@Author: Miro
@Date: 22/05/2023
@Version: 1.0
@Objective: esecuzione test di unità
@TODO:
"""

import importlib.util
import logging
import os
import sys
import unittest

if __name__ == "__main__":
    os.environ['TEST_ENV'] = '1'
    sys.stdout = open(os.devnull, 'w')
    script_directory = os.path.dirname(os.path.abspath(__file__))

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Search in "test/"
    for root, dirs, files in os.walk(os.path.join(script_directory, "test")):
        for file in files:
            if file.endswith(".py"):
                module_path = os.path.join(root, file)
                module_name = os.path.splitext(file)[0]

                # Import test module
                spec = importlib.util.spec_from_file_location(module_name, module_path)
                test_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(test_module)

                suite.addTests(loader.loadTestsFromModule(test_module))

    runner = unittest.TextTestRunner(verbosity=2)
    test_result = runner.run(suite)

    logging.shutdown()
    # check if there are any failures
    if test_result.wasSuccessful():
        sys.exit(0)
    else:
        sys.exit(1)
