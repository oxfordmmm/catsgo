# Test catsgo.py with clockwork config
# Make a config file for testing server: https://cats.oxfordfun.com
# Run all tests: python3 test_catsgo_clockwork.py
# Run one test: python3 test_catsgo_clockwork.py TestCatsgo.test_fetch
# Run code coverage: coverage run test_catsgo_clockwork.py
# View code coverage report: coverage report -m
# Generate code coverage html report: coverage html

import unittest
import json
import os

import catsgo
import utils

class TestCatsgo(unittest.TestCase):
    def test_load_config(self):
        expected_sp3url = "https://cats.oxfordfun.com"
        result = utils.load_config("config.json")
        self.assertEqual(expected_sp3url, result['sp3_url'])

    def test_login(self):
        result = catsgo.login()
        self.assertTrue(result != 'yes')

    def test_fetch(self):
        result = catsgo.fetch("/data/inputs/uploads/oxforduni/sp3_test_data")
        self.assertTrue("guid" in result.keys())

    def test_run_clockwork(self):
       fetch_result = catsgo.fetch("/data/inputs/uploads/oxforduni/sp3_test_data")
       result = catsgo.run_clockwork("sp3test1-Clockwork_combined", fetch_result["guid"])
       self.assertTrue("run_uuid" in result.keys())

    def test_check_run(self):
       fetch_result = catsgo.fetch("/data/inputs/uploads/oxforduni/sp3_test_data")
       run_result = catsgo.run_clockwork("sp3test1-Clockwork_combined", fetch_result["guid"])
       result = catsgo.check_run("sp3test1-Clockwork_combined", run_result["run_uuid"])
       print(result)

    def test_run_info(self):
       fetch_result = catsgo.fetch("/data/inputs/uploads/oxforduni/sp3_test_data")
       run_result = catsgo.run_clockwork("sp3test1-Clockwork_combined", fetch_result["guid"])
       result = catsgo.run_info("sp3test1-Clockwork_combined", run_result["run_uuid"])
       print(result)

    def test_go(self):
        catsgo.go("/data/inputs/uploads/oxforduni/sp3_test_data")

if __name__ == "__main__":
    unittest.main()
