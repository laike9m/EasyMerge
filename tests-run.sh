#!/bin/bash
cd $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
python -m unittest test.test_utils
python -m unittest test.test_flask_response
