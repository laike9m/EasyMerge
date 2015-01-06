#!/bin/bash
cd $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
python -m unittest test.test_xml_update
python -m unittest test.test_flask_response
