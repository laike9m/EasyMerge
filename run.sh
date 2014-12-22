#!/bin/bash
cd app
../../bin/gunicorn main:app -b 0.0.0.0:5000
