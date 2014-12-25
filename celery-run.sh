#!/bin/bash
celery -A app.main.celery worker --loglevel=info
