# coding: utf-8

import os
from subprocess import Popen, PIPE
from flask import Flask, render_template, request, redirect, url_for
from flask import send_from_directory
from celery import Celery
import utils

app = Flask(__name__, template_folder='../templates')


@app.route('/')
def hello():
    config = utils.get_config()
    return render_template('index.html', config=config)


@app.route('/js/<path:filename>')
def serve_static(filename):
    root_dir = os.path.dirname(os.getcwd())
    return send_from_directory(os.path.join(root_dir, 'static', 'js'), filename)


@app.route('/new_mr_task/', methods=['POST'])
def init_mr_task():
    print(request.method)
    print(request.form.items())
    init_mr_task.delay()
    return redirect(url_for('hello'))


def make_celery(app):
    celery = Celery(app.import_name,
                    broker=app.config['CELERY_BROKER_URL'],
                    )
    celery.conf.update(app.config)
    #celery.conf.update(CELERY_IMPORTS=['main'])
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

app.config.update(
    CELERY_BROKER_URL='amqp://localhost:5672',
    CELERY_RESULT_BACKEND='amqp://'
)
celery = make_celery(app)


@celery.task(name="app.main.init_mr_task")
def init_mr_task():
    """ 这个脚本的路径 are relative path to where celery is run
    """
    Popen(['app/sample.sh'], shell=True, stdout=PIPE)


if __name__ == '__main__':
    app.run(debug=True)