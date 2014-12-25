# coding: utf-8

import os
from subprocess import Popen, PIPE
from flask import Flask, render_template, request, redirect, url_for, jsonify
from flask import send_from_directory, make_response
from celery import Celery
import arrow
import pika
import utils

app = Flask(__name__, template_folder='../templates', static_folder='../static')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()


@app.route('/')
def index():
    config = utils.get_config()
    return render_template('index.html', config=config)


@app.route('/task/<string:task_id>')
def task(task_id):
    print(request.path)
    if request.args.get('fetch', ''):
        method_frame, header_frame, body = channel.basic_get(task_id)
        if method_frame:
            channel.basic_ack(method_frame.delivery_tag)
            if body == "quit":
                print("task finish")
            print method_frame, header_frame, body
            return jsonify(result=body)
        else:
            print 'No message returned'
            return jsonify(request='')
    else:
        resp = make_response(render_template('task.html'))
        resp.set_cookie('task_id', task_id)
        return resp


@app.route('/new_mr_task/', methods=['POST'])
def init_mr_task():
    print(request.method)
    print(request.form.items())
    new_task_id = str(arrow.utcnow().timestamp)
    init_mr_task.apply_async(task_id=new_task_id)
    channel.queue_declare(queue=new_task_id, durable=True, auto_delete=True)
    return redirect('/task/%s' % new_task_id)


def make_celery(app):
    celery = Celery(app.import_name,
                    broker=app.config['CELERY_BROKER_URL'],
                    backend=app.config['CELERY_RESULT_BACKEND']
                    )
    celery.conf.update(app.config)
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
    CELERY_RESULT_BACKEND='amqp://',
    CELERY_QUEUE_HA_POLICY='all',
    CELERY_TASK_RESULT_EXPIRES=None
)
celery = make_celery(app)


@celery.task(name="app.main.init_mr_task", bind=True)
def init_mr_task(self):
    """ 这个脚本的路径 are relative path to where celery is run
    """
    proc = Popen(['app/sample.sh'], shell=True, stdout=PIPE)
    while proc.returncode is None:  # running
        line = proc.stdout.readline()
        print line
        if line:
            channel.basic_publish(
                exchange='',
                routing_key=self.request.id,
                body=line
            )
        else:
            channel.basic_publish(
                exchange='',
                routing_key=self.request.id,
                body="quit"
            )
            break


if __name__ == '__main__':
    app.run(debug=True)