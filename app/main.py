# coding: utf-8

import os
import logging
from logging.handlers import RotatingFileHandler
from subprocess import Popen, PIPE
from flask import Flask, render_template, request, redirect, url_for, jsonify
from flask import send_from_directory, make_response
from celery import Celery
import arrow
import pika
from utils import get_config
import traceback


file_abspath = os.path.abspath(__file__)
EasyMerge_root = os.path.dirname(os.path.dirname(file_abspath))
app = Flask(__name__, template_folder='../templates', static_folder='../static')
handler = RotatingFileHandler(
    os.path.join(EasyMerge_root, 'logs', 'flask.log'),
    maxBytes=10000, backupCount=1
)
handler.setLevel(logging.INFO)
app.logger.addHandler(handler)

connection_keeper = {}


@app.route('/')
def index():
    config = get_config("merge.xml")
    return render_template('index.html', config=config)


@app.route('/config/<path:filepath>')
def show_config(filepath):
    return jsonify(**get_config(filepath))


@app.route('/task/<string:task_id>')
def task(task_id):
    channel = connection_keeper[task_id].channel()
    print(request.path)
    if request.args.get('fetch', ''):
        method_frame, header_frame, body = channel.basic_get(task_id)
        if method_frame:
            channel.basic_ack(method_frame.delivery_tag)
            if body == "quit":
                print("task finish")
                connection_keeper[task_id].close()
            print method_frame, header_frame, body
            del connection_keeper[task_id]
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
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    connection_keeper[str(new_task_id)] = connection
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
    CELERY_TASK_RESULT_EXPIRES=None,
    PROPAGATE_EXCEPTIONS=True
)
celery = make_celery(app)


@celery.task(name="app.main.init_mr_task", bind=True)
def init_mr_task(self):
    """ 这个脚本的路径 are relative path to where celery is run
    """
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    task_channel = conn.channel()
    if os.path.exists('ada-merge/celery-mr-task.sh'):
        proc = Popen(['ada-merge/celery-mr-task.sh'], shell=True, stdout=PIPE)
    else:
        proc = Popen(['app/sample.sh'], shell=True, stdout=PIPE)
    while proc.returncode is None:  # running
        line = proc.stdout.readline()
        print line
        if line:
            task_channel.basic_publish(
                exchange='',
                routing_key=self.request.id,
                body=line
            )
        else:
            task_channel.basic_publish(
                exchange='',
                routing_key=self.request.id,
                body="quit"
            )
            break
    proc.communicate()
    conn.close()


if __name__ == '__main__':
    app.run(debug=True)