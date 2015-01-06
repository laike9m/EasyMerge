# coding: utf-8

import os
import logging
import json
from logging.handlers import RotatingFileHandler
from subprocess import Popen, PIPE
from flask import Flask, render_template, request, redirect, url_for, jsonify
from flask import send_from_directory, make_response
from celery import Celery
import arrow
import pika
from utils import get_config, update_config, update_and_fetch_mrtask_script
from settings import merge_json_dir, MERGEJSON_NOT_EXIST_ERR, OUTPUT, QUIT, debug
import traceback
from pprint import pprint


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


@app.route('/config/', methods=['GET', 'POST'], defaults={'filepath': ''})
@app.route('/config/<path:filepath>', methods=['GET', 'POST'])
def read_or_write_config(filepath):
    """
    GET: 读取配置文件并在页面显示
    POST: 更新配置文件, 返回依照页面上填写信息生成的shell命令
    """
    if request.method == 'GET':
        return jsonify(**get_config(filepath))
    if request.method == 'POST':
        configs = {t[0]: t[1] for t in request.form.items()}
        configs["channel"] = request.form.getlist("channel")
        pprint(configs)

        update_config('merge.xml', configs)
        update_config(configs['core-site'], configs)
        update_config(configs['mapred-site'], configs)
        update_config(configs['hbase-site'], configs)

        return jsonify(update_and_fetch_mrtask_script(configs))


@app.route('/task/<string:task_id>')
def task(task_id):
    try:
        channel = connection_keeper[task_id].channel()
        print(request.path)
        if request.args.get('fetch', ''):
            method_frame, header_frame, body = channel.basic_get(task_id)
            if method_frame:
                body = json.loads(body)
                channel.basic_ack(method_frame.delivery_tag)
                if body['type'] == OUTPUT:
                    return jsonify(content=body['content'])
                elif body['type'] == QUIT:
                    app.logger.info("task finish")
                    connection_keeper[task_id].close()
                    del connection_keeper[task_id]
                    return jsonify(content="quit")
                elif body['type'] == MERGEJSON_NOT_EXIST_ERR:
                    app.logger.warning("merge_json_not_exist")
                    connection_keeper[task_id].close()
                    del connection_keeper[task_id]
                    return jsonify(content="merge_json_not_exist", file=body['content'])
                else:
                    app.logger.error("wrong type", body)
                    return "err"
            else:
                print 'No message returned'
                return jsonify(request='')
        else:
            resp = make_response(render_template('task.html'))
            resp.set_cookie('task_id', task_id)
            return resp
    except Exception as e:
        traceback.print_exc()


@app.route('/new_mr_task/', methods=['POST'])
def init_mr_task():

    new_task_id = str(arrow.utcnow().timestamp)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    connection_keeper[str(new_task_id)] = connection
    channel.queue_declare(queue=new_task_id, durable=True, auto_delete=True)

    # apply_async 要放在channle声明之后, 否则会出现无法在connection_keeper中找到new_task_id
    # 这一项的问题, 原因未知
    init_mr_task.apply_async((channels, merge_json), task_id=new_task_id)
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
def init_mr_task(self, channel, merge_json):
    """ 这个脚本的路径 are relative path to where celery is run
    """
    print("channel:", channel)
    print("merge-json:", merge_json)
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    task_channel = conn.channel()

    if not debug:
        if not os.path.exists(os.path.join(merge_json_dir, merge_json)):
            task_channel.basic_publish(
                exchange='',
                routing_key=self.request.id,
                body=json.dumps({"type": MERGEJSON_NOT_EXIST_ERR, "content": merge_json})
            )
            conn.close()
            return

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
                body=json.dumps({"type": OUTPUT, "content": line})
            )
        else:
            task_channel.basic_publish(
                exchange='',
                routing_key=self.request.id,
                body=json.dumps({"type": QUIT})
            )
            break
    proc.communicate()
    conn.close()


if __name__ == '__main__':
    app.run(debug=True)