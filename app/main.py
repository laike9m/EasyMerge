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
from settings import MERGE_JSON_DIR, MERGEJSON_NOT_EXIST_ERR, OUTPUT, QUIT, \
    debug, MR_TASK
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


@app.route('/new_mr_task/', methods=['POST'])
def init_mr_task():
    print(request.form.items())
    tuple_list = request.form.items()
    mr_task_id = tuple_list[0][0]
    script_location = MR_TASK[mr_task_id]
    kwargs = {}
    if len(tuple_list) > 1:
        json_file_type = tuple_list[1][0]
        json_file_name = tuple_list[1][1]
        if json_file_type == 'merge-json':
            kwargs = {'merge_json': json_file_name}
        if json_file_type == 'gdb-json':
            kwargs = {'gdb_json': json_file_name}

    new_celery_task_id = str(arrow.utcnow().timestamp)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    connection_keeper[str(new_celery_task_id)] = connection
    channel.queue_declare(queue=new_celery_task_id, durable=True, auto_delete=True)

    # apply_async 要放在channle声明之后, 否则会出现无法在connection_keeper中找到new_celery_task_id
    # 这一项的问题, 原因未知
    init_mr_task.apply_async((script_location,), kwargs=kwargs, task_id=new_celery_task_id)
    return redirect('/task/%s' % new_celery_task_id)


@app.route('/task/<string:celery_task_id>')
def task(celery_task_id):
    try:
        channel = connection_keeper[celery_task_id].channel()
        print(request.path)
        if request.args.get('fetch', ''):
            method_frame, header_frame, body = channel.basic_get(celery_task_id)
            if method_frame:
                body = json.loads(body)
                channel.basic_ack(method_frame.delivery_tag)
                if body['type'] == OUTPUT:
                    return jsonify(content=body['content'])
                elif body['type'] == QUIT:
                    app.logger.info("task finish")
                    connection_keeper[celery_task_id].close()
                    del connection_keeper[celery_task_id]
                    return jsonify(content="quit")
                elif body['type'] == MERGEJSON_NOT_EXIST_ERR:
                    app.logger.warning("merge_json_not_exist")
                    connection_keeper[celery_task_id].close()
                    del connection_keeper[celery_task_id]
                    return jsonify(content="merge_json_not_exist", file=body['content'])
                else:
                    app.logger.error("wrong type", body)
                    return "err"
            else:
                print 'No message returned'
                return jsonify(request='')
        else:
            resp = make_response(render_template('task.html'))
            resp.set_cookie('celery_task_id', celery_task_id)
            return resp
    except Exception as e:
        traceback.print_exc()


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
def init_mr_task(self, script_location, merge_json=None, gdb_json=None):
    """ 这个脚本的路径 are relative path to where celery is run
    """
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    task_channel = conn.channel()

    print(script_location, merge_json, gdb_json)

    if debug:
        proc = Popen(['app/sample.sh'], shell=True, stdout=PIPE)
        while True:
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
        return

    # check input file/folder existence

    if not os.path.exists(os.path.join(MERGE_JSON_DIR, merge_json)):
        task_channel.basic_publish(
            exchange='',
            routing_key=self.request.id,
            body=json.dumps({"type": MERGEJSON_NOT_EXIST_ERR, "content": merge_json})
        )
        conn.close()
        return

    # HDFS operations

    if merge_json:
        """说明是task1, 需要先把 merge_json 放到HDFS 的相应位置
        """
        date = arrow.now().format("YYMMDD")
        proc = Popen(
            "hadoop fs -rmr /tmp/sname-merge/%s/%s 2>&1" % (date, merge_json),
            stdout=PIPE
        )
        stdout1, _ = proc.communicate()

        # will cause timeout putting to fs without trailing slash
        merge_json = merge_json+'/' if os.path.isdir(merge_json) else merge_json
        merge_json_abspath = os.path.join(MERGE_JSON_DIR, merge_json)
        proc = Popen(
            "hadoop fs -copyFromLocal %s /tmp/sname-merge/%s/%s 2>&1" %
                (merge_json_abspath, date, merge_json),
            stdout=PIPE
        )
        stdout2, _ = proc.communicate()

        task_channel.basic_publish(
            exchange='',
            routing_key=self.request.id,
            body=json.dumps({"type": OUTPUT, "content": stdout1+'\n'+stdout2})
        )

    if gdb_json:
        """task3, 要先把HDFS 上作为输出路径的文件夹删除掉
        """
        proc = Popen("hadoop fs -rmr %s 2>&1" % gdb_json, stdout=PIPE)
        stdout2, _ = proc.communicate()
        task_channel.basic_publish(
            exchange='',
            routing_key=self.request.id,
            body=json.dumps({"type": OUTPUT, "content": stdout2})
        )

    # execute mapreduce task

    proc = Popen(script_location, shell=True, stdout=PIPE)
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