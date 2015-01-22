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
from utils import get_config, update_config, update_and_fetch_mrtask_script, \
    set_ada_merge_dir, get_path_in_ada_merge_dir
from settings import *
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

@app.route('/upload/', methods=['POST'])
def upload():
    return_json = {'success': False, 'uploaded': [], 'error': ''}
    pprint(request.files.getlist("file"))
    upload_path = request.form.get('upload_path')

    for file in request.files.getlist("file"):
        file_dir = os.path.join(upload_path, os.path.dirname(file.filename))
        if not os.path.exists(file_dir):
            try:
                os.makedirs(file_dir)  # create subdir for upload file
            except Exception as e:
                return_json['error'] = str(e)
                app.logger.error(str(e))
                return jsonify(return_json)
        try:
            file.save(os.path.join(upload_path, file.filename))
        except Exception as e:
            return_json['error'] = str(e)
            app.logger.error(str(e))
            return jsonify(return_json)
        else:
            return_json['uploaded'].append(file.filename)
            app.logger.info("upload success: " + file.filename)

    return_json['success'] = True
    return jsonify(return_json)


@app.route('/')
def index():
    ada_merge_dir = request.args.get("ada_merge_dir")
    if ada_merge_dir:
        try:
            set_ada_merge_dir(ada_merge_dir)
        except IOError:
            return "no merge.xml in directory: " + ada_merge_dir

    json_obj = json.load(open("../config.json"))
    ada_merge_dir_list = json_obj["ada_merge_dir_list"]
    ada_merge_dir = json_obj["ada_merge_dir"]
    MERGE_JSON_DIR = json_obj["merge_json_dir"]
    merge_json_candidates = os.listdir(MERGE_JSON_DIR)
    config = get_config("merge.xml")
    return render_template('index.html', config=config,
                           ada_merge_dir_list=ada_merge_dir_list,
                           ada_merge_dir=ada_merge_dir,
                           MERGE_JSON_DIR=MERGE_JSON_DIR,
                           merge_json_candidates=merge_json_candidates)


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
        if "merge-json-local" not in configs:
            configs["merge-json-local"] = ''

        update_config('merge.xml', configs)
        update_config(configs['core-site'], configs)
        update_config(configs['mapred-site'], configs)
        update_config(configs['hbase-site'], configs)

        MERGE_JSON_DIR = json.load(open("../config.json"))["merge_json_dir"]
        merge_json_local = os.path.join(MERGE_JSON_DIR, configs['merge-json-local'])
        return_json = update_and_fetch_mrtask_script(configs)
        return_json.update({'merge_json_local': merge_json_local})
        return jsonify(return_json)


@app.route('/new_mr_task/', methods=['POST'])
def init_mr_task():
    print(request.form.items())
    form_data = {item[0]: item[1] for item in request.form.items()}
    for key in form_data.keys():
        if key in ('1', '2', '3'):
            mr_task_type = key
            break
    script_location = get_path_in_ada_merge_dir(MR_TASK[mr_task_type])
    kwargs = {}
    if len(form_data) > 1:
        if mr_task_type == '1':
            kwargs = {
                'merge_json_hdfs': form_data['merge-json-hdfs'],
                'merge_json_local': form_data['merge-json-local']
                # 这里传过来的 merge-json-local 已经是完整的绝对路径了, 而非文件名
            }
        if mr_task_type == '3':
            kwargs = {'gdb_json': form_data['gdb-json']}

    print(kwargs)
    new_celery_task_id = str(arrow.utcnow().timestamp)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    connection_keeper[str(new_celery_task_id)] = connection
    channel.queue_declare(queue=new_celery_task_id, durable=True, auto_delete=True)

    # apply_async 要放在channle声明之后,
    # 否则会出现无法在connection_keeper中找到new_celery_task_id这一项的问题, 原因未知
    init_mr_task.apply_async((script_location,), kwargs=kwargs,
                             task_id=new_celery_task_id)
    return redirect('/task/%s?mr_task_type=%s' % (new_celery_task_id, mr_task_type))


@app.route('/task/<string:celery_task_id>')
def task(celery_task_id):
    try:
        channel = connection_keeper[celery_task_id].channel()
        if request.args.get('fetch', ''):
            method_frame, header_frame, body = channel.basic_get(celery_task_id)
            if method_frame:
                try:
                    body = json.loads(body)
                except ValueError:
                    app.logger.error("json decode error: " + body)
                    return jsonify(request='')
                channel.basic_ack(method_frame.delivery_tag)
                if body['type'] == OUTPUT:
                    return jsonify(content=body['content'])
                elif body['type'] == QUIT:
                    app.logger.info("task finish")
                    connection_keeper[celery_task_id].close()
                    channel.basic_cancel(celery_task_id)
                    del connection_keeper[celery_task_id]
                    return jsonify(content="quit")
                elif body['type'] == MERGEJSON_NOT_EXIST_ERR:
                    app.logger.warning("merge_json_not_exist")
                    connection_keeper[celery_task_id].close()
                    channel.basic_cancel(celery_task_id)
                    del connection_keeper[celery_task_id]
                    return jsonify(content="merge_json_not_exist", file=body['content'])
                else:
                    app.logger.error("wrong type", body)
                    return "err"
            else:
                return jsonify(request='')
        else:
            mr_task_type = request.args.get("mr_task_type")
            resp = make_response(render_template('task.html', mr_task_type=mr_task_type))
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
def init_mr_task(self, script_location, merge_json_local=None, merge_json_hdfs=None, gdb_json=None):
    """ 这个脚本的路径 are relative path to where celery is run
    :merge_json merge_json 在 HDFS 的路径
    """
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    task_channel = conn.channel()

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

    # HDFS operations

    if merge_json_local:
        """task1, 需要先把 merge_json 放到 HDFS 的相应位置
        """
        # check input file/folder existence

        if not os.path.exists(merge_json_local):
            task_channel.basic_publish(
                exchange='',
                routing_key=self.request.id,
                body=json.dumps({
                    "type": MERGEJSON_NOT_EXIST_ERR,
                    "content": merge_json_local
                })
            )
            conn.close()
            return

        proc = Popen("hadoop fs -rmr %s 2>&1" % merge_json_hdfs, shell=True,
                     stdout=PIPE)
        stdout1, _ = proc.communicate()

        # will cause timeout putting to fs without trailing slash
        merge_json_local = merge_json_local+'/' \
            if os.path.isdir(merge_json_local) else merge_json_local
        proc = Popen(
            "hadoop fs -copyFromLocal %s %s 2>&1" % (merge_json_local, merge_json_hdfs),
            shell=True,
            stdout=PIPE
        )
        stdout2, _ = proc.communicate()

        task_channel.basic_publish(
            exchange='',
            routing_key=self.request.id,
            body=json.dumps({"type": OUTPUT, "content": stdout1+'\n'+stdout2})
        )

    if gdb_json:
        """task3, 要先把 HDFS 上作为输出路径的文件夹删除掉
        """
        proc = Popen("hadoop fs -rmr %s 2>&1" % gdb_json, shell=True,
                     stdout=PIPE)
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