import os
from flask import Flask, render_template, request, redirect
from flask import send_from_directory
import utils

app = Flask(__name__, template_folder='../templates')


@app.route('/')
def hello():
    config = utils.get_config()
    return render_template('index.html', config=config)


@app.route('/js/<path:filename>')
def serve_static(filename):
    root_dir = os.path.dirname(os.getcwd())
    print(os.path.join(root_dir, 'static', 'js', filename))
    return send_from_directory(os.path.join(root_dir, 'static', 'js'), filename)


@app.route('/new_mr_task/', methods=['POST'])
def init_mr_task():
    print(request.method)
    print(request.form.items())
    return redirect(url_for('hello'))


if __name__ == '__main__':
    app.run(debug=True)