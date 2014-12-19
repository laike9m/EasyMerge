from flask import Flask, render_template, request
import utils

app = Flask(__name__, template_folder='../templates')


@app.route('/')
def hello():
    config = utils.get_config()
    return render_template('index.html', config=config)


@app.route('/new_mr_task/', methods=['POST'])
def init_mr_task():
    print(request.method)
    print(request.form.items())
    return "succeed"


if __name__ == '__main__':
    app.run(debug=True)