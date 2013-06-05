from os import getenv

from flask import Flask, request, jsonify, render_template, g, session, abort

from .. import statsd
from ..core.parse_csv import parse_csv
from ..core.errors import ParseError, ValidationError
from ..core import database, log_handler, records, cache_control, config
from ..core.bucket import Bucket
from . import sign_on
from ..core.validation import bucket_is_valid
from .validation import bearer_token_is_valid
from .permissions import Permissions
from .signonotron2 import protected

MAX_UPLOAD_SIZE = 100000


def setup_logging():
    log_handler.set_up_logging(app, "write",
                               getenv("GOVUK_ENV", "development"))


app = Flask(__name__)

# Configuration
config.load(app, 'write')

db = database.Database(
    app.config['MONGO_HOST'],
    app.config['MONGO_PORT'],
    app.config['DATABASE_NAME']
)

setup_logging()

app.before_request(log_handler.create_request_logger(app))
app.after_request(log_handler.create_response_logger(app))

app.permissions = Permissions(app.config["PERMISSIONS"])
if sign_on.use_single_sign_on(app):
    app.secret_key = app.config['SECRET_KEY']
    sign_on.setup(app)


@app.errorhandler(500)
@app.errorhandler(405)
@app.errorhandler(404)
def exception_handler(e):
    app.logger.exception(e)

    bucket_name = getattr(g, 'bucket_name', request.path)
    statsd.incr("write.error", bucket=bucket_name)

    code = getattr(e, 'code', 500)
    name = getattr(e, 'name', "Internal Error")

    return jsonify(status='error', message=name), code


@app.route("/", methods=['GET'])
def index():
    if sign_on.use_single_sign_on(app):
        return render_template("index.html")
    else:
        return "Backdrop is running."


@app.route('/_status', methods=['GET'])
@cache_control.nocache
def health_check():
    if db.alive():
        return jsonify(status='ok', message='database seems fine')
    else:
        return jsonify(status='error',
                       message='cannot connect to database'), 500


@app.route('/<bucket_name>', methods=['POST'])
@cache_control.nocache
def post_to_bucket(bucket_name):
    g.bucket_name = bucket_name

    if not bucket_is_valid(bucket_name):
        return jsonify(status="error",
                       message="Bucket name is invalid"), 400

    tokens = app.config['TOKENS']
    auth_header = request.headers.get('Authorization', None)

    if not bearer_token_is_valid(tokens, auth_header, bucket_name):
        statsd.incr("write_api.bad_token", bucket=g.bucket_name)
        return jsonify(status='error', message='Forbidden'), 403

    try:
        parse_and_store(
            load_json(request.json),
            bucket_name)

        return jsonify(status='ok')
    except (ParseError, ValidationError) as e:
        return jsonify(status="error", message=str(e)), 400


@app.route('/<bucket_name>/upload', methods=['GET', 'POST'])
@protected
def upload(bucket_name):
    if not bucket_is_valid(bucket_name):
        return _invalid_upload("Bucket name is invalid")

    current_user_email = session.get("user").get("email")
    if not app.permissions.allowed(current_user_email, bucket_name):
        return abort(404)

    if request.method == 'GET':
        return render_template("upload_csv.html")

    return _store_csv_data(bucket_name)


def _store_csv_data(bucket_name):
    file_stream = request.files["file"].stream
    try:
        if request.content_length > MAX_UPLOAD_SIZE:
            return _invalid_upload("file too large")
        try:
            parse_and_store(
                parse_csv(file_stream),
                bucket_name)

            return render_template("upload_ok.html")
        except (ParseError, ValidationError) as e:
            return _invalid_upload(e.message)
    finally:
        file_stream.close()


def _invalid_upload(msg):
    return render_template("upload_error.html", message=msg), 400


def load_json(data):
    if data is None:
        raise ValidationError("Request must be JSON")

    if isinstance(data, list):
        return data
    else:
        return [data]


def parse_and_store(incoming_data, bucket_name):
    incoming_records = records.parse_all(incoming_data)

    app.logger.info(
        "request contains %s documents" % len(incoming_records))

    bucket = Bucket(db, bucket_name)
    bucket.store(incoming_records)


def start(port):
    # this method only gets run on dev
    # app.debug = True
    app.run(host='0.0.0.0', port=port)
    app.logger.info("Backdrop Write API started")
