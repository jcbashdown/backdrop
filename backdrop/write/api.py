from os import getenv

from flask import Flask, request, jsonify, render_template, abort
from backdrop.core.parse_csv import parse_csv, \
    MoreValuesThanColumnsException, MissingValuesForSomeColumnsException
from backdrop.core.log_handler \
    import create_request_logger, create_response_logger

from .validation import validate_post_to_bucket
from ..core import database, log_handler, records, cache_control
from ..core.bucket import Bucket


def setup_logging():
    log_handler.set_up_logging(app, "write",
                               getenv("GOVUK_ENV", "development"))


def environment():
    return getenv("GOVUK_ENV", "development")


app = Flask(__name__)

# Configuration
app.config.from_object(
    "backdrop.write.config.%s" % environment()
)

db = database.Database(
    app.config['MONGO_HOST'],
    app.config['MONGO_PORT'],
    app.config['DATABASE_NAME']
)

setup_logging()

app.before_request(create_request_logger(app))
app.after_request(create_response_logger(app))


@app.errorhandler(500)
@app.errorhandler(405)
@app.errorhandler(404)
def exception_handler(e):
    app.logger.exception(e)
    code = (e.code if hasattr(e, 'code') else None)
    return jsonify(status='error', message=''), code


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
    def extract_bearer_token(header):
        if header is None or len(header) < 8:
            return ''
            # Strip the leading "Bearer " from the header value
        return header[7:]

    expected_token = app.config['TOKENS'].get(bucket_name, None)
    auth_header = request.headers.get('Authorization', None)
    request_token = extract_bearer_token(auth_header)

    if request_token != expected_token:
        app.logger.error("expected <%s> but was <%s>" %
                         (expected_token, request_token))
        return jsonify(status='error', message='Forbidden'), 403

    if request.json is None:
        app.logger.error("Request must be JSON")
        return jsonify(status='error', message='Request must be JSON'), 400

    incoming_data = prep_data(request.json)

    app.logger.info("request contains %d documents" % len(incoming_data))

    result = validate_post_to_bucket(incoming_data, bucket_name)

    # TODO: We currently don't test that incoming data gets validated
    # feels too heavy to be in the controller anyway - pull out later?
    if not result.is_valid:
        app.logger.error(result.message)
        return jsonify(status='error', message=result.message), 400

    incoming_records = []

    for datum in incoming_data:
        incoming_records.append(records.parse(datum))

    bucket = Bucket(db, bucket_name)
    bucket.store(incoming_records)

    return jsonify(status='ok')


@app.route('/<bucket_name>/upload', methods=['GET', 'POST'])
def get_upload(bucket_name):
    if request.method == 'GET':
        return render_template("upload_csv.html")
    elif request.method == 'POST':
        if request.content_length > 100000:
            abort(411)
        try:
            data = parse_csv(request.files["file"].stream)
            data = [records.parse(datum) for datum in data]

            bucket = Bucket(db, bucket_name)
            bucket.store(data)
        except MoreValuesThanColumnsException:
            return "Some rows in the csv file contain more values " \
                   "than the specified number of columns", 400
        except MissingValuesForSomeColumnsException:
            return "Some rows in the csv file contain less values " \
                   "than the specified number of columns", 400
    return "ok"


def prep_data(incoming_json):
    if isinstance(incoming_json, list):
        return incoming_json
    else:
        return [incoming_json]


def start(port):
    # this method only gets run on dev
    # app.debug = True
    app.run(host='0.0.0.0', port=port)
    app.logger.info("Backdrop Write API started")
