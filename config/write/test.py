SINGLE_SIGN_ON = True
SECRET_KEY = "something unique and secret"
CLIENT_ID = "it's not important here"
CLIENT_SECRET = "it's not important here"

TOKENS = {
    '_bucket': '_bucket-bearer-token',
    '_status': '_status-bearer-token',  # not expected to be here
    'bucket': 'bucket-bearer-token',
    'data_with_times': 'data_with_times-bearer-token',
    'foo': 'foo-bearer-token',
    'foo_bucket': 'foo_bucket-bearer-token',
    'flavour_events': 'flavour_events-bearer-token',
    'licensing': 'licensing-bearer-token',
    'my_dinosaur_bucket': 'my_dinosaur_bucket-bearer-token',
    'reptiles': 'reptiles-bearer-token',
    'month': 'month-bearer-token',
    'month_no_raw_access': 'month_no_raw_access-bearer-token'
}
