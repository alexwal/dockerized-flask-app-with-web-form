import sys, os, boto3, threading, queue, json
from flask import Flask, render_template, flash, request
from wtforms import Form, TextField, validators

from SQS_threading import SQSSendMessagesThread
from SQS_threading import SQS_utils
import S3_utils


# App config.
app = Flask(__name__)
app.config['SECRET_KEY'] = '8a441f27d44i131f27567d441f2b6176a'

class ReusableForm(Form):
    s3uri = TextField('s3uri:', validators=[validators.required()])

'''
publishes batches of documents to in_queue (creates a new one when necessary)
'''

def create_raw_message(cid, url):
  '''
  ...
  '''
  # Also, maybe we should track source tsv?
  raw_message = {'cid': cid, 'url': url}
  return json.dumps(raw_message)

def get_sqs_dest_queue():
  '''
  returns in_queue object
  '''
  # Queue name and input data file
  in_queue = os.getenv('IN_QUEUE_NAME')
  sqs = SQS_utils.get_sqs()
  # Prepare SQS by deleting existing queues
  delete_queues = False
  if delete_queues:
    SQS_utils.try_delete_queues(sqs,
                            os.getenv('IN_QUEUE_NAME'),
                            os.getenv('OUT_QUEUE_NAME'),
                            os.getenv('LOG_QUEUE_NAME'))
  in_q = SQS_utils.create_queue(sqs, in_queue)
  return in_q

def check_message_for_size(message):
  # A batch of messages must have less than SQS_MAX_MESSAGE_SIZE bytes in total.
  # Here, we are checking one message in a batch.
  SQS_MAX_MESSAGE_SIZE = 262144 # bytes
  assert sys.getsizeof(message) < SQS_MAX_MESSAGE_SIZE // FILL_BATCH_SIZE, 'A batch of messages must have less than SQS_MAX_MESSAGE_SIZE bytes in total.'

def fill_task():
  # Make sure to add all the env vars to terraform/*/main.tf -> locals { env_vars {...}}.
  while True:
    print('Thread running!')
    s3_tsv_uri = s3_uri_queue.get()
    try:
      bucket_name, tsv_file = S3_utils.s3_uri_to_bucket_and_filename(s3_tsv_uri)
      # Get a pandas object with rows equivalent to the rows in tsv_file.
      tsv_data = S3_utils.read_tsv(bucket_name, tsv_file)
    except Exception as e:
      print('ERROR (I)', e)
      continue
    for _, (cid, s3_uri) in tsv_data.iterrows():
      raw_message = create_raw_message(cid, s3_uri)
      try:
        check_message_for_size(raw_message)
      except AssertionError:
        print('Message too big.')
        continue
      try:
        py_source_queue.put(raw_message)
      except Exception as e:
        print('ERROR (II)', e)

@app.route('/', methods=['GET', 'POST'])
def put_s3_tsv_uri_on_py_source_queue():
    form = ReusableForm(request.form)
    try:
        print(form.errors)
        if request.method == 'POST':
            s3_tsv_uri = request.form['s3uri']
            success, message = S3_utils.can_get_s3_uri(s3_tsv_uri)
            if success and form.validate():
                # Let the fill_task worker begin filling py_source_queue.
                flash('Submitted ' + s3_tsv_uri + ' for processing.')
                s3_uri_queue.put(s3_tsv_uri)
            else:
                flash('Error: Please review the S3 TSV URI input.')
    except Exception as e:
        print('ERROR: put_s3_tsv_uri_on_py_source_queue\n', e)
        flash('Error: {} Please review the S3 TSV URI input.'.format(e))
    return render_template('hello.html', form=form)

# Health check route
@app.route('/health')
def ping():
  return '200 OK'

# Prepare Python queue.Queues
max_size = 0
s3_uri_queue = queue.Queue(max_size)
py_source_queue = queue.Queue(max_size)

# Prepare SQS Queues
FILL_BATCH_SIZE = int(os.getenv('FILL_BATCH_SIZE'))
sqs_dest_queue = get_sqs_dest_queue()

# Launch sender threads
num_threads = 1
for i in range(num_threads):
  t = SQSSendMessagesThread(i, py_source_queue, sqs_dest_queue, batch_size=FILL_BATCH_SIZE)
  # Begin "getting" raw messages from py_source_queue and "putting" on sqs_dest_queue in proper format.
  t.start()

# Start filling py_source_queue (worker thread blocks until user sends s3_tsv_uri)
thread = threading.Thread(target=fill_task)
thread.start()

if __name__ == '__main__':
  app.run(use_reloader=False, debug=True, host='0.0.0.0', port=5000)
  # docker run -p 80(HOST_PORT):5000(CONTAINER_PORT)  --name flask-test-container -t flask-test

