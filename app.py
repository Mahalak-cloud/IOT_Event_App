from flask import Flask, request, jsonify
from flask_cors import CORS
from models import Event
from services import EventService
from tasks import process_event
from datetime import datetime
from redis import Redis
from rq import Queue

app = Flask(__name__)
CORS(app)

# Redis Queue
redis_conn = Redis(host='localhost', port=6379)
queue = Queue(connection=redis_conn)

event_service = EventService()

@app.route('/')
def display():
    return "Hello, welcome to the Python server"

@app.route('/api/events', methods=['POST'])
def receive_event():
    data = request.get_json()
    job = queue.enqueue(process_event, data)
    return jsonify({"message": "Event received, processing started", "job_id": job.id}), 202

@app.route('/api/events/query', methods=['GET'])
def query_events():
    device_id = request.args.get('device_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    events = event_service.query_events(device_id, start_date, end_date)
    return jsonify([event.to_dict() for event in events])

@app.route('/api/events/summary', methods=['GET'])
def summary_report():
    device_id = request.args.get('device_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    summary = event_service.summary_report(device_id, start_date, end_date)
    return jsonify(summary)

if __name__ == '__main__':
    app.run(debug=True, threaded=True)
