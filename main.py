from flask import Flask, jsonify, send_file, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd


app = Flask(__name__)

username = 'root'
password = 'root123'
host = 'localhost'
database = 'loop_store_monitoring'

engine = create_engine (f"mysql+mysqlconnector://{username}:{password}@{host}/{database}")

app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://username:password@localhost/database'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

class StoreStatus(db.Model):
    __tablename__ = 'store_status'
    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.String(80), nullable=False)
    timestamp_utc = db.Column(db.DateTime, nullable=False)
    status = db.Column(db.String(80), nullable=False)

class StoreHours(db.Model):
    __tablename__ = 'store_hours'
    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.String(80), nullable=False)
    dayOfWeek = db.Column(db.Integer, nullable=False)
    start_time_local = db.Column(db.Time, nullable=False)
    end_time_local = db.Column(db.Time, nullable=False)

class StoreTimezone(db.Model):
    __tablename__ = 'store_timezone'
    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.String(80), nullable=False)
    timezone_str = db.Column(db.String(80), nullable=False)


def load_data():
    # Load Store Status Data
    store_status_df = pd.read_csv('/Users/surya/Downloads/store_status.csv')
    for idx, row in store_status_df.iterrows():
        timestamp_str = row['timestamp_utc']

        try:
            timestamp_utc = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f UTC')
        except ValueError:
            timestamp_utc = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S UTC')

        db.session.add(StoreStatus(
            store_id=row['store_id'],
            timestamp_utc=timestamp_utc,
            status=row['status']
        ))


    store_hours_df = pd.read_csv('/Users/surya/Downloads/menu_hours.csv')
    for idx, row in store_hours_df.iterrows():
        # Adjust the format string for parsing time with seconds
        db.session.add(StoreHours(
            store_id=row['store_id'],
            dayOfWeek=row['day'],
            start_time_local=datetime.strptime(row['start_time_local'], '%H:%M:%S').time(),
            end_time_local=datetime.strptime(row['end_time_local'], '%H:%M:%S').time()
        ))


    store_timezone_df = pd.read_csv('/Users/surya/Downloads/timezone.csv')
    for idx, row in store_timezone_df.iterrows():
        db.session.add(StoreTimezone(
            store_id=row['store_id'],
            timezone_str=row['timezone_str']
        ))
    
    db.session.commit()
    



import pytz

def convert_local_to_utc(day_of_week, local_time, timezone_str):
    day_mapping = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 'Friday': 4, 'Saturday': 5, 'Sunday': 6}
    day_index = day_mapping.get(day_of_week, 0)

    local_datetime = datetime.combine(datetime.utcnow().date(), local_time) + timedelta(days=day_index)

    timezone_obj = pytz.timezone(timezone_str)
    local_datetime = timezone_obj.localize(local_datetime)
    utc_datetime = local_datetime.astimezone(pytz.utc)

    return utc_datetime

def calculate_overlap(start1, end1, start2, end2):
    overlap_start = max(start1, start2)
    overlap_end = min(end1, end2)
    return max(0, (overlap_end - overlap_start).total_seconds() / 3600)  # Convert seconds to hours

def extrapolate_to_business_hours(uptime, downtime, store_hours, timezone_data):
    extrapolated_data = []
    
    for store_id, day_of_week, start_time_local, end_time_local in store_hours:
        timezone_str = timezone_data[0].timezone_str
        start_datetime_utc = convert_local_to_utc(day_of_week, start_time_local, timezone_str)
        end_datetime_utc = convert_local_to_utc(day_of_week, end_time_local, timezone_str)

        total_business_hours = (end_datetime_utc - start_datetime_utc).total_seconds() / 3600

        extrapolated_data.append({
            'store_id': store_id,
            'day_of_week': day_of_week,
            'start_time_local': start_time_local.strftime('%H:%M:%S'),
            'end_time_local': end_time_local.strftime('%H:%M:%S'),
            'uptime': (uptime / total_business_hours) * 24,
            'downtime': (downtime / total_business_hours) * 24,
        })

    return extrapolated_data

@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    with app.app_context():
        store_status_data = StoreStatus.query.all()
        store_hours_data = db.session.query(StoreHours.store_id, StoreHours.dayOfWeek, StoreHours.start_time_local, StoreHours.end_time_local).all()
        store_timezone_data = StoreTimezone.query.all()
        current_time = datetime.utcnow()
        one_hour_ago = current_time - timedelta(hours=1)
        one_day_ago = current_time - timedelta(days=1)
        one_week_ago = current_time - timedelta(weeks=1)

        def calculate_uptime_downtime(statuses, start, end):
            relevant_statuses = [status for status in statuses if start <= status.timestamp_utc <= end]
            total_time = (end - start).total_seconds() / 3600  # Convert seconds to hours

            uptime = sum(
                calculate_overlap(status.timestamp_utc, status.timestamp_utc, start, end) for status in
                relevant_statuses if status.status == 'active')
            downtime = total_time - uptime

            return uptime, downtime

        uptime_last_hour, downtime_last_hour = calculate_uptime_downtime(store_status_data, one_hour_ago, current_time)
        uptime_last_day, downtime_last_day = calculate_uptime_downtime(store_status_data, one_day_ago, current_time)
        uptime_last_week, downtime_last_week = calculate_uptime_downtime(store_status_data, one_week_ago, current_time)

        extrapolated_data = extrapolate_to_business_hours(uptime_last_week, downtime_last_week, store_hours_data, store_timezone_data)

        report_df = pd.DataFrame(extrapolated_data)
        report_path = '/Users/surya/Downloads/notion/generated_report.csv'
        report_df.to_csv(report_path, index=False)

        return jsonify({'report_id': report_path}), 202

@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')
    if not report_id:
        return jsonify({"error": "Report ID is required"}), 400
    try:
        return send_file(report_id, as_attachment=True)
    except FileNotFoundError:
        return jsonify({"error": "Report not found"}), 404


if __name__ == '__main__':
    app.run(debug=True)