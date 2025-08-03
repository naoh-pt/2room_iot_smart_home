import paho.mqtt.client as mqtt
import mysql.connector
from flask import Flask, render_template, jsonify, request, redirect, url_for
import json
import time
from datetime import datetime
import threading

app = Flask(__name__)

# Cấu hình MQTT (HiveMQ Cloud)
MQTT_BROKER = "4a97043579714e55aba4f9b977919abb.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "admin-1"
MQTT_PASSWORD = "1n1n1n1N!"
MQTT_TOPIC = "sensor/esp32/data"
CONTROL_TOPIC_PREFIX = "sensor/esp32/set"

# Cấu hình MariaDB
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="iot_db_2room"
)
cursor = db.cursor()

# Tạo bảng data và state nếu chưa có
cursor.execute("""
    CREATE TABLE IF NOT EXISTS data (
        datetime DATETIME,
        room VARCHAR(50),
        temperature FLOAT,
        humidity FLOAT,
        PRIMARY KEY (datetime, room)
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS state (
        datetime DATETIME,
        room VARCHAR(50),
        devices VARCHAR(50),
        state VARCHAR(10),
        PRIMARY KEY (datetime, room, devices)
    )
""")
db.commit()

# Biến toàn cục để lưu trạng thái thiết bị và dữ liệu mới nhất từ MQTT cho từng phòng
device_states = {
    "phong_ngu": {"light": "off", "fan": "off"},
    "room2": {"light": "off", "fan": "off"}
}
latest_data = {
    "phong_ngu": {"temperature": None, "humidity": None},
    "room2": {"temperature": None, "humidity": None}
}

# Hàm callback MQTT
def on_message(client, userdata, message):
    payload = json.loads(message.payload.decode())
    room = payload.get("room")
    light = payload.get("light_status")
    fan = payload.get("fan_status")
    temp = payload.get("temperature")
    hum = payload.get("humidity")
    if room in ["phong_ngu", "room2"] and temp is not None and hum is not None:
        latest_data[room]["temperature"] = float(temp)
        latest_data[room]["humidity"] = float(hum)
        device_states[room]["light"] = "on" if int(light) == 1 else "off"
        device_states[room]["fan"] = "on" if int(fan) == 1 else "off"

# Khởi tạo MQTT client
client = mqtt.Client()
client.tls_set()
client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
client.on_message = on_message
client.connect(MQTT_BROKER, MQTT_PORT)
client.subscribe(MQTT_TOPIC)
client.loop_start()

# Hàm lưu dữ liệu từ MQTT vào database mỗi 1 phút cho từng phòng
def save_data_periodically():
    while True:
        time.sleep(60)
        for room in ["phong_ngu", "room2"]:
            if latest_data[room]["temperature"] is not None and latest_data[room]["humidity"] is not None:
                now = datetime.now()
                cursor.execute("INSERT INTO data (datetime, room, temperature, humidity) VALUES (%s, %s, %s, %s)", 
                              (now, room, latest_data[room]["temperature"], latest_data[room]["humidity"]))
                db.commit()
                print(f"Saved at {now} for {room}: temp={latest_data[room]['temperature']}, hum={latest_data[room]['humidity']}")

# Khởi động thread lưu dữ liệu
threading.Thread(target=save_data_periodically, daemon=True).start()

# Hàm cập nhật dữ liệu từ DB (5 bản ghi mới nhất cho phòng cụ thể)
def get_latest_data(room):
    cursor.execute("SELECT * FROM data WHERE room = %s ORDER BY datetime DESC LIMIT 5", (room,))
    return cursor.fetchall()

def get_latest_state(room):
    cursor.execute("SELECT * FROM state WHERE room = %s ORDER BY datetime DESC LIMIT 5", (room,))
    return cursor.fetchall()

# Route cho trang chọn phòng
@app.route('/')
def select_room():
    return render_template('select_room.html')

# Route cho trang điều khiển phòng cụ thể
@app.route('/room/<room>')
def room_control(room):
    # Kiểm tra phòng hợp lệ
    valid_rooms = ["phong_ngu", "room2"]
    if room not in valid_rooms:
        # Trả về JSON báo lỗi
        return jsonify({"error": "Room not found"}), 404

    # Lấy dữ liệu
    data = get_latest_data(room)
    states = get_latest_state(room)

    # Đóng gói thành dict
    result = {
        "room": room,
        "data": data,
        "states": states,
        "device_states": device_states[room],
        "live_temp": latest_data[room]["temperature"],
        "live_hum": latest_data[room]["humidity"]
    }

    # Trả JSON
    return jsonify(result)

# API để cập nhật trạng thái thiết bị qua MQTT cho phòng cụ thể
@app.route('/toggle/<room>/<device>', methods=['POST'])
def toggle_device(room, device):
    if room in ["phong_ngu", "room2"] and device in ["light", "fan"]:
        new_state = "on" if device_states[room][device] == "off" else "off"
        device_states[room][device] = new_state
        now = datetime.now()
        cursor.execute("INSERT INTO state (datetime, room, devices, state) VALUES (%s, %s, %s, %s)", (now, room, device, new_state))
        db.commit()
        # Gửi lệnh qua MQTT tới topic cụ thể cho phòng và thiết bị
        control_topic = CONTROL_TOPIC_PREFIX
        light_state_value = 1 if device_states[room]["light"] == "on" else 0
        fan_state_value = 1 if device_states[room]["fan"] == "on" else 0
        payload = json.dumps({
            "room": room,
            "light": light_state_value,
            "fan": fan_state_value
        })
        client.publish(control_topic, payload)
    return jsonify({"status": "success", "room": room, "device": device, "state": new_state})

# API tìm kiếm theo datetime cho phòng cụ thể
@app.route('/search/<room>/<datetime_str>')
def search_data(room, datetime_str):
    cursor.execute("SELECT * FROM data WHERE room = %s AND datetime LIKE %s", (room, f"%{datetime_str}%"))
    return jsonify(cursor.fetchall())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)