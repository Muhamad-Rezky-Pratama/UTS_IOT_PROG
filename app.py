from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import paho.mqtt.client as mqtt
import mysql.connector
import json
import statistics
from datetime import datetime

# ==============================
# 1️⃣ Inisialisasi Flask
# ==============================
app = Flask(__name__)
CORS(app)

# ==============================
# 2️⃣ Database Configuration
# ==============================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "sensor_db"
}

def get_db_connection():
    """Membuat koneksi baru ke database."""
    return mysql.connector.connect(**DB_CONFIG)

# ==============================
# 3️⃣ Variabel global untuk data terakhir & MQTT
# ==============================
sensor_data = {
    "suhu": None,
    "humidity": None,
    "lux": None,
    "relay_state": "OFF" # Default state
}

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883

# ==============================
# 4️⃣ Setup Database (Pastikan tabel ada)
# ==============================
try:
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS data_sensor (
        id INT AUTO_INCREMENT PRIMARY KEY,
        suhu FLOAT,
        humidity FLOAT,
        lux FLOAT,
        relay_state VARCHAR(10),
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    db.commit()
    cursor.close()
    db.close()
    print("✅ Database dan Tabel Siap.")
except Exception as e:
    print(f"❌ Error saat inisialisasi database: {e}")

# ==============================
# 5️⃣ Fungsi callback MQTT
# ==============================
def on_connect(client, userdata, flags, reason_code, properties):
    print("Terhubung ke MQTT Broker dengan kode:", reason_code)
    client.subscribe("esp32/rezky/data") # subscribe topic sensor

def on_message(client, userdata, msg):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        payload = msg.payload.decode()
        data = json.loads(payload)

        # Update global state
        sensor_data["suhu"] = float(data.get("suhu", 0))
        sensor_data["humidity"] = float(data.get("humidity", 0))
        sensor_data["lux"] = float(data.get("lux", 0))
        # Mengambil state dalam format string ("ON" atau "OFF")
        sensor_data["relay_state"] = data.get("relay_state", "OFF") 

        sql = """
            INSERT INTO data_sensor (suhu, humidity, lux, relay_state)
            VALUES (%s, %s, %s, %s)
        """
        val = (
            sensor_data["suhu"],
            sensor_data["humidity"],
            sensor_data["lux"],
            sensor_data["relay_state"]
        )
        cursor.execute(sql, val)
        conn.commit()
        print("✅ Data disimpan:", val)

    except Exception as e:
        print("❌ Error parsing/saving message:", e)
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ==============================
# 6️⃣ Setup MQTT Client
# ==============================
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_start()

# ==============================
# 7️⃣ Route Flask
# ==============================
@app.route("/")
def home():
    # Asumsi file index.html berada di folder 'templates'
    return render_template("index.html") 

@app.route("/api/sensor_data", methods=["GET"])
def get_sensor_data():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        # Ambil 50 data terakhir untuk statistik yang lebih baik
        cur.execute("SELECT * FROM data_sensor ORDER BY timestamp DESC LIMIT 50") 
        rows = cur.fetchall()

        if not rows:
            # Tetap kirim status relay terakhir meskipun tidak ada data sensor
            return jsonify({
                "message": "Belum ada data sensor", 
                "relay_state": sensor_data["relay_state"]
            }), 200

        # Statistik
        suhu_values = [r["suhu"] for r in rows]
        hum_values = [r["humidity"] for r in rows]
        lux_values = [r["lux"] for r in rows]
        
        # Contoh Agregasi Sederhana: Suhu Maks per Bulan/Tahun (memerlukan kolom timestamp)
        # Ini hanya contoh, logika agregasi yang kompleks memerlukan query SQL atau Pandas
        month_year_max = {}
        for r in rows:
             # Konversi objek datetime ke string
            if isinstance(r['timestamp'], datetime):
                dt_str = r['timestamp'].strftime("%Y-%m") 
            else:
                # Jika timestamp sudah string/format lain
                dt_str = str(r['timestamp'])[:7] 
                
            if dt_str not in month_year_max or r['suhu'] > month_year_max[dt_str]:
                month_year_max[dt_str] = r['suhu']
        
        month_year_list = [{"month_year": k, "max_suhu": v} for k, v in month_year_max.items()]

        result = {
            "luxmax": round(max(lux_values), 2),
            "luxmin": round(min(lux_values), 2),
            "luxrata": round(statistics.mean(lux_values), 2),
            "suhumax": round(max(suhu_values), 2),
            "suhumin": round(min(suhu_values), 2),
            "suhurata": round(statistics.mean(suhu_values), 2),
            "humiditymax": round(max(hum_values), 2),
            "humiditymin": round(min(hum_values), 2),
            "humidityrata": round(statistics.mean(hum_values), 2),
            "relay_state": sensor_data["relay_state"], # Kirim status relay saat ini
            "month_year_max": month_year_list,
            "records": rows
        }

        return jsonify(result)
    except Exception as e:
        print("Error mengambil data:", e)
        return jsonify({"error": str(e)}), 500
    finally:
        if conn and conn.is_connected():
            conn.close()

@app.route("/relay", methods=["POST"])
def control_relay():
    try:
        data = request.get_json()
        state = data.get("state") # Mengambil state "ON" atau "OFF"

        if state not in ["ON", "OFF"]:
            return jsonify({"error": "State harus 'ON' atau 'OFF'"}), 400

        # ✅ Publish perintah ke MQTT dengan kunci "state"
        # Payload yang dikirim: {"state": "ON"} atau {"state": "OFF"}
        mqtt_client.publish("esp32/rezky/relay", json.dumps({"state": state})) 
        print(f"Perintah relay dikirim ke MQTT: {json.dumps({'state': state})}")

        # Update status terakhir
        sensor_data["relay_state"] = state
        return jsonify({"status": f"Relay {state}", "relay_state": state})
    except Exception as e:
        print("Error mengirim relay:", e)
        return jsonify({"error": str(e)}), 500

# ==============================
# 8️⃣ Jalankan Flask
# ==============================
if __name__ == "__main__":
    print("Menjalankan Flask Server di http://127.0.0.1:5000")
    # Menonaktifkan debug untuk produksi, tetapi tetap berguna untuk pengembangan
    app.run(host="0.0.0.0", port=5000, debug=True)