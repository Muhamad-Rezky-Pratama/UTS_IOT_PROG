#include <DHT.h>

#define DHTPIN 8
#define DHTTYPE DHT22
DHT dht(DHTPIN, DHTTYPE);

#define LED_HIJAU 5
#define LED_KUNING 10
#define LED_MERAH 12
#define RELAY_POMPA 7
#define BUZZER 9

void setup() {
  Serial.begin(9600);
  dht.begin();

  pinMode(LED_HIJAU, OUTPUT);
  pinMode(LED_KUNING, OUTPUT);
  pinMode(LED_MERAH, OUTPUT);
  pinMode(RELAY_POMPA, OUTPUT);
  pinMode(BUZZER, OUTPUT);
  
  digitalWrite(LED_HIJAU, LOW);
  digitalWrite(LED_KUNING, LOW);
  digitalWrite(LED_MERAH, LOW);
  digitalWrite(RELAY_POMPA, LOW);
  digitalWrite(BUZZER, LOW);
}

void loop() {
  float suhu = dht.readTemperature();
  float hum = dht.readHumidity();

  if (isnan(suhu) || isnan(hum)) {
    Serial.println("Gagal membaca DHT!");
    delay(2000);
    return;
  }

  Serial.print("Suhu: "); Serial.print(suhu);
  Serial.print(" °C | Kelembapan: "); Serial.print(hum);
  Serial.println(" %");

  if (suhu > 35) {
    digitalWrite(LED_MERAH, HIGH);
    digitalWrite(LED_KUNING, LOW);
    digitalWrite(LED_HIJAU, LOW);
    digitalWrite(BUZZER, HIGH);
    digitalWrite(RELAY_POMPA, HIGH);
  }
  else if (suhu >= 30 && suhu <= 35) {
    digitalWrite(LED_KUNING, HIGH);
    digitalWrite(LED_MERAH, LOW);
    digitalWrite(LED_HIJAU, LOW);
    digitalWrite(BUZZER, LOW);
    digitalWrite(RELAY_POMPA, LOW);
  }
  else {
    digitalWrite(LED_HIJAU, HIGH);
    digitalWrite(LED_KUNING, LOW);
    digitalWrite(LED_MERAH, LOW);
    digitalWrite(BUZZER, LOW);
    digitalWrite(RELAY_POMPA, LOW);
  }

  delay(3000);
}
