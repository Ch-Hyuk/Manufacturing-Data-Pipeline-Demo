# Manufacturing Data Pipeline Project

제조 공정의 설비 센서, 생산 실적, 품질 검사 데이터를 기반으로 Airflow, MQTT, Spark, PostgreSQL, Tableau 연계를 연습하는 데이터 엔지니어링 포트폴리오 프로젝트입니다.

## 프로젝트 목표

- 가상 설비가 실제 센서처럼 온도, 압력, 진동, 전류, RPM 데이터를 주기적으로 생성
- MQTT 프로토콜을 통해 설비 텔레메트리 데이터를 publish/subscribe 방식으로 수집
- Airflow DAG로 수집, 검증, ETL, 적재, Data Mart 생성을 자동화
- PySpark로 센서/생산/품질 데이터를 집계하고 설비 상태를 계산
- PostgreSQL에 raw 테이블과 분석용 Data Mart를 구성
- Tableau 또는 BI 도구에서 생산량, 불량률, 설비 이상 징후 KPI를 시각화

## 전체 아키텍처

```text
Virtual Machines
  -> MQTT Broker
  -> MQTT Sensor Collector
  -> Raw CSV
  -> Airflow DAG
  -> Spark ETL
  -> PostgreSQL Raw Tables
  -> PostgreSQL Data Mart
  -> Tableau / BI Dashboard
```

## 데이터 흐름

```text
machine-simulator
  -> manufacturing/F01/L01/M01/telemetry
  -> mosquitto MQTT broker
  -> collect_mqtt_sensor_data
  -> raw_sensor_data.csv

generate_raw_data
  -> raw_production_data.csv
  -> raw_quality_data.csv

Airflow
  -> validate_raw_data
  -> run_spark_etl
  -> load_to_postgresql
  -> create_data_mart
  -> check_data_quality
```

## 프로젝트 구조

```text
.
├── config/
│   └── mosquitto.conf
├── data_generation/
│   └── raw_data_generator.py
├── dags/
│   └── manufacturing_pipeline_dag.py
├── data/
│   ├── processed/
│   └── raw/
├── docker/
│   └── start_airflow.sh
├── ingestion/
│   └── mqtt_sensor_collector.py
├── quality/
│   └── raw_data_validator.py
├── simulators/
│   └── machine_simulator.py
├── spark/
│   └── etl_manufacturing.py
├── sql/
│   ├── 00_create_databases.sql
│   ├── 01_create_raw_tables.sql
│   └── 02_create_data_marts.sql
├── storage/
│   └── postgres_loader.py
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── requirements.txt
└── README.md
```

## 주요 서비스

| Service | 역할 |
| --- | --- |
| `mqtt` | Eclipse Mosquitto 기반 MQTT broker |
| `machine-simulator` | 가상 설비 센서 데이터를 MQTT로 publish |
| `postgres` | Airflow metadata DB와 제조 Data Warehouse 저장 |
| `airflow` | 데이터 파이프라인 오케스트레이션 |
| `flow-viewer` | MQTT 수집부터 Data Mart까지 흐름과 결과를 보여주는 Streamlit UI |

## 실행 방법

1. 환경 변수 파일을 준비합니다.

```powershell
Copy-Item .env.example .env -Force
```

2. 기존 컨테이너와 볼륨을 초기화합니다.

```powershell
docker compose down -v
```

3. 컨테이너를 빌드하고 실행합니다.

```powershell
docker compose up --build -d
```

4. Airflow UI에 접속합니다.

- URL: http://localhost:8080
- ID: `airflow`
- PW: `airflow`

5. Flow Viewer UI에 접속합니다.

- URL: http://localhost:8501

6. Airflow에서 `manufacturing_data_pipeline` DAG를 수동 실행한 뒤 Flow Viewer에서 각 단계 상태와 결과를 확인합니다.

## Airflow DAG Task

| Task | 설명 |
| --- | --- |
| `collect_sensor_data_from_mqtt` | MQTT broker에서 설비 센서 데이터를 60초 동안 수집해 `raw_sensor_data.csv` 생성 |
| `generate_raw_data` | 생산 실적과 품질 검사 데이터를 현실적인 분포로 생성 |
| `validate_raw_data` | 필수 컬럼, null, 중복 row 검증 |
| `run_spark_etl` | Spark로 일별 생산량, 불량률, 설비 상태 계산 |
| `load_to_postgresql` | raw CSV를 PostgreSQL raw 테이블에 적재 |
| `create_data_mart` | PostgreSQL SQL로 분석용 Data Mart 생성 |
| `check_data_quality` | `DANGER` 설비 발생 여부 확인 |
| `notify_result` | 파이프라인 완료 로그 출력 |

## Flow Viewer에서 보는 화면

`http://localhost:8501`에서 다음 탭을 확인할 수 있습니다.

| Tab | 설명 |
| --- | --- |
| `Flow` | 가상 설비, MQTT, 수집기, Airflow, Spark, PostgreSQL, Data Mart 단계별 상태 확인 |
| `Telemetry` | MQTT로 수집된 최신 센서 데이터와 설비별 센서 추이 |
| `Raw Layer` | raw CSV와 PostgreSQL raw table row count 확인 |
| `Data Mart` | 생산량, 불량률, 설비 health status KPI와 차트 확인 |

Flow Viewer의 `Flow` 탭에서는 Airflow UI에 직접 접속하지 않아도 `Run Pipeline` 버튼으로 `manufacturing_data_pipeline` DAG를 실행할 수 있습니다. Flow Viewer는 Airflow REST API를 호출하고, Airflow는 백엔드 오케스트레이션 엔진으로 MQTT 수집, raw 검증, Spark ETL, PostgreSQL 적재, Data Mart 생성을 수행합니다.

```text
Flow Viewer Run Pipeline button
  -> Airflow REST API
  -> manufacturing_data_pipeline DAG
  -> MQTT collection / ETL / Data Mart
  -> Flow Viewer result monitoring
```

## 가상 설비 On/Off 제어

Flow Viewer의 `Flow` 탭에서 `Start Publishing`, `Stop Publishing` 버튼으로 가상 설비의 MQTT publish 상태를 제어할 수 있습니다.

내부적으로는 다음 MQTT control topic을 사용합니다.

```text
manufacturing/control/simulator
```

명령 값은 다음과 같습니다.

| Command | 동작 |
| --- | --- |
| `START` | 가상 설비 센서 데이터 publish 재개 |
| `STOP` | 가상 설비 센서 데이터 publish 중지 |

시뮬레이터는 현재 상태를 retained status topic으로 publish합니다.

```text
manufacturing/status/simulator
```

## MQTT Topic

가상 설비는 다음 패턴으로 데이터를 publish합니다.

```text
manufacturing/{factory_id}/{line_id}/{machine_id}/telemetry
```

예시:

```text
manufacturing/F01/L01/M01/telemetry
```

## Sensor Payload 예시

```json
{
  "event_time": "2026-06-09T00:30:00.000000+00:00",
  "factory_id": "F01",
  "line_id": "L01",
  "machine_id": "M01",
  "product_id": "P1001",
  "mode": "RUN",
  "temperature": 73.42,
  "pressure": 4.31,
  "vibration": 0.143,
  "motor_current": 18.8,
  "rpm": 1452,
  "anomaly_type": null,
  "sequence": 124
}
```

## 주요 테이블

### Raw

- `raw_sensor_data`: MQTT로 수집한 설비 센서 데이터
- `raw_production_data`: LOT 단위 생산 실적 데이터
- `raw_quality_data`: LOT 단위 품질 검사 결과 데이터

### Data Mart

- `dm_daily_production`: 일자, 설비, 제품별 생산량
- `dm_daily_quality`: 일자, 설비별 LOT 수와 불량률
- `dm_machine_health`: 일자, 설비별 평균 센서 상태와 health status

## 포트폴리오 설명 포인트

이 프로젝트는 단순히 CSV를 생성하는 배치 프로젝트가 아니라, 가상 설비가 MQTT로 송신하는 센서 데이터를 수집한 뒤 Airflow로 전체 파이프라인을 운영하는 구조입니다. 따라서 제조 현장의 IoT 데이터 수집, 배치 오케스트레이션, Spark ETL, PostgreSQL Data Mart 설계까지 하나의 흐름으로 설명할 수 있습니다.
