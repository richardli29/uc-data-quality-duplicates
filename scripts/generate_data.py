#!/usr/bin/env python3
"""Generate test data for the UC Data Quality Explorer via Databricks SQL.

Usage:
    python scripts/generate_data.py --catalog <CATALOG> --warehouse <WAREHOUSE_ID> [--profile <CLI_PROFILE>]

Example:
    python scripts/generate_data.py \\
        --catalog my_catalog \\
        --warehouse abc123def456 \\
        --profile my-workspace

Creates 20 tables across 5 schemas (bronze, silver, gold, team_analytics,
team_reporting) with deliberate duplicates, applies table comments, and
grants permissions to workspace groups (data_engineers, data_analysts,
reporting_team).  Requires the Databricks CLI to be installed and
authenticated.
"""

import argparse
import json
import subprocess
import sys


def parse_args():
    p = argparse.ArgumentParser(description="Generate test data for UC Data Quality Explorer")
    p.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    p.add_argument("--warehouse", required=True, help="SQL warehouse ID")
    p.add_argument("--profile", default=None, help="Databricks CLI profile (optional)")
    return p.parse_args()


args = parse_args()
CATALOG = args.catalog
WAREHOUSE_ID = args.warehouse
PROFILE = args.profile


def run_sql(desc: str, sql: str):
    print(f"  -> {desc}")
    payload = json.dumps({
        "statement": sql.strip(),
        "warehouse_id": WAREHOUSE_ID,
        "wait_timeout": "50s",
    })
    cmd = ["databricks", "api", "post", "/api/2.0/sql/statements/", "--json", payload]
    if PROFILE:
        cmd += ["--profile", PROFILE]
    result = subprocess.run(cmd, capture_output=True, text=True)
    try:
        data = json.loads(result.stdout)
        state = data.get("status", {}).get("state", "UNKNOWN")
        if state != "SUCCEEDED":
            err = data.get("status", {}).get("error", {}).get("message", "")
            print(f"    FAILED: {state} - {err[:200]}")
            return False
        return True
    except json.JSONDecodeError:
        print(f"    ERROR parsing response: {result.stderr[:200]}")
        return False


print(f"=== Target: {CATALOG} (warehouse {WAREHOUSE_ID}) ===")
print("=== Creating schemas ===")
for schema in ["bronze", "silver", "gold", "team_analytics", "team_reporting"]:
    run_sql(f"Schema {schema}", f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# ============================================================
# SILVER
# ============================================================
print("\n=== SILVER ===")

run_sql("silver.students (5000 rows)", f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.students AS
WITH names AS (
  SELECT id,
    CASE abs(hash(id)) % 14
      WHEN 0 THEN 'James' WHEN 1 THEN 'Emma' WHEN 2 THEN 'Oliver' WHEN 3 THEN 'Amelia'
      WHEN 4 THEN 'Harry' WHEN 5 THEN 'Isla' WHEN 6 THEN 'George' WHEN 7 THEN 'Sophie'
      WHEN 8 THEN 'Jack' WHEN 9 THEN 'Emily' WHEN 10 THEN 'Charlie' WHEN 11 THEN 'Mia'
      WHEN 12 THEN 'Thomas' ELSE 'Grace' END as first_name,
    CASE abs(hash(id * 31)) % 12
      WHEN 0 THEN 'Smith' WHEN 1 THEN 'Jones' WHEN 2 THEN 'Williams' WHEN 3 THEN 'Taylor'
      WHEN 4 THEN 'Brown' WHEN 5 THEN 'Davies' WHEN 6 THEN 'Wilson' WHEN 7 THEN 'Evans'
      WHEN 8 THEN 'Thomas' WHEN 9 THEN 'Roberts' WHEN 10 THEN 'Johnson' ELSE 'Patel' END as last_name
  FROM range(1, 5001) t(id)
)
SELECT
  10000 + id as student_id,
  first_name,
  last_name,
  date_add('2005-01-01', abs(hash(id * 7)) % 2920) as date_of_birth,
  CASE abs(hash(id * 13)) % 25 WHEN 0 THEN 'Other' ELSE (CASE WHEN abs(hash(id * 13)) % 2 = 0 THEN 'Male' ELSE 'Female' END) END as gender,
  concat(lower(first_name), '.', lower(last_name), cast(id % 1000 as string), '@school.edu') as email,
  concat('07', lpad(cast(abs(hash(id * 17)) % 900000000 + 100000000 as string), 9, '0')) as phone,
  concat(cast(abs(hash(id * 19)) % 200 as string), ' ',
    CASE abs(hash(id * 23)) % 5 WHEN 0 THEN 'High Street' WHEN 1 THEN 'Church Road'
      WHEN 2 THEN 'Station Lane' WHEN 3 THEN 'Park Avenue' ELSE 'Mill Road' END) as address,
  CASE abs(hash(id * 29)) % 9 WHEN 0 THEN 'London' WHEN 1 THEN 'Birmingham' WHEN 2 THEN 'Manchester'
    WHEN 3 THEN 'Leeds' WHEN 4 THEN 'Bristol' WHEN 5 THEN 'Sheffield'
    WHEN 6 THEN 'Liverpool' WHEN 7 THEN 'Newcastle' ELSE 'Nottingham' END as city,
  concat(CASE abs(hash(id * 37)) % 4 WHEN 0 THEN 'SW' WHEN 1 THEN 'EC' WHEN 2 THEN 'NW' ELSE 'SE' END,
    cast(abs(hash(id * 41)) % 20 as string), ' ',
    cast(abs(hash(id * 43)) % 9 as string),
    CASE abs(hash(id * 47)) % 3 WHEN 0 THEN 'AB' WHEN 1 THEN 'CD' ELSE 'EF' END) as postcode,
  cast(1 + abs(hash(id * 53)) % 500 as int) as school_id,
  cast(7 + abs(hash(id * 59)) % 7 as int) as year_group,
  date_add('2018-09-01', abs(hash(id * 61)) % 2190) as enrollment_date,
  CASE WHEN abs(hash(id * 67)) % 100 < 15 THEN true ELSE false END as is_sen,
  CASE WHEN abs(hash(id * 71)) % 100 < 23 THEN true ELSE false END as fsm_eligible
FROM names
""")

run_sql("silver.schools (500 rows)", f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.schools AS
SELECT
  id as school_id,
  concat(
    CASE abs(hash(id * 3)) % 8 WHEN 0 THEN 'St Marys' WHEN 1 THEN 'Kings' WHEN 2 THEN 'Oakwood'
      WHEN 3 THEN 'Riverside' WHEN 4 THEN 'Greenfield' WHEN 5 THEN 'Parklands'
      WHEN 6 THEN 'Hillcrest' ELSE 'Westgate' END, ' ',
    CASE abs(hash(id * 5)) % 4 WHEN 0 THEN 'Academy' WHEN 1 THEN 'School'
      WHEN 2 THEN 'College' ELSE 'High School' END) as school_name,
  CASE abs(hash(id * 7)) % 10
    WHEN 0 THEN 'Academy' WHEN 1 THEN 'Academy' WHEN 2 THEN 'Academy' WHEN 3 THEN 'Academy'
    WHEN 4 THEN 'Community School' WHEN 5 THEN 'Community School' WHEN 6 THEN 'Free School'
    WHEN 7 THEN 'Free School' WHEN 8 THEN 'Grammar School' ELSE 'Faith School' END as school_type,
  CASE abs(hash(id * 11)) % 9 WHEN 0 THEN 'London' WHEN 1 THEN 'Birmingham' WHEN 2 THEN 'Manchester'
    WHEN 3 THEN 'Leeds' WHEN 4 THEN 'Bristol' WHEN 5 THEN 'Sheffield'
    WHEN 6 THEN 'Liverpool' WHEN 7 THEN 'Newcastle' ELSE 'Nottingham' END as local_authority,
  CASE abs(hash(id * 13)) % 9 WHEN 0 THEN 'London' WHEN 1 THEN 'West Midlands' WHEN 2 THEN 'North West'
    WHEN 3 THEN 'Yorkshire' WHEN 4 THEN 'South West' WHEN 5 THEN 'Yorkshire'
    WHEN 6 THEN 'North West' WHEN 7 THEN 'North East' ELSE 'East Midlands' END as region,
  concat(CASE abs(hash(id * 17)) % 4 WHEN 0 THEN 'SW' WHEN 1 THEN 'EC' WHEN 2 THEN 'NW' ELSE 'SE' END,
    cast(abs(hash(id * 19)) % 20 as string), ' ', cast(abs(hash(id * 23)) % 9 as string), 'XY') as postcode,
  CASE abs(hash(id * 29)) % 20
    WHEN 0 THEN 'Outstanding' WHEN 1 THEN 'Outstanding' WHEN 2 THEN 'Outstanding' WHEN 3 THEN 'Outstanding'
    WHEN 16 THEN 'Requires Improvement' WHEN 17 THEN 'Requires Improvement'
    WHEN 18 THEN 'Requires Improvement' WHEN 19 THEN 'Inadequate'
    ELSE 'Good' END as ofsted_rating,
  cast(200 + abs(hash(id * 31)) % 2300 as int) as num_pupils,
  CASE abs(hash(id * 37)) % 20
    WHEN 0 THEN 'All-through' WHEN 1 THEN 'All-through' WHEN 2 THEN 'All-through'
    WHEN 3 THEN 'Primary' WHEN 4 THEN 'Primary' WHEN 5 THEN 'Primary' WHEN 6 THEN 'Primary'
    WHEN 7 THEN 'Primary' WHEN 8 THEN 'Primary' WHEN 9 THEN 'Primary'
    ELSE 'Secondary' END as phase,
  concat(
    CASE abs(hash(id * 41)) % 5 WHEN 0 THEN 'Dr' WHEN 1 THEN 'Mr' WHEN 2 THEN 'Mrs'
      WHEN 3 THEN 'Ms' ELSE 'Prof' END, ' ',
    CASE abs(hash(id * 43)) % 4 WHEN 0 THEN 'James' WHEN 1 THEN 'Sarah' WHEN 2 THEN 'David' ELSE 'Helen' END, ' ',
    CASE abs(hash(id * 47)) % 4 WHEN 0 THEN 'Smith' WHEN 1 THEN 'Patel' WHEN 2 THEN 'Khan' ELSE 'Williams' END) as headteacher,
  concat('head@school', cast(id as string), '.edu') as contact_email,
  concat('020 ', lpad(cast(abs(hash(id * 53)) % 9000000 + 1000000 as string), 7, '0')) as phone_number,
  concat('https://www.school', cast(id as string), '.edu') as website
FROM range(1, 501) t(id)
""")

run_sql("silver.exam_results (50000 rows)", f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.exam_results AS
SELECT
  id as result_id,
  cast(10001 + abs(hash(id * 3)) % 5000 as int) as student_id,
  cast(1 + abs(hash(id * 5)) % 500 as int) as school_id,
  CASE abs(hash(id * 7)) % 10 WHEN 0 THEN 'Mathematics' WHEN 1 THEN 'English' WHEN 2 THEN 'Science'
    WHEN 3 THEN 'History' WHEN 4 THEN 'Geography' WHEN 5 THEN 'Art'
    WHEN 6 THEN 'Music' WHEN 7 THEN 'PE' WHEN 8 THEN 'Computing'
    ELSE 'Modern Languages' END as subject,
  date_add('2023-05-01', abs(hash(id * 11)) % 60) as exam_date,
  cast(greatest(0, least(100, 50 + abs(hash(id * 13)) % 51)) as int) as score,
  CASE abs(hash(id * 17)) % 20
    WHEN 0 THEN 'A*' WHEN 1 THEN 'A' WHEN 2 THEN 'A' WHEN 3 THEN 'B' WHEN 4 THEN 'B'
    WHEN 5 THEN 'B' WHEN 6 THEN 'B' WHEN 7 THEN 'C' WHEN 8 THEN 'C' WHEN 9 THEN 'C'
    WHEN 10 THEN 'C' WHEN 11 THEN 'C' WHEN 12 THEN 'D' WHEN 13 THEN 'D' WHEN 14 THEN 'D'
    WHEN 15 THEN 'E' WHEN 16 THEN 'E' WHEN 17 THEN 'E' WHEN 18 THEN 'U'
    ELSE 'C' END as grade,
  CASE abs(hash(id * 19)) % 20
    WHEN 0 THEN 'AQA' WHEN 1 THEN 'AQA' WHEN 2 THEN 'AQA' WHEN 3 THEN 'AQA'
    WHEN 4 THEN 'AQA' WHEN 5 THEN 'AQA' WHEN 6 THEN 'AQA'
    WHEN 7 THEN 'Edexcel' WHEN 8 THEN 'Edexcel' WHEN 9 THEN 'Edexcel'
    WHEN 10 THEN 'Edexcel' WHEN 11 THEN 'Edexcel' WHEN 12 THEN 'Edexcel'
    WHEN 13 THEN 'OCR' WHEN 14 THEN 'OCR' WHEN 15 THEN 'OCR' WHEN 16 THEN 'OCR' WHEN 17 THEN 'OCR'
    ELSE 'WJEC' END as exam_board,
  CASE WHEN abs(hash(id * 23)) % 100 < 45 THEN '2022/23' ELSE '2023/24' END as academic_year
FROM range(1, 50001) t(id)
""")

run_sql("silver.attendance (100000 rows)", f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.attendance AS
SELECT
  id as attendance_id,
  cast(10001 + abs(hash(id * 3)) % 5000 as int) as student_id,
  cast(1 + abs(hash(id * 5)) % 500 as int) as school_id,
  date_add('2023-09-01', abs(hash(id * 7)) % 200) as date,
  CASE abs(hash(id * 11)) % 20
    WHEN 0 THEN 'Authorised Absence' WHEN 1 THEN 'Authorised Absence'
    WHEN 2 THEN 'Unauthorised Absence' WHEN 3 THEN 'Late'
    ELSE 'Present' END as status,
  CASE WHEN abs(hash(id * 13)) % 2 = 0 THEN 'AM' ELSE 'PM' END as session,
  '2023/24' as academic_year,
  CASE abs(hash(id * 17)) % 3 WHEN 0 THEN 'Autumn' WHEN 1 THEN 'Spring' ELSE 'Summer' END as term
FROM range(1, 100001) t(id)
""")

# ============================================================
# BRONZE
# ============================================================
print("\n=== BRONZE ===")

run_sql("bronze.raw_students", f"""
CREATE OR REPLACE TABLE {CATALOG}.bronze.raw_students AS
SELECT student_id as STUDENT_ID, first_name as FirstName, last_name as LastName,
  date_of_birth as DOB, gender, email as Email_Address, phone, address, city, postcode,
  school_id, year_group, enrollment_date, is_sen, fsm_eligible,
  'raw_import_2024' as _source_file, '2024-01-15T10:30:00' as _ingestion_ts
FROM {CATALOG}.silver.students
""")

run_sql("bronze.raw_schools", f"""
CREATE OR REPLACE TABLE {CATALOG}.bronze.raw_schools AS
SELECT school_id as SchoolID, school_name as SchoolName, school_type, local_authority, region,
  postcode, ofsted_rating, num_pupils, phase, headteacher, contact_email, phone_number, website,
  'gias_extract' as _source
FROM {CATALOG}.silver.schools
""")

run_sql("bronze.raw_exam_results", f"""
CREATE OR REPLACE TABLE {CATALOG}.bronze.raw_exam_results AS
SELECT result_id as ResultID, student_id as StudentID, school_id, subject, exam_date,
  score as RawScore, grade, exam_board, academic_year, 'exam_board_feed' as _data_source
FROM {CATALOG}.silver.exam_results
""")

run_sql("bronze.raw_attendance", f"""
CREATE OR REPLACE TABLE {CATALOG}.bronze.raw_attendance AS
SELECT attendance_id as RecordID, student_id as StudentID, school_id, date,
  status as AttendanceCode, session, academic_year, term, 'mis_export' as _source_system
FROM {CATALOG}.silver.attendance
""")

# ============================================================
# GOLD
# ============================================================
print("\n=== GOLD ===")

run_sql("gold.dim_students", f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.dim_students AS
SELECT *, true as is_current, 1 as scd_version, enrollment_date as valid_from,
  cast(null as date) as valid_to
FROM {CATALOG}.silver.students
""")

run_sql("gold.dim_schools", f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.dim_schools AS
SELECT *, true as is_current, 1 as scd_version
FROM {CATALOG}.silver.schools
""")

run_sql("gold.fact_exam_results", f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.fact_exam_results AS
SELECT *, cast(score as double) as score_pct,
  CASE WHEN grade IN ('A*','A','B','C') THEN true ELSE false END as is_pass
FROM {CATALOG}.silver.exam_results
""")

run_sql("gold.fact_attendance_agg", f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.fact_attendance_agg AS
SELECT student_id, school_id, date_trunc('month', date) as month, academic_year, term,
  count(*) as total_sessions,
  sum(CASE WHEN status='Present' THEN 1 ELSE 0 END) as present_count,
  sum(CASE WHEN status='Authorised Absence' THEN 1 ELSE 0 END) as authorised_absence_count,
  sum(CASE WHEN status='Unauthorised Absence' THEN 1 ELSE 0 END) as unauthorised_absence_count,
  sum(CASE WHEN status='Late' THEN 1 ELSE 0 END) as late_count,
  round(sum(CASE WHEN status='Present' THEN 1 ELSE 0 END)*100.0/count(*), 1) as attendance_pct
FROM {CATALOG}.silver.attendance
GROUP BY student_id, school_id, date_trunc('month', date), academic_year, term
""")

# ============================================================
# TEAM_ANALYTICS
# ============================================================
print("\n=== TEAM_ANALYTICS ===")

run_sql("team_analytics.student_data", f"""
CREATE OR REPLACE TABLE {CATALOG}.team_analytics.student_data AS
SELECT student_id as learner_id, first_name as given_name, last_name as family_name,
  date_of_birth as dob, gender, email, phone, address, city, postcode,
  school_id, year_group, enrollment_date, is_sen as has_send, fsm_eligible as pupil_premium
FROM {CATALOG}.silver.students
""")

run_sql("team_analytics.school_info", f"""
CREATE OR REPLACE TABLE {CATALOG}.team_analytics.school_info AS
SELECT school_id, school_name, school_type, local_authority, region, ofsted_rating, num_pupils, phase
FROM {CATALOG}.silver.schools
""")

run_sql("team_analytics.exam_scores", f"""
CREATE OR REPLACE TABLE {CATALOG}.team_analytics.exam_scores AS
SELECT result_id, student_id, school_id, subject, exam_date,
  score as mark, grade as result_grade, exam_board as awarding_body, academic_year
FROM {CATALOG}.silver.exam_results
""")

run_sql("team_analytics.student_attendance", f"""
CREATE OR REPLACE TABLE {CATALOG}.team_analytics.student_attendance AS
SELECT * FROM {CATALOG}.silver.attendance
""")

# ============================================================
# TEAM_REPORTING
# ============================================================
print("\n=== TEAM_REPORTING ===")

run_sql("team_reporting.pupils", f"""
CREATE OR REPLACE TABLE {CATALOG}.team_reporting.pupils AS
SELECT student_id as pupil_id, first_name as pupil_first_name, last_name as pupil_last_name,
  date_of_birth as pupil_dob, gender, email, phone, address, city, postcode,
  school_id as establishment_id, year_group as national_curriculum_year,
  enrollment_date, is_sen, fsm_eligible
FROM {CATALOG}.silver.students
""")

run_sql("team_reporting.school_directory", f"""
CREATE OR REPLACE TABLE {CATALOG}.team_reporting.school_directory AS
SELECT *, 'England' as country, 'State-funded' as funding_type,
  CASE abs(hash(school_id * 99)) % 3 WHEN 0 THEN 'Urban' WHEN 1 THEN 'Suburban' ELSE 'Rural' END as area_classification
FROM {CATALOG}.silver.schools
""")

run_sql("team_reporting.assessment_results", f"""
CREATE OR REPLACE TABLE {CATALOG}.team_reporting.assessment_results AS
SELECT result_id, student_id, school_id, subject, exam_date,
  score as percentage_score, grade as final_grade, exam_board, academic_year,
  CASE WHEN grade IN ('A*','A','B','C') THEN 'Pass' ELSE 'Fail' END as pass_fail_indicator
FROM {CATALOG}.silver.exam_results
""")

run_sql("team_reporting.attendance_register", f"""
CREATE OR REPLACE TABLE {CATALOG}.team_reporting.attendance_register AS
SELECT attendance_id as record_id, student_id as pupil_id, school_id as establishment_id,
  date, status as attendance_mark, session as am_pm, academic_year, term as half_term
FROM {CATALOG}.silver.attendance
""")

# ============================================================
# TABLE COMMENTS
# ============================================================
print("\n=== Adding table comments ===")
comments = {
    "gold.dim_students": "Gold standard student dimension table with SCD Type 2 tracking. Source: silver.students. Refreshed daily.",
    "gold.dim_schools": "Gold standard school dimension table with SCD tracking. Source: GIAS extract via silver.schools. Refreshed weekly.",
    "gold.fact_exam_results": "Gold standard exam results fact table with pass/fail derivation. Source: silver.exam_results. Refreshed termly.",
    "gold.fact_attendance_agg": "Gold standard monthly attendance aggregation. Source: silver.attendance. Refreshed daily.",
    "silver.students": "Cleaned student records from MIS export.",
    "silver.schools": "Cleaned school records from GIAS.",
    "silver.exam_results": "Cleaned exam results from exam board feeds.",
    "silver.attendance": "Cleaned daily attendance records from MIS.",
}
for table, comment in comments.items():
    run_sql(f"Comment: {table}", f"COMMENT ON TABLE {CATALOG}.{table} IS '{comment}'")

# ============================================================
# GRANTS
# ============================================================
print("\n=== Applying grants ===")
for schema in ["bronze", "silver", "gold"]:
    run_sql(f"GRANT data_engineers on {schema}",
            f"GRANT ALL PRIVILEGES ON SCHEMA {CATALOG}.{schema} TO data_engineers")

run_sql("GRANT data_analysts on gold",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO data_analysts")
run_sql("GRANT data_analysts SELECT on gold",
        f"GRANT SELECT ON SCHEMA {CATALOG}.gold TO data_analysts")
run_sql("GRANT data_analysts on team_analytics",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.team_analytics TO data_analysts")
run_sql("GRANT data_analysts SELECT on team_analytics",
        f"GRANT SELECT ON SCHEMA {CATALOG}.team_analytics TO data_analysts")

run_sql("GRANT reporting_team on gold",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO reporting_team")
run_sql("GRANT reporting_team SELECT on gold",
        f"GRANT SELECT ON SCHEMA {CATALOG}.gold TO reporting_team")
run_sql("GRANT reporting_team on team_reporting",
        f"GRANT ALL PRIVILEGES ON SCHEMA {CATALOG}.team_reporting TO reporting_team")

print("\n=== ALL DONE ===")
