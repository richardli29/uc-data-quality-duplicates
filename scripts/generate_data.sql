-- =============================================================================
-- UC Data Duplicates — Seed Data Generator
-- =============================================================================
-- Run in a Databricks SQL editor or notebook.
--
-- Before running, set the widget value or find-and-replace:
--   ${catalog}  →  your catalog name (e.g. my_catalog)
--
-- Creates 20 tables across 5 schemas with deliberate duplicates for testing
-- the duplicate-detection app.
-- =============================================================================

CREATE WIDGET TEXT catalog DEFAULT 'main';

-- ===========================  SCHEMAS  =======================================

CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze;
CREATE SCHEMA IF NOT EXISTS ${catalog}.silver;
CREATE SCHEMA IF NOT EXISTS ${catalog}.gold;
CREATE SCHEMA IF NOT EXISTS ${catalog}.team_analytics;
CREATE SCHEMA IF NOT EXISTS ${catalog}.team_reporting;

-- ===========================  SILVER  ========================================

CREATE OR REPLACE TABLE ${catalog}.silver.students AS
WITH names AS (
  SELECT id,
    CASE abs(hash(id)) % 14
      WHEN 0 THEN 'James' WHEN 1 THEN 'Emma' WHEN 2 THEN 'Oliver' WHEN 3 THEN 'Amelia'
      WHEN 4 THEN 'Harry' WHEN 5 THEN 'Isla' WHEN 6 THEN 'George' WHEN 7 THEN 'Sophie'
      WHEN 8 THEN 'Jack' WHEN 9 THEN 'Emily' WHEN 10 THEN 'Charlie' WHEN 11 THEN 'Mia'
      WHEN 12 THEN 'Thomas' ELSE 'Grace' END AS first_name,
    CASE abs(hash(id * 31)) % 12
      WHEN 0 THEN 'Smith' WHEN 1 THEN 'Jones' WHEN 2 THEN 'Williams' WHEN 3 THEN 'Taylor'
      WHEN 4 THEN 'Brown' WHEN 5 THEN 'Davies' WHEN 6 THEN 'Wilson' WHEN 7 THEN 'Evans'
      WHEN 8 THEN 'Thomas' WHEN 9 THEN 'Roberts' WHEN 10 THEN 'Johnson' ELSE 'Patel' END AS last_name
  FROM range(1, 5001) t(id)
)
SELECT
  10000 + id AS student_id,
  first_name,
  last_name,
  date_add('2005-01-01', abs(hash(id * 7)) % 2920) AS date_of_birth,
  CASE abs(hash(id * 13)) % 25
    WHEN 0 THEN 'Other'
    ELSE CASE WHEN abs(hash(id * 13)) % 2 = 0 THEN 'Male' ELSE 'Female' END
  END AS gender,
  concat(lower(first_name), '.', lower(last_name), cast(id % 1000 AS string), '@school.edu') AS email,
  concat('07', lpad(cast(abs(hash(id * 17)) % 900000000 + 100000000 AS string), 9, '0')) AS phone,
  concat(cast(abs(hash(id * 19)) % 200 AS string), ' ',
    CASE abs(hash(id * 23)) % 5
      WHEN 0 THEN 'High Street' WHEN 1 THEN 'Church Road'
      WHEN 2 THEN 'Station Lane' WHEN 3 THEN 'Park Avenue' ELSE 'Mill Road' END) AS address,
  CASE abs(hash(id * 29)) % 9
    WHEN 0 THEN 'London' WHEN 1 THEN 'Birmingham' WHEN 2 THEN 'Manchester'
    WHEN 3 THEN 'Leeds' WHEN 4 THEN 'Bristol' WHEN 5 THEN 'Sheffield'
    WHEN 6 THEN 'Liverpool' WHEN 7 THEN 'Newcastle' ELSE 'Nottingham' END AS city,
  concat(
    CASE abs(hash(id * 37)) % 4 WHEN 0 THEN 'SW' WHEN 1 THEN 'EC' WHEN 2 THEN 'NW' ELSE 'SE' END,
    cast(abs(hash(id * 41)) % 20 AS string), ' ',
    cast(abs(hash(id * 43)) % 9 AS string),
    CASE abs(hash(id * 47)) % 3 WHEN 0 THEN 'AB' WHEN 1 THEN 'CD' ELSE 'EF' END) AS postcode,
  cast(1 + abs(hash(id * 53)) % 500 AS int) AS school_id,
  cast(7 + abs(hash(id * 59)) % 7 AS int) AS year_group,
  date_add('2018-09-01', abs(hash(id * 61)) % 2190) AS enrollment_date,
  CASE WHEN abs(hash(id * 67)) % 100 < 15 THEN true ELSE false END AS is_sen,
  CASE WHEN abs(hash(id * 71)) % 100 < 23 THEN true ELSE false END AS fsm_eligible
FROM names;

-- silver.schools (500 rows)
CREATE OR REPLACE TABLE ${catalog}.silver.schools AS
SELECT
  id AS school_id,
  concat(
    CASE abs(hash(id * 3)) % 8
      WHEN 0 THEN 'St Marys' WHEN 1 THEN 'Kings' WHEN 2 THEN 'Oakwood'
      WHEN 3 THEN 'Riverside' WHEN 4 THEN 'Greenfield' WHEN 5 THEN 'Parklands'
      WHEN 6 THEN 'Hillcrest' ELSE 'Westgate' END, ' ',
    CASE abs(hash(id * 5)) % 4
      WHEN 0 THEN 'Academy' WHEN 1 THEN 'School'
      WHEN 2 THEN 'College' ELSE 'High School' END) AS school_name,
  CASE abs(hash(id * 7)) % 10
    WHEN 0 THEN 'Academy' WHEN 1 THEN 'Academy' WHEN 2 THEN 'Academy' WHEN 3 THEN 'Academy'
    WHEN 4 THEN 'Community School' WHEN 5 THEN 'Community School' WHEN 6 THEN 'Free School'
    WHEN 7 THEN 'Free School' WHEN 8 THEN 'Grammar School' ELSE 'Faith School' END AS school_type,
  CASE abs(hash(id * 11)) % 9
    WHEN 0 THEN 'London' WHEN 1 THEN 'Birmingham' WHEN 2 THEN 'Manchester'
    WHEN 3 THEN 'Leeds' WHEN 4 THEN 'Bristol' WHEN 5 THEN 'Sheffield'
    WHEN 6 THEN 'Liverpool' WHEN 7 THEN 'Newcastle' ELSE 'Nottingham' END AS local_authority,
  CASE abs(hash(id * 13)) % 9
    WHEN 0 THEN 'London' WHEN 1 THEN 'West Midlands' WHEN 2 THEN 'North West'
    WHEN 3 THEN 'Yorkshire' WHEN 4 THEN 'South West' WHEN 5 THEN 'Yorkshire'
    WHEN 6 THEN 'North West' WHEN 7 THEN 'North East' ELSE 'East Midlands' END AS region,
  concat(
    CASE abs(hash(id * 17)) % 4 WHEN 0 THEN 'SW' WHEN 1 THEN 'EC' WHEN 2 THEN 'NW' ELSE 'SE' END,
    cast(abs(hash(id * 19)) % 20 AS string), ' ',
    cast(abs(hash(id * 23)) % 9 AS string), 'XY') AS postcode,
  CASE abs(hash(id * 29)) % 20
    WHEN 0 THEN 'Outstanding' WHEN 1 THEN 'Outstanding' WHEN 2 THEN 'Outstanding' WHEN 3 THEN 'Outstanding'
    WHEN 16 THEN 'Requires Improvement' WHEN 17 THEN 'Requires Improvement'
    WHEN 18 THEN 'Requires Improvement' WHEN 19 THEN 'Inadequate'
    ELSE 'Good' END AS ofsted_rating,
  cast(200 + abs(hash(id * 31)) % 2300 AS int) AS num_pupils,
  CASE abs(hash(id * 37)) % 20
    WHEN 0 THEN 'All-through' WHEN 1 THEN 'All-through' WHEN 2 THEN 'All-through'
    WHEN 3 THEN 'Primary' WHEN 4 THEN 'Primary' WHEN 5 THEN 'Primary' WHEN 6 THEN 'Primary'
    WHEN 7 THEN 'Primary' WHEN 8 THEN 'Primary' WHEN 9 THEN 'Primary'
    ELSE 'Secondary' END AS phase,
  concat(
    CASE abs(hash(id * 41)) % 5
      WHEN 0 THEN 'Dr' WHEN 1 THEN 'Mr' WHEN 2 THEN 'Mrs' WHEN 3 THEN 'Ms' ELSE 'Prof' END, ' ',
    CASE abs(hash(id * 43)) % 4
      WHEN 0 THEN 'James' WHEN 1 THEN 'Sarah' WHEN 2 THEN 'David' ELSE 'Helen' END, ' ',
    CASE abs(hash(id * 47)) % 4
      WHEN 0 THEN 'Smith' WHEN 1 THEN 'Patel' WHEN 2 THEN 'Khan' ELSE 'Williams' END) AS headteacher,
  concat('head@school', cast(id AS string), '.edu') AS contact_email,
  concat('020 ', lpad(cast(abs(hash(id * 53)) % 9000000 + 1000000 AS string), 7, '0')) AS phone_number,
  concat('https://www.school', cast(id AS string), '.edu') AS website
FROM range(1, 501) t(id);

-- silver.exam_results (50,000 rows)
CREATE OR REPLACE TABLE ${catalog}.silver.exam_results AS
SELECT
  id AS result_id,
  cast(10001 + abs(hash(id * 3)) % 5000 AS int) AS student_id,
  cast(1 + abs(hash(id * 5)) % 500 AS int) AS school_id,
  CASE abs(hash(id * 7)) % 10
    WHEN 0 THEN 'Mathematics' WHEN 1 THEN 'English' WHEN 2 THEN 'Science'
    WHEN 3 THEN 'History' WHEN 4 THEN 'Geography' WHEN 5 THEN 'Art'
    WHEN 6 THEN 'Music' WHEN 7 THEN 'PE' WHEN 8 THEN 'Computing'
    ELSE 'Modern Languages' END AS subject,
  date_add('2023-05-01', abs(hash(id * 11)) % 60) AS exam_date,
  cast(greatest(0, least(100, 50 + abs(hash(id * 13)) % 51)) AS int) AS score,
  CASE abs(hash(id * 17)) % 20
    WHEN 0 THEN 'A*' WHEN 1 THEN 'A' WHEN 2 THEN 'A' WHEN 3 THEN 'B' WHEN 4 THEN 'B'
    WHEN 5 THEN 'B' WHEN 6 THEN 'B' WHEN 7 THEN 'C' WHEN 8 THEN 'C' WHEN 9 THEN 'C'
    WHEN 10 THEN 'C' WHEN 11 THEN 'C' WHEN 12 THEN 'D' WHEN 13 THEN 'D' WHEN 14 THEN 'D'
    WHEN 15 THEN 'E' WHEN 16 THEN 'E' WHEN 17 THEN 'E' WHEN 18 THEN 'U'
    ELSE 'C' END AS grade,
  CASE abs(hash(id * 19)) % 20
    WHEN 0 THEN 'AQA' WHEN 1 THEN 'AQA' WHEN 2 THEN 'AQA' WHEN 3 THEN 'AQA'
    WHEN 4 THEN 'AQA' WHEN 5 THEN 'AQA' WHEN 6 THEN 'AQA'
    WHEN 7 THEN 'Edexcel' WHEN 8 THEN 'Edexcel' WHEN 9 THEN 'Edexcel'
    WHEN 10 THEN 'Edexcel' WHEN 11 THEN 'Edexcel' WHEN 12 THEN 'Edexcel'
    WHEN 13 THEN 'OCR' WHEN 14 THEN 'OCR' WHEN 15 THEN 'OCR' WHEN 16 THEN 'OCR' WHEN 17 THEN 'OCR'
    ELSE 'WJEC' END AS exam_board,
  CASE WHEN abs(hash(id * 23)) % 100 < 45 THEN '2022/23' ELSE '2023/24' END AS academic_year
FROM range(1, 50001) t(id);

-- silver.attendance (100,000 rows)
CREATE OR REPLACE TABLE ${catalog}.silver.attendance AS
SELECT
  id AS attendance_id,
  cast(10001 + abs(hash(id * 3)) % 5000 AS int) AS student_id,
  cast(1 + abs(hash(id * 5)) % 500 AS int) AS school_id,
  date_add('2023-09-01', abs(hash(id * 7)) % 200) AS date,
  CASE abs(hash(id * 11)) % 20
    WHEN 0 THEN 'Authorised Absence' WHEN 1 THEN 'Authorised Absence'
    WHEN 2 THEN 'Unauthorised Absence' WHEN 3 THEN 'Late'
    ELSE 'Present' END AS status,
  CASE WHEN abs(hash(id * 13)) % 2 = 0 THEN 'AM' ELSE 'PM' END AS session,
  '2023/24' AS academic_year,
  CASE abs(hash(id * 17)) % 3
    WHEN 0 THEN 'Autumn' WHEN 1 THEN 'Spring' ELSE 'Summer' END AS term
FROM range(1, 100001) t(id);

-- ===========================  BRONZE  ========================================

CREATE OR REPLACE TABLE ${catalog}.bronze.raw_students AS
SELECT
  student_id AS STUDENT_ID, first_name AS FirstName, last_name AS LastName,
  date_of_birth AS DOB, gender, email AS Email_Address, phone, address, city, postcode,
  school_id, year_group, enrollment_date, is_sen, fsm_eligible,
  'raw_import_2024' AS _source_file, '2024-01-15T10:30:00' AS _ingestion_ts
FROM ${catalog}.silver.students;

CREATE OR REPLACE TABLE ${catalog}.bronze.raw_schools AS
SELECT
  school_id AS SchoolID, school_name AS SchoolName, school_type, local_authority, region,
  postcode, ofsted_rating, num_pupils, phase, headteacher, contact_email, phone_number, website,
  'gias_extract' AS _source
FROM ${catalog}.silver.schools;

CREATE OR REPLACE TABLE ${catalog}.bronze.raw_exam_results AS
SELECT
  result_id AS ResultID, student_id AS StudentID, school_id, subject, exam_date,
  score AS RawScore, grade, exam_board, academic_year, 'exam_board_feed' AS _data_source
FROM ${catalog}.silver.exam_results;

CREATE OR REPLACE TABLE ${catalog}.bronze.raw_attendance AS
SELECT
  attendance_id AS RecordID, student_id AS StudentID, school_id, date,
  status AS AttendanceCode, session, academic_year, term, 'mis_export' AS _source_system
FROM ${catalog}.silver.attendance;

-- ===========================  GOLD  ==========================================

CREATE OR REPLACE TABLE ${catalog}.gold.dim_students AS
SELECT *, true AS is_current, 1 AS scd_version,
  enrollment_date AS valid_from, cast(null AS date) AS valid_to
FROM ${catalog}.silver.students;

CREATE OR REPLACE TABLE ${catalog}.gold.dim_schools AS
SELECT *, true AS is_current, 1 AS scd_version
FROM ${catalog}.silver.schools;

CREATE OR REPLACE TABLE ${catalog}.gold.fact_exam_results AS
SELECT *, cast(score AS double) AS score_pct,
  CASE WHEN grade IN ('A*','A','B','C') THEN true ELSE false END AS is_pass
FROM ${catalog}.silver.exam_results;

CREATE OR REPLACE TABLE ${catalog}.gold.fact_attendance_agg AS
SELECT
  student_id, school_id, date_trunc('month', date) AS month, academic_year, term,
  count(*) AS total_sessions,
  sum(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) AS present_count,
  sum(CASE WHEN status = 'Authorised Absence' THEN 1 ELSE 0 END) AS authorised_absence_count,
  sum(CASE WHEN status = 'Unauthorised Absence' THEN 1 ELSE 0 END) AS unauthorised_absence_count,
  sum(CASE WHEN status = 'Late' THEN 1 ELSE 0 END) AS late_count,
  round(sum(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) * 100.0 / count(*), 1) AS attendance_pct
FROM ${catalog}.silver.attendance
GROUP BY student_id, school_id, date_trunc('month', date), academic_year, term;

-- ===========================  TEAM_ANALYTICS  ================================

CREATE OR REPLACE TABLE ${catalog}.team_analytics.student_data AS
SELECT
  student_id AS learner_id, first_name AS given_name, last_name AS family_name,
  date_of_birth AS dob, gender, email, phone, address, city, postcode,
  school_id, year_group, enrollment_date,
  is_sen AS has_send, fsm_eligible AS pupil_premium
FROM ${catalog}.silver.students;

CREATE OR REPLACE TABLE ${catalog}.team_analytics.school_info AS
SELECT school_id, school_name, school_type, local_authority, region, ofsted_rating, num_pupils, phase
FROM ${catalog}.silver.schools;

CREATE OR REPLACE TABLE ${catalog}.team_analytics.exam_scores AS
SELECT
  result_id, student_id, school_id, subject, exam_date,
  score AS mark, grade AS result_grade, exam_board AS awarding_body, academic_year
FROM ${catalog}.silver.exam_results;

CREATE OR REPLACE TABLE ${catalog}.team_analytics.student_attendance AS
SELECT * FROM ${catalog}.silver.attendance;

-- ===========================  TEAM_REPORTING  ================================

CREATE OR REPLACE TABLE ${catalog}.team_reporting.pupils AS
SELECT
  student_id AS pupil_id, first_name AS pupil_first_name, last_name AS pupil_last_name,
  date_of_birth AS pupil_dob, gender, email, phone, address, city, postcode,
  school_id AS establishment_id, year_group AS national_curriculum_year,
  enrollment_date, is_sen, fsm_eligible
FROM ${catalog}.silver.students;

CREATE OR REPLACE TABLE ${catalog}.team_reporting.school_directory AS
SELECT *,
  'England' AS country, 'State-funded' AS funding_type,
  CASE abs(hash(school_id * 99)) % 3
    WHEN 0 THEN 'Urban' WHEN 1 THEN 'Suburban' ELSE 'Rural' END AS area_classification
FROM ${catalog}.silver.schools;

CREATE OR REPLACE TABLE ${catalog}.team_reporting.assessment_results AS
SELECT
  result_id, student_id, school_id, subject, exam_date,
  score AS percentage_score, grade AS final_grade, exam_board, academic_year,
  CASE WHEN grade IN ('A*','A','B','C') THEN 'Pass' ELSE 'Fail' END AS pass_fail_indicator
FROM ${catalog}.silver.exam_results;

CREATE OR REPLACE TABLE ${catalog}.team_reporting.attendance_register AS
SELECT
  attendance_id AS record_id, student_id AS pupil_id, school_id AS establishment_id,
  date, status AS attendance_mark, session AS am_pm, academic_year, term AS half_term
FROM ${catalog}.silver.attendance;

-- ===========================  TABLE COMMENTS  ================================

COMMENT ON TABLE ${catalog}.gold.dim_students IS 'Gold standard student dimension table with SCD Type 2 tracking. Source: silver.students. Refreshed daily.';
COMMENT ON TABLE ${catalog}.gold.dim_schools IS 'Gold standard school dimension table with SCD tracking. Source: GIAS extract via silver.schools. Refreshed weekly.';
COMMENT ON TABLE ${catalog}.gold.fact_exam_results IS 'Gold standard exam results fact table with pass/fail derivation. Source: silver.exam_results. Refreshed termly.';
COMMENT ON TABLE ${catalog}.gold.fact_attendance_agg IS 'Gold standard monthly attendance aggregation. Source: silver.attendance. Refreshed daily.';
COMMENT ON TABLE ${catalog}.silver.students IS 'Cleaned student records from MIS export.';
COMMENT ON TABLE ${catalog}.silver.schools IS 'Cleaned school records from GIAS.';
COMMENT ON TABLE ${catalog}.silver.exam_results IS 'Cleaned exam results from exam board feeds.';
COMMENT ON TABLE ${catalog}.silver.attendance IS 'Cleaned daily attendance records from MIS.';

-- ===========================  GRANTS  ========================================
-- These require the groups to already exist in the workspace.
-- Remove or adjust if you don't have these groups.

GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.bronze TO data_engineers;
GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.silver TO data_engineers;
GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.gold TO data_engineers;

GRANT USE SCHEMA ON SCHEMA ${catalog}.gold TO data_analysts;
GRANT SELECT ON SCHEMA ${catalog}.gold TO data_analysts;
GRANT USE SCHEMA ON SCHEMA ${catalog}.team_analytics TO data_analysts;
GRANT SELECT ON SCHEMA ${catalog}.team_analytics TO data_analysts;

GRANT USE SCHEMA ON SCHEMA ${catalog}.gold TO reporting_team;
GRANT SELECT ON SCHEMA ${catalog}.gold TO reporting_team;
GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.team_reporting TO reporting_team;
