-- Data Source: federated.14u9jyo1x4rfq40zzuj6c1nmag30
-- Caption: nfl_teams (postgres)
-- Fields Available: 75
-- ==================================================

-- CONNECTION DETAILS
-- ------------------------------
-- Connection 1:
--   server: aws-0-us-east-1.pooler.supabase.com
--   dbname: postgres
--   username: postgres.wlmdmpgnmadyuvsiztyx
--   dbclass: postgres
--   port: 5432

-- SQL QUERIES FROM TABLEAU
-- ------------------------------
-- Query 1: Table Import
SELECT * FROM [public].[nfl_dimers_lines];

-- Query 2: Table Import
SELECT * FROM [public].[props_teams];

-- Query 3: Table Import
SELECT * FROM [public].[nfl_teams];

-- Query 4: Instructions
-- TABLES TO IMPORT INTO POWER BI:
-- Away = [props_teams]
-- Away Teams = [nfl_teams]
-- Home = [props_teams]
-- Home Teams = [nfl_teams]
-- nfl_dimers_lines = [nfl_dimers_lines]



-- Query 5: Instructions
-- TABLEAU RELATIONSHIP STRUCTURE:
SELECT * FROM nfl_dimers_lines
left join Away.abbr = Away Teams.team_abbr
left join Home.abbr = Home Teams.team_abbr
left join nfl_dimers_lines.away_team = Away.abbr
left join nfl_dimers_lines.home_team = Home.abbr

-- POWER BI SETUP:
-- 1. Import all tables above
-- 2. Create relationships in Model view using the JOIN conditions above
-- 3. Set cardinality: Many (nfl_dimers_lines) to One (other tables)


-- POWER BI SETUP
-- ------------------------------
-- 1. Use connection details above to connect to database
-- 2. Import tables as needed
-- 3. Use field mapping CSV for column details
-- 4. Use SQL queries above as reference for data structure
