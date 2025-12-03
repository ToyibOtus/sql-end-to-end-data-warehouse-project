/*
=================================================================
Database Exploration
=================================================================
Script Purpose:
	This script explores the database, and checks objects and 
	columns relevant for analytics.
=================================================================
*/
SELECT * FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'gold' AND TABLE_TYPE = 'VIEW';

SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'gold' AND TABLE_NAME LIKE ('%view');
