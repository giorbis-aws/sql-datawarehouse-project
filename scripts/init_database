/*
==========================================================================================
Create Database and Schemas
==========================================================================================
Script Purpose:
  The script creates a new database named 'Datawarehouse' after cheching if it alredy exists.
  If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

!Warning:
  Running this script will drop the entire 'Datawarehouse' database if exists.
  All data in the database will be permanently deleted. Proceed with caution and ensure you have proper backups before running this script.
*/

USE master;

-- Drop and recreate the 'Datawarehouse' database

Begin
  ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE Datawarehouse;
END;

GO

-- Create the 'Datawarehouse' database

CREATE DATABASE Datawarehouse;
GO
USE Datawarehouse;
GO
create Schema bronze;
GO
create Schema silver;
GO
create Schema gold;
