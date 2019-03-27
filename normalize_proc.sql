DROP PROCEDURE IF EXISTS `normalize`;
DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `normalize`()
BEGIN

DECLARE done INT DEFAULT 0;

DECLARE my_id int;
DECLARE my_denorm_table varchar(255);
DECLARE my_nf3_table varchar(255);
DECLARE my_denorm_cols varchar(255);
DECLARE my_nf3_col_names varchar(255);
DECLARE my_denorm_keys varchar(255);
DECLARE my_nf3_biz_key	varchar(255);
DECLARE my_nf3_identity_col varchar(255);
DECLARE my_nf3_identity_value varchar(255);
DECLARE my_denorm_where varchar(255);
DECLARE my_flags bigint;

DECLARE meta_cur CURSOR FOR
SELECT id, denorm_table, nf3_table, denorm_cols, nf3_col_names, denorm_keys, nf3_biz_key, nf3_identity_col, nf3_identity_value, denorm_where, flags 
FROM `normalize_metadata`
WHERE `is_completed` = 0
order by id;

DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = 1;

OPEN meta_cur;
-- USAGE = convert a 1nf relation to 3nf. Here's an example of 1NF
-- Customer ID	First Name	Surname	Tel. No. 1	Tel. No. 2    Tel. No. 3
-- 123	Robert		Ingram	555-861-2025	
-- 456	Jane	Wright			555-403-1659	555-776-4100
-- 789	Maria	Fernandez		555-808-9633	
	
-- TODO 2NF normalization (multiple rows per record) for eg.
-- TODO create a new base + derived table off the old base
-- #1 CREATE MASTER TABLE 'A' WITH UNIQUE INDEX
-- #2 INSERT IGNORE INTO 'A' 
-- #3 CREATE MASTER TABLE 'B' WITH UNIQUE INDEX
-- #4 INSERT IGNORE INTO 'B'
-- #5 CREATE JOIN TABLE 'X' WITH UNIQUE INDEX ON BOTH KEYS, ADD ADDITIONAL ATTRIBUTES OF JOIN (IF ANY)
-- #6 INSERT INTO X WHILE TRACKING ERRORS 
-- Employee	Skill		Current Work Location
-- Jones		Typing		114 Main Street
-- Jones		Shorthand		114 Main Street
-- Jones		Whittling		114 Main Street
-- Bravo		Light Cleaning	73 Industrial Way
-- Ellis		Alchemy		73 Industrial Way
-- Ellis		Juggling		73 Industrial Way
-- Harrison	Light Cleaning	73 Industrial Way

-- TODO sort fields in order of input rows
REPEAT
	FETCH meta_cur into my_id, my_denorm_table, my_nf3_table, my_denorm_cols, my_nf3_col_names, my_denorm_keys, my_nf3_biz_key, my_nf3_identity_col, my_nf3_identity_value, my_denorm_where, my_flags;
	-- FLAGS (upto 64 - bigint)
	-- create new nf3 table = 1, else use existing table
	-- delete denorm table fields = 2  delete extra columns (can only be used in case of 1NF and recommended for large tables)
	-- don't insert null values flag = 4
	-- Use UUID for idcolumn instead of auto-increment = 8
	-- 
	IF NOT done THEN
		IF (my_flags & 4 = 4) THEN 
			SET @flag_where = CONCAT(' AND ',arrMerge(my_denorm_cols,'',',',' is not null',' and '));
			SELECT @flag_where;
		ELSE
			SET @flag_where = '';
		END IF;
		IF (my_flags & 1 = 1) THEN
			SET @qry = CONCAT('CREATE TABLE ',my_nf3_table,' as 
SELECT ');
			IF (my_flags & 8 = 8) THEN
				SET @qry := CONCAT(@qry,'uuid() as id,');
			ELSE
				-- assuming table does not exist since we are creating it, so can start with 1
				SELECT @m:=0;
				SET @qry := CONCAT(@qry,'@m := @m+1 as id,');
			END IF;
			SET @qry := CONCAT(@qry, arrMerge(my_denorm_cols,my_nf3_col_names,',',' as ',','),',',arrMerge(my_nf3_identity_value,my_nf3_identity_col,',',' as ',','),',',my_denorm_keys,' as ',my_nf3_biz_key,' 
FROM ',my_denorm_table,' 
WHERE ',my_denorm_where,' ',@flag_where,' 
GROUP BY ',my_denorm_table,'.',my_denorm_keys);
			select @qry;
			PREPARE stmt FROM @qry;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			COMMIT;
			-- ALTER TABLE MAKE ID AS NOT NULL AUTO_INCREMENT PRIMARY KEY
			IF (my_flags & 8 = 8) THEN
				SET @qry := CONCAT('ALTER TABLE ',my_nf3_table,' MODIFY ID CHAR(36) NOT NULL PRIMARY KEY');
			ELSE
				SET @qry = CONCAT('ALTER TABLE ',my_nf3_table,' MODIFY ID INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY');
			END IF;
			select @qry;
			PREPARE stmt FROM @qry;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			COMMIT;
		ELSE
			SET @qry := CONCAT('INSERT INTO ',my_nf3_table,'(',my_nf3_col_names,',',my_nf3_identity_col,',',my_nf3_biz_key,')',' 
SELECT ');
			IF (my_flags & 8 = 8) THEN
				SET @qry := CONCAT(@qry,'uuid(),');
			END IF;
			SET @qry := CONCAT(@qry,my_denorm_cols,',',my_nf3_identity_value,',',my_denorm_keys,' 
FROM ',my_denorm_table,' 
WHERE ',my_denorm_where,' ',@flag_where,' 
GROUP BY ',my_denorm_table,'.',my_denorm_keys);
			select @qry;
			PREPARE stmt FROM @qry;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			COMMIT;

		END IF;

		-- delete extra columns in denorm table
		if (my_flags & 2 = 2) then
			set @delim = ',';
			set @cnt = if(isnull(my_denorm_cols) or my_denorm_cols =' ', 0, substrCount(my_denorm_cols, @delim));
			SET @i = 0;
			-- add 1 to @cnt for base0 to base1 conversion
			SET @cnt = @cnt + 1;
			WHILE @i<@cnt DO
				-- increment operation
				set @i = @i+1;
				set @k = strSplit(my_denorm_cols, @delim, @i);
				SET @qry = CONCAT('alter table ',my_denorm_table,' drop column ',@k);
				select @qry;
				PREPARE stmt FROM @qry;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
				COMMIT;
			END WHILE;
		end if;

		-- mark this row as completed
		update normalize_metadata set is_completed = 1 where id = my_id;
		commit;
	END IF;
UNTIL done END REPEAT;

CLOSE meta_cur;

END
$$