-- There are 2 types of normalization
-- 1. Normal Form (NF) 1 -> NF3 for which this procedure can be used
-- 2. NF2 -> NF3 accompanying procedure has comments on how to proceed using simple queries, please implement if anyone is interested

-- Your process will be in 3 steps:
 -- 1. Load client data into a staging Table - which is refered here as primod_account
 -- 2. Create the metadata, this SQL does that part
 -- 3. Run the normalize_procedure to get a high performance Normalization ENGINE

-- flags are documented in normalize_procedure

INSERT INTO NORMALIZE_METADATA
(denorm_table, nf3_table, denorm_cols, nf3_col_names, denorm_keys, nf3_biz_key, nf3_identity_col, nf3_identity_value, denorm_where, flags, is_completed)
VALUES
('primod_account','accounts_revenue','tot_ind_rev_prev2','amount','accountid','account_id',"type,year","42,2009",'tot_ind_rev_prev2 > 0','5','0'),
('primod_account','accounts_revenue','tot_grp_rev_prev2','amount','accountid','account_id','type,year','43,2009','tot_grp_rev_prev2 > 0','6','0'),
('primod_account','accounts_revenue','tot_rev_prev2','amount','accountid','account_id','type,year','44,2009','tot_rev_prev2 > 0','6','0'),
('primod_account','accounts_revenue','tot_ind_rev_prev1','amount','accountid','account_id','type,year','42,2010','tot_ind_rev_prev1 > 0','6','0'),
('primod_account','accounts_revenue','tot_grp_rev_prev1','amount','accountid','account_id','type,year','43,2010','tot_grp_rev_prev1 > 0','6','0'),
('primod_account','accounts_revenue','tot_rev_prev1','amount','accountid','account_id','type,year','44,2010','tot_rev_prev1 > 0','6','0'),
('primod_account','accounts_revenue','tot_ind_rev_curr','amount','accountid','account_id','type,year','42,2011','tot_ind_rev_curr > 0','6','0'),
('primod_account','accounts_revenue','tot_grp_rev_curr','amount','accountid','account_id','type,year','43,2011','tot_grp_rev_curr > 0','6','0'),
('primod_account','accounts_revenue','tot_rev_curr','amount','accountid','account_id','type,year','44,2011','tot_rev_curr > 0','6','0'),
('primod_account','accounts_revenue','tot_ind_rev_nex1','amount','accountid','account_id','type,year','42,2012','tot_ind_rev_nex1 > 0','6','0'),
('primod_account','accounts_revenue','tot_grp_rev_nex1','amount','accountid','account_id','type,year','43,2012','tot_grp_rev_nex1 > 0','6','0'),
('primod_account','accounts_revenue','tot_rev_nex1','amount','accountid','account_id','type,year','44,2012','tot_rev_nex1 > 0','6','0'),
('primod_account','accounts_pax','ind_pax_prev2','amount','accountid','account_id','type,year','42,2009','ind_pax_prev2 > 0','7','0'),
('primod_account','accounts_pax','grp_pax_prev2','amount','accountid','account_id','type,year','43,2009','grp_pax_prev2 > 0','6','0'),
('primod_account','accounts_pax','tot_pax_prev2','amount','accountid','account_id','type,year','44,2009','ind_pax_prev2 > 0','6','0'),
('primod_account','accounts_pax','ind_pax_prev1','amount','accountid','account_id','type,year','42,2010','ind_pax_prev1 > 0','6','0'),
('primod_account','accounts_pax','grp_pax_prev1','amount','accountid','account_id','type,year','43,2010','grp_pax_prev1 > 0','6','0'),
('primod_account','accounts_pax','tot_pax_prev1','amount','accountid','account_id','type,year','44,2010','tot_pax_prev1 > 0','6','0'),
('primod_account','accounts_pax','ind_pax_curr','amount','accountid','account_id','type,year','42,2011','ind_pax_curr > 0','6','0'),
('primod_account','accounts_pax','grp_pax_curr','amount','accountid','account_id','type,year','43,2011','grp_pax_curr > 0','6','0'),
('primod_account','accounts_pax','tot_pax_curr','amount','accountid','account_id','type,year','44,2011','tot_pax_curr > 0','6','0'),
('primod_account','accounts_pax','ind_pax_nxt1','amount','accountid','account_id','type,year','42,2012','ind_pax_nxt1 > 0','6','0'),
('primod_account','accounts_pax','grp_pax_nex1','amount','accountid','account_id','type,year','43,2012','grp_pax_nex1 > 0','6','0'),
('primod_account','accounts_pax','tot_pax_nex1','amount','accountid','account_id','type,year','44,2012','tot_pax_nex1 > 0','6','0');

-- Assuming these as type id's
-- individual 42,
-- group 43,
-- total 44,

call normalize();

-- delete all excess columns
-- This is client data centric
ALTER TABLE primod_accounts (
change column accountid id CHAR(36) NOT NULL PRIMARY KEY,
drop column accountimpid,
drop column industry,
drop column annualrevenue,
drop column rating,
drop column siccode,
drop column tickersymbol,
drop column otherphone,
drop column email2,
drop column brandimpid,
drop column regionimpid,
drop column territoryimpid,
drop column primaryuserid,
drop column secondaryuserid,
drop column tertiaryuserid,
drop column quaternaryuserid,
drop column primaryuserimpid,
drop column secondaryuserimpid,
drop column tertiaryuserimpid,
drop column quaternaryuserimpid,
drop column primaryusercovering,
drop column secondaryusercovering,
drop column tertiaryusercovering,
drop column quaternaryusercovering,
drop column primaryuserimprole,
drop column secondaryuserimprole,
drop column tertiaryuserimprole,
drop column quaternaryuserimprole,
drop column external_role_1,
drop column external_name_1,
drop column external_role_2,
drop column external_name_2,
drop column external_role_3,
drop column external_name_3,
drop column external_role_4,
drop column external_name_4,
drop column campaignid,
drop column ytd_ind_bk,
drop column lastmonth_ind_bk,
drop column lastweek_ind_bk,
drop column ytd_grp_bk,
drop column lastmonth_grp_bk,
drop column lastweek_grp_bk,
drop column ytd_tot_bk,
drop column lastmonth_tot_bk,
drop column lastweek_tot_bk,
drop column ytd_ind_of,
drop column lastmonth_ind_of,
drop column lastweek_ind_of,
drop column ytd_grp_of,
drop column lastmonth_grp_of,
drop column lastweek_grp_of,
drop column ytd_tot_of,
drop column lastmonth_tot_of,
drop column lastweek_tot_of,
drop column lastweek_tot_sl,
drop column lastweek_ind_sl,
drop column lastweek_grp_sl,
drop column ytd_tot_sl,
drop column campaignid
,drop column ytd_ind_bk
,drop column lastmonth_ind_bk
,drop column lastweek_ind_bk
,drop column ytd_grp_bk
,drop column lastmonth_grp_bk
,drop column lastweek_grp_bk
,drop column ytd_tot_bk
,drop column lastmonth_tot_bk
,drop column lastweek_tot_bk
,drop column ytd_ind_of
,drop column lastmonth_ind_of
,drop column lastweek_ind_of
,drop column ytd_grp_of
,drop column lastmonth_grp_of
,drop column lastweek_grp_of
,drop column ytd_tot_of
,drop column lastmonth_tot_of
,drop column lastweek_tot_of
,drop column flag_coop
,drop column flag_salesplan
,drop column flag_rmbcpi
,drop column flag_emailbounce
,drop column flag_firstdepositdue
,drop column flag_seconddepositdue
,drop column flag_homebased
,drop column bd_amt_prev2
,drop column bd_amt_prev1
,drop column bd_amt_curr
,drop column bd_amt_next
,drop column gtr_accel_prev2
,drop column gtr_accel_prev1
,drop column gtr_accel_curr
,drop column gtr_accel_next
,drop column vvi_amt_prev2
,drop column vvi_amt_prev1
,drop column vvi_amt_curr
,drop column vvi_amt_next
,drop column sum_sailed_days_prev2
,drop column sum_sailed_days_prev1
,drop column sum_sailed_days_curr
,drop column sum_sailed_days_next
,drop column automation
,drop column p1
,drop column p2
,drop column p3
,drop column p4
,drop column p5
,drop column p6
,drop column p7
,drop column p8
,drop column p9
,drop column p10
,drop column p11
,drop column p12
,drop column p13
,drop column p14
,drop column p15
,drop column p16
,drop column p17
,drop column p18
,drop column p19
,drop column p20
,drop column p21
,drop column p22
,drop column lastweek_tot_sl
,drop column lastweek_ind_sl
,drop column lastweek_grp_sl
,drop column ytd_tot_sl
,drop column grp_ret_prev2
,drop column grp_ret_prev1
,drop column grp_ret_curr
,drop column grp_ret_next
,drop column grp_mix_prev2
,drop column grp_mix_prev1
,drop column grp_mix_curr
,drop column grp_mix_nex1
,drop column tot_bk_rev_prev1
,drop column tot_bk_rev_curr  );
