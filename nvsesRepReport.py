import pandas as pd
import json
import pymysql
import paramiko
from datetime import date
import dropbox
from sshtunnel import SSHTunnelForwarder

today = date.today()
today = today.strftime("%Y%m%d")
print(today)

# from paramiko import SSHClient
from sshtunnel import SSHTunnelForwarder

with open("/Users/SergioPina/Documents/pyCredents.json") as f:
    credents = json.load(f)

custCountry = 'novartises'
# ssh config  , pending to put this file rout in credents.Json

mypkey = paramiko.RSAKey.from_private_key_file('/Program Files/DBeaver/sergio.pina.pem')
ssh_host = 'bdpeu001.aktana.com'
ssh_port = 22
# mysql config
sql_hostname = custCountry + 'rds.aktana.com'
sql_main_database = custCountry + 'prod'
sql_port = 3306
host = custCountry + 'rds.aktana.com'
tunnel = SSHTunnelForwarder(
    (ssh_host, ssh_port),
    ssh_username=credents['username'],
    ssh_password=credents['password'],
    ssh_pkey=mypkey,
    remote_bind_address=(sql_hostname, sql_port))
tunnel.start()
# tunnel.close()
conn = pymysql.connect(host='127.0.0.1', user=credents['username'],
                       passwd=credents['password'], db=sql_main_database,
                       port=tunnel.local_bind_port)

print("MySQL connection succeded at {}.", pymysql.DATETIME)

list = ['1', '2', '19', '20', '21']

total_df = pd.DataFrame()

for i in list:
    cur = conn.cursor()

    query = '''Select 
    case when ifnull((select if(count(*)>0, 'Time Off', null) from RepUnavailablePeriod rup where rup.repId = r.repId and curdate() between startDate and endDate group by rup.repid),'Working') ='Time Off'
       then 'Time Off'
       else if(suggestions.suggesteddate>0 or ins.published_ins>0,date(now()),'') 
       end as suggestions_insights_received
        ,rgr.rungroupid
        ,concat(r.`seConfigId`,' - ',d.seConfigName) as Config
        -- ,suggestions.runId
        ,rt.repteamname
     		, r.`isActivated`
     		,ifnull((select if(count(*)>0, 'Time Off', null) from RepUnavailablePeriod rup where rup.repId = r.repId and curdate() between startDate and endDate group by rup.repid),'Working') as RepUnavailable
            , r.repname
        -- ,ifnull((select 1 as is_Awareness from novartisesprod_cs.`User` u join novartisesprod.Rep Re on u.Id=Re.externalId join novartisesprod_stage.AKT_Awareness_Reps ar on ar.Division=u.xR1_Division__c where Re.repId = r.repId),0) as is_Awareness
        -- , suggestions.factorname
        -- , suggestions.factortype
        ,suggestions.accountcount as suggested_Accounts_DSE
        ,suggestions.factorcount as suggested_Factors_DSE
        #,sugg.published as suggested_Accounts_published
       ,(select count(distinct etl.accountuid) from DSESuggestionLifecycle dl join ETL_Publish_Veeva_SparkSuggestion3_v etl on etl.suggestionReferenceId =dl.suggestionReferenceId 
    where dl.repId=r.repid and lastPublishRunId in (Select MAX(r.runId) from SparkDSERun r  where date(r.startDateTime) = date(now()) and seconfigId in  ("''' + i + '''") group by r.seConfigId, r.rungroupid having MAX(r.startDateTime)) and externalId is not null and dl.suggestionReferenceId not like '%AKTEI%') as suggested_Accounts_PUB
           ,(select count(*) from DSESuggestionLifecycle dl where dl.repId=r.repid and lastPublishRunId in (Select MAX(r.runId) from SparkDSERun r where date(r.startDateTime) = date(now()) and seconfigId in  ("''' + i + '''") group by r.seConfigId, r.rungroupid having MAX(r.startDateTime)) and externalId is not null and suggestionReferenceId like '%AKTEI%') as insights_all_PUB
               ,ins.published_ins as insights_Accounts_PUB
             #  ,(select count(distinct accountUID) from ETL_Publish_Veeva_SparkEnhancedInsight3_v sugg where sugg.repuId=r.repid and RunId in (Select MAX(r.runId) from SparkDSERun r where date(r.startDateTime) = date(now()) and seconfigId in  ("''' + i + '''") group by r.seConfigId, r.rungroupid having MAX(r.startDateTime))) as insight_ETL
          # ,(select count(distinct etl.accountuid) from DSESuggestionLifecycle dl join ETL_Publish_Veeva_SparkEnhancedInsight3_v etl on etl.suggestionReferenceId =dl.suggestionReferenceId 
    #where dl.repId=r.repid and lastPublishRunId in (Select MAX(r.runId) from SparkDSERun r  where date(r.startDateTime) = date(now()) and seconfigId in  ("''' + i + '''") group by r.seConfigId, r.rungroupid having MAX(r.startDateTime)) and externalId is not null and dl.suggestionReferenceId like '%AKTEI%') as insights_new2
        #,is_targeted.target as targeted_Accounts
        #,instarg.acc as targeted_Accounts_Insights_SFDC
        ,(Select count(*) FROM Account where accountId in (SELECT accountId FROM RepAccountAssignment  where repId = r.repId and endDate > date(now())) and isDeleted = 0) as num_aligned_Accounts
        ,(select count(*) from RepAccountAssignment raa where raa.repId = r.repId and accountId not in (select accountId from RepAccountAssignment raa2 where raa2.repId <> raa.repId)) as non_Shared_Accounts
        ,(select count(distinct accountid) from `StrategyTarget`st join TargetsPeriod tp using(targetsPeriodId) where st.repid=r.repid  and tp.startDate <= curdate() and tp.endDate >= curdate()) as targeted_accounts_strategytarget
        #,(select count(distinct accountid) from `AccountProduct`ap where enumIsHCPTargetted_akt='yes' and 
        #accountId in (SELECT accountId FROM RepAccountAssignment  where repId = r.repId and endDate > date(now())) and r.isDeleted = 0) and
        #productId in (Select p.productid from Product p where p.externalid in (Select t.product from novartisesprod_stage.AKT_PFIZERUK_TEAMS t where t.cms_Salesforce__c in (Select t.cms_Salesforce__c from novartisesprod_cs.User u where u.id=r.externalid and r.rep)))) as targeted_accounts
     #   ,(Select count(distinct accountid) from Account a where hcpEmailConsent_akt = 'yes' and accountId in (SELECT accountId FROM RepAccountAssignment  where repId = r.repId)) as consented_Accounts
        ,(Select Count(distinct accountid) from Account a where enumIsPersonAccount_akt='yes' and accountId in (SELECT accountId FROM RepAccountAssignment  where repId = r.repId)) as IsPersonAccount
        #,(Select Count(distinct accountid) from AccountProduct a where enumSegment_akt not in ('N/A') and accountId in (SELECT accountId FROM RepAccountAssignment where repId = r.repId)) as SegmentedAccounts
        #,(select max(suggestedDate) from SparkDSERunRepDate rrd where rrd.repid=r.repid) as last_suggestion
       # ,(SELECT count(*) FROM Interaction where repId = r.repId and isDeleted = 0 and interactionId in
       #     (SELECT interactionId FROM InteractionAccount where accountId in
        #    (SELECT accountId FROM RepAccountAssignment  where repId = r.repId and endDate > date(now())) and isDeleted = 0))
        #    as numfutureInteractions
        #,(SELECT count(*) FROM Event where
        #    productId in (SELECT productId FROM Product where productId in (SELECT productId FROM RepProductAuthorization where repId = r.repId) and isActive = 1 and isDeleted = 0)
        #    and accountId in (Select accountId FROM Account where accountId in (SELECT accountId FROM RepAccountAssignment  where repId = r.repId and endDate > date(now())) and isDeleted = 0)
        #    and eventdate<=date(now()))
         #   as numpastEvents
       # ,(SELECT count(*) FROM Event where
      #      productId in (SELECT productId FROM Product where productId in (SELECT productId FROM RepProductAuthorization where repId = r.repId) and isActive = 1 and isDeleted = 0)
        #    and accountId in (Select accountId FROM Account where accountId in (SELECT accountId FROM RepAccountAssignment  where repId = r.repId and endDate > date(now())) and isDeleted = 0)
          #  and eventdate>date(now()))
          #  as numfutureEvents
        ,(select max(syncTime) as maxsync from novartisesprod_stage.VeevaSync vs where vs.repUID = r.externalId) as last_veevaSync
        ,da.daily_avr_sync_last_week
       # ,(select `Last_iPad_iOS_Version_vod__c`from novartisesprod_cs.User u where u.id=r.externalid) as Last_iPad_iOS_Version
      #  ,(select GROUP_CONCAT(p.productName SEPARATOR ', ') from RepProductAuthorization rpa join Product p on rpa.productId = p.productId where rpa.repId = r.repId)
       #     as repProducts_Authorization_DSE
         ,(select GROUP_CONCAT(distinct p.Name SEPARATOR ', ') from 
        novartisesprod_cs.My_Setup_Products_vod__c ss  
        join novartisesprod.Rep Re on ss.OwnerId=Re.externalId 
        join novartisesprod_cs.Product_vod__c p on ss.Product_vod__c=p.id 
        where 
         Re.repId = r.repId) as Rep_prods_cs
        from Rep r
        join RepTeamRep rtr on rtr.repid=r.repid
        join RepTeam rt on rtr.repteamid=rt.repteamid
        join DSEConfigRunGroupRep rgr on rgr.repid=r.repid
        join DSEConfig d on d.seConfigId =r.seConfigId 
            left join
        (SELECT distinct
            r.runId,
            rrd.suggesteddate,
            rr.externalid,
            -- rcf.factorname,
            -- rcf.factorType,
            #rrda.suggestedRepActionTypeUID as Channeltype, 
            concat(count(distinct rrda.accountUID),' (SENT ',if(rrda.suggestedRepActionTypeUID='SEND_ANY',COUNT(distinct rrda.accountUID),'0'),')') AS accountcount,
    count(distinct rrda.runRepDateSuggestionId) as nrsugg,
            #count(distinct rr.repid) as repcount
            #, 
            count(distinct rcf.factorname) as factorcount
        FROM
            SparkDSERun r
                JOIN
            SparkDSERunRepDate rrd USING (runUID)
                JOIN
            SparkDSERunRepDateSuggestion rrda USING (runRepDateId)
                LEFT JOIN
            SparkDSERunRepDateSuggestionReason rrdar USING (runRepDateSuggestionId)
                INNER JOIN
            SparkDSERunConfigFactor rcf ON rcf.factorUID = rrdar.factorUID
                JOIN
            Account a USING (accountId)
                JOIN
            Rep rr ON (rr.repId = rrd.repId)
        WHERE r.runId in (Select MAX(r.runId) from SparkDSERun r where date(r.startDateTime) = date(now()) and seconfigId in ("''' + i + '''") group by r.seConfigId, r.rungroupid having MAX(r.startDateTime))
        #WHERE r.runId = ('4948')
        and rcf.factorname <> 'DSE Internal factor for constructing content for SUGGESTED' -- header of suggestions
        group by rrd.suggesteddate,rr.externalid#,rr.repname
        #, rrda.suggestedRepActionTypeUID
        ORDER BY runid DESC) as suggestions on suggestions.externalid=r.externalid
        left join 
        (
        SELECT
         rl.externalid, rl.repname, count(distinct Suggestion_vod__c.account_vod__c) as published
        FROM novartisesprod_cs.Suggestion_vod__c
        #join novartisesprod_cs.User u on u.id=OwnerId
        join novartisesprod_stage.AKT_RepLicense rl on rl.externalid=OwnerId
        WHERE 1=1
        and OwnerId in (select externalId from Rep where repId in (select repId from DSEConfigRunGroupRep where seconfigid in ("''' + i + '''") group by repid))
        and Call_Objective_Record_Type_vod__c = 'Suggestion_vod' -- Enhanced Inishts this chande
        and Expiration_date_vod__c > now()
        group by rl.repname) as sugg on sugg.externalid=r.externalid
        left join 
        (
        SELECT
         rl.externalid, rl.repname, count(distinct Suggestion_vod__c.account_vod__c) as published_ins
        FROM novartisesprod_cs.Suggestion_vod__c
        #join novartisesprod_cs.User u on u.id=OwnerId
        join novartisesprod_stage.AKT_RepLicense rl on rl.externalid=OwnerId
        WHERE 1=1
        and OwnerId in (select externalId from Rep where repId in (select repId from DSEConfigRunGroupRep where seconfigid in ("''' + i + '''") group by repid))
        and Call_Objective_Record_Type_vod__c = 'Insight_vod' -- Enhanced Inishts this chande
        and Expiration_date_vod__c > now()
        group by rl.repname) as ins on ins.externalid=r.externalid
        left join (select SYNC.repId, concat(round(SYNC.count_sync/(SYNC.day_ww-SYNC.days_off),2),' (working days considered for the average syncing: ',(SYNC.day_ww-SYNC.days_off),')')as daily_avr_sync_last_week from
        (select TOT.count_sync, TOT.repId, TOT.start_date, TOT.end_date
        ,ifnull(case when TOT.start_date=TOT.end_date then '1'
        when TOT.end_date=CURDATE() THEN 5 * (DATEDIFF(TOT.end_date, TOT.start_date) DIV 7) + MID('0123444401233334012222340111123400012345001234550', 7 * WEEKDAY(TOT.start_date) + WEEKDAY(TOT.end_date) + 1, 1) 
        ELSE 5 * (DATEDIFF(TOT.end_date, TOT.start_date) DIV 7) + MID('0123444401233334012222340111123400012345001234550', 7 * WEEKDAY(TOT.start_date) + WEEKDAY(TOT.end_date) + 1, 1) + 1
        end,'0') as days_off
        , 5 * (DATEDIFF(CURDATE() , CURDATE() - interval 7 day) DIV 7) + MID('0123444401233334012222340111123400012345001234550', 7 * WEEKDAY(CURDATE()) + WEEKDAY(CURDATE() - interval 7 day) + 1, 1) 
        as day_ww
        ,CURDATE() - interval 7 day
        ,CURDATE()
        FROM
        (SELECT Rr.repId,count(vs.syncTime) as count_sync
        ,max(B.start_date)
        ,max(B.end_date)
        ,CASE WHEN max(B.start_date)>CURDATE() - interval 7 day THEN max(B.start_date)
        	  when max(B.start_date)<=CURDATE() - interval 7 day THEN CURDATE() - interval 7 day
        	  else NULL end as start_date
        ,CASE WHEN max(B.end_date)<CURDATE() then max(B.end_date)
        	  when max(B.end_date)>=CURDATE() THEN CURDATE() 
        	  else NULL end as end_date
        from novartisesprod_stage.VeevaSync vs
        join novartisesprod.Rep Rr on vs.repUID=Rr.externalId 
        left join
        (select A.repId, min(A.startDate) as start_date, max(A.endDate) as end_date from
        (select Re.repId, run.startDate,run.endDate
        from RepUnavailablePeriod run
        join novartisesprod.Rep Re on Re.repId =run.repId 
        where  1=1
        -- and Re.repId='1496'
        and (((run.startDate BETWEEN CURDATE() - interval 7 day and CURDATE()) and  run.endDate>CURDATE())  -- W ,AW
        or ((run.startDate < CURDATE() - interval 7 day) and run.endDate>CURDATE())                    -- BW  AW
        or ((run.startDate < CURDATE() - interval 7 day) and run.endDate BETWEEN CURDATE() - interval 7 day and CURDATE()) -- BW W
        or ((run.startDate BETWEEN CURDATE() - interval 7 day and CURDATE()) and  run.endDate BETWEEN CURDATE() - interval 7 day and CURDATE())) -- W W
        ) as A
        group by A.repId) as B
        on Rr.repId=B.repId 
        where 
        DAYOFWEEK(SyncTime) NOT IN ('7', '1') 
        and (SyncTime BETWEEN CURDATE() - interval 7 day and CURDATE())
        group by Rr.repId) as TOT) AS SYNC) as da on da.repId=r.repId
        where r.seconfigid in ("''' + i + '''")
        order by 
         rt.repteamname,rgr.rungroupid;'''

    print(query)

    cur.execute(query)

    rows = cur.fetchall()

    df = pd.DataFrame(rows)
    cur.close()

    print(df)
    column_names = [i[0] for i in cur.description]
    print(column_names)

    total_df = total_df.append(df, ignore_index=True)

    print(total_df)

    print(i)

total_df.columns = column_names
total_df['suggested_Accounts_DSE'] = total_df['suggested_Accounts_DSE'].str.decode('ASCII')

export_csv = total_df.to_csv(
    "/Users/SergioPina/Desktop/Report_novartisSP_Onyx_Cuarzo_Cosentyx" + today + ".csv",
    sep=';', index=None, header=True)

###############################################################################################

list = ['35']
total_df = pd.DataFrame()

for i in list:
    cur = conn.cursor()

    query = '''Select 
    case when ifnull((select if(count(*)>0, 'Time Off', null) from RepUnavailablePeriod rup where rup.repId = r.repId and curdate() between startDate and endDate group by rup.repid),'Working') ='Time Off'
       then 'Time Off'
       else if(suggestions.suggesteddate>0 or ins.published_ins>0,date(now()),'') 
       end as suggestions_insights_received
        ,rgr.rungroupid
        ,concat(r.`seConfigId`,' - ',d.seConfigName) as Config
        -- ,suggestions.runId
        ,rt.repteamname
     		, r.`isActivated`
     		,ifnull((select if(count(*)>0, 'Time Off', null) from RepUnavailablePeriod rup where rup.repId = r.repId and curdate() between startDate and endDate group by rup.repid),'Working') as RepUnavailable
            , r.repname
        -- ,ifnull((select 1 as is_Awareness from novartisesprod_cs.`User` u join novartisesprod.Rep Re on u.Id=Re.externalId join novartisesprod_stage.AKT_Awareness_Reps ar on ar.Division=u.xR1_Division__c where Re.repId = r.repId),0) as is_Awareness
        -- , suggestions.factorname
        -- , suggestions.factortype
        ,suggestions.accountcount as suggested_Accounts_DSE
        ,suggestions.factorcount as suggested_Factors_DSE
        #,sugg.published as suggested_Accounts_published
       ,(select count(distinct etl.accountuid) from DSESuggestionLifecycle dl join ETL_Publish_Veeva_SparkSuggestion3_v etl on etl.suggestionReferenceId =dl.suggestionReferenceId 
    where dl.repId=r.repid and lastPublishRunId in (Select MAX(r.runId) from SparkDSERun r  where date(r.startDateTime) = date(now()) and seconfigId in  ("''' + i + '''") group by r.seConfigId, r.rungroupid having MAX(r.startDateTime)) and externalId is not null and dl.suggestionReferenceId not like '%AKTEI%') as suggested_Accounts_PUB
           ,(select count(*) from DSESuggestionLifecycle dl where dl.repId=r.repid and lastPublishRunId in (Select MAX(r.runId) from SparkDSERun r where date(r.startDateTime) = date(now()) and seconfigId in  ("''' + i + '''") group by r.seConfigId, r.rungroupid having MAX(r.startDateTime)) and externalId is not null and suggestionReferenceId like '%AKTEI%') as insights_all_PUB
               ,ins.published_ins as insights_Accounts_PUB
             #  ,(select count(distinct accountUID) from ETL_Publish_Veeva_SparkEnhancedInsight3_v sugg where sugg.repuId=r.repid and RunId in (Select MAX(r.runId) from SparkDSERun r where date(r.startDateTime) = date(now()) and seconfigId in  ("''' + i + '''") group by r.seConfigId, r.rungroupid having MAX(r.startDateTime))) as insight_ETL
          # ,(select count(distinct etl.accountuid) from DSESuggestionLifecycle dl join ETL_Publish_Veeva_SparkEnhancedInsight3_v etl on etl.suggestionReferenceId =dl.suggestionReferenceId 
    #where dl.repId=r.repid and lastPublishRunId in (Select MAX(r.runId) from SparkDSERun r  where date(r.startDateTime) = date(now()) and seconfigId in  ("''' + i + '''") group by r.seConfigId, r.rungroupid having MAX(r.startDateTime)) and externalId is not null and dl.suggestionReferenceId like '%AKTEI%') as insights_new2
        #,is_targeted.target as targeted_Accounts
        #,instarg.acc as targeted_Accounts_Insights_SFDC
        ,(Select count(*) FROM Account where accountId in (SELECT accountId FROM RepAccountAssignment  where repId = r.repId and endDate > date(now())) and isDeleted = 0) as num_aligned_Accounts
        ,(select count(*) from RepAccountAssignment raa where raa.repId = r.repId and accountId not in (select accountId from RepAccountAssignment raa2 where raa2.repId <> raa.repId)) as non_Shared_Accounts
        ,(select count(distinct accountid) from `StrategyTarget`st join TargetsPeriod tp using(targetsPeriodId) where st.repid=r.repid  and tp.startDate <= curdate() and tp.endDate >= curdate()) as targeted_accounts_strategytarget
        #,(select count(distinct accountid) from `AccountProduct`ap where enumIsHCPTargetted_akt='yes' and 
        #accountId in (SELECT accountId FROM RepAccountAssignment  where repId = r.repId and endDate > date(now())) and r.isDeleted = 0) and
        #productId in (Select p.productid from Product p where p.externalid in (Select t.product from novartisesprod_stage.AKT_PFIZERUK_TEAMS t where t.cms_Salesforce__c in (Select t.cms_Salesforce__c from novartisesprod_cs.User u where u.id=r.externalid and r.rep)))) as targeted_accounts
     #   ,(Select count(distinct accountid) from Account a where hcpEmailConsent_akt = 'yes' and accountId in (SELECT accountId FROM RepAccountAssignment  where repId = r.repId)) as consented_Accounts
        ,(Select Count(distinct accountid) from Account a where enumIsPersonAccount_akt='yes' and accountId in (SELECT accountId FROM RepAccountAssignment  where repId = r.repId)) as IsPersonAccount
        #,(Select Count(distinct accountid) from AccountProduct a where enumSegment_akt not in ('N/A') and accountId in (SELECT accountId FROM RepAccountAssignment where repId = r.repId)) as SegmentedAccounts
        #,(select max(suggestedDate) from SparkDSERunRepDate rrd where rrd.repid=r.repid) as last_suggestion
       # ,(SELECT count(*) FROM Interaction where repId = r.repId and isDeleted = 0 and interactionId in
       #     (SELECT interactionId FROM InteractionAccount where accountId in
        #    (SELECT accountId FROM RepAccountAssignment  where repId = r.repId and endDate > date(now())) and isDeleted = 0))
        #    as numfutureInteractions
        #,(SELECT count(*) FROM Event where
        #    productId in (SELECT productId FROM Product where productId in (SELECT productId FROM RepProductAuthorization where repId = r.repId) and isActive = 1 and isDeleted = 0)
        #    and accountId in (Select accountId FROM Account where accountId in (SELECT accountId FROM RepAccountAssignment  where repId = r.repId and endDate > date(now())) and isDeleted = 0)
        #    and eventdate<=date(now()))
         #   as numpastEvents
       # ,(SELECT count(*) FROM Event where
      #      productId in (SELECT productId FROM Product where productId in (SELECT productId FROM RepProductAuthorization where repId = r.repId) and isActive = 1 and isDeleted = 0)
        #    and accountId in (Select accountId FROM Account where accountId in (SELECT accountId FROM RepAccountAssignment  where repId = r.repId and endDate > date(now())) and isDeleted = 0)
          #  and eventdate>date(now()))
          #  as numfutureEvents
        ,(select max(syncTime) as maxsync from novartisesprod_stage.VeevaSync vs where vs.repUID = r.externalId) as last_veevaSync
        ,da.daily_avr_sync_last_week
       # ,(select `Last_iPad_iOS_Version_vod__c`from novartisesprod_cs.User u where u.id=r.externalid) as Last_iPad_iOS_Version
      #  ,(select GROUP_CONCAT(p.productName SEPARATOR ', ') from RepProductAuthorization rpa join Product p on rpa.productId = p.productId where rpa.repId = r.repId)
       #     as repProducts_Authorization_DSE
         ,(select GROUP_CONCAT(distinct p.Name SEPARATOR ', ') from 
        novartisesprod_cs.My_Setup_Products_vod__c ss  
        join novartisesprod.Rep Re on ss.OwnerId=Re.externalId 
        join novartisesprod_cs.Product_vod__c p on ss.Product_vod__c=p.id 
        where 
         Re.repId = r.repId) as Rep_prods_cs
        from Rep r
        join RepTeamRep rtr on rtr.repid=r.repid
        join RepTeam rt on rtr.repteamid=rt.repteamid
        join DSEConfigRunGroupRep rgr on rgr.repid=r.repid
        join DSEConfig d on d.seConfigId =r.seConfigId 
            left join
        (SELECT distinct
            r.runId,
            rrd.suggesteddate,
            rr.externalid,
            -- rcf.factorname,
            -- rcf.factorType,
            #rrda.suggestedRepActionTypeUID as Channeltype, 
            concat(count(distinct rrda.accountUID),' (SENT ',if(rrda.suggestedRepActionTypeUID='SEND_ANY',COUNT(distinct rrda.accountUID),'0'),')') AS accountcount,
    count(distinct rrda.runRepDateSuggestionId) as nrsugg,
            #count(distinct rr.repid) as repcount
            #, 
            count(distinct rcf.factorname) as factorcount
        FROM
            SparkDSERun r
                JOIN
            SparkDSERunRepDate rrd USING (runUID)
                JOIN
            SparkDSERunRepDateSuggestion rrda USING (runRepDateId)
                LEFT JOIN
            SparkDSERunRepDateSuggestionReason rrdar USING (runRepDateSuggestionId)
                INNER JOIN
            SparkDSERunConfigFactor rcf ON rcf.factorUID = rrdar.factorUID
                JOIN
            Account a USING (accountId)
                JOIN
            Rep rr ON (rr.repId = rrd.repId)
        WHERE r.runId in (Select MAX(r.runId) from SparkDSERun r where date(r.startDateTime) = date(now()) and seconfigId in ("''' + i + '''") group by r.seConfigId, r.rungroupid having MAX(r.startDateTime))
        #WHERE r.runId = ('4948')
        and rcf.factorname <> 'DSE Internal factor for constructing content for SUGGESTED' -- header of suggestions
        group by rrd.suggesteddate,rr.externalid#,rr.repname
        #, rrda.suggestedRepActionTypeUID
        ORDER BY runid DESC) as suggestions on suggestions.externalid=r.externalid
        left join 
        (
        SELECT
         rl.externalid, rl.repname, count(distinct Suggestion_vod__c.account_vod__c) as published
        FROM novartisesprod_cs.Suggestion_vod__c
        #join novartisesprod_cs.User u on u.id=OwnerId
        join novartisesprod_stage.AKT_RepLicense rl on rl.externalid=OwnerId
        WHERE 1=1
        and OwnerId in (select externalId from Rep where repId in (select repId from DSEConfigRunGroupRep where seconfigid in ("''' + i + '''") group by repid))
        and Call_Objective_Record_Type_vod__c = 'Suggestion_vod' -- Enhanced Inishts this chande
        and Expiration_date_vod__c > now()
        group by rl.repname) as sugg on sugg.externalid=r.externalid
        left join 
        (
        SELECT
         rl.externalid, rl.repname, count(distinct Suggestion_vod__c.account_vod__c) as published_ins
        FROM novartisesprod_cs.Suggestion_vod__c
        #join novartisesprod_cs.User u on u.id=OwnerId
        join novartisesprod_stage.AKT_RepLicense rl on rl.externalid=OwnerId
        WHERE 1=1
        and OwnerId in (select externalId from Rep where repId in (select repId from DSEConfigRunGroupRep where seconfigid in ("''' + i + '''") group by repid))
        and Call_Objective_Record_Type_vod__c = 'Insight_vod' -- Enhanced Inishts this chande
        and Expiration_date_vod__c > now()
        group by rl.repname) as ins on ins.externalid=r.externalid
        left join (select SYNC.repId, concat(round(SYNC.count_sync/(SYNC.day_ww-SYNC.days_off),2),' (working days considered for the average syncing: ',(SYNC.day_ww-SYNC.days_off),')')as daily_avr_sync_last_week from
        (select TOT.count_sync, TOT.repId, TOT.start_date, TOT.end_date
        ,ifnull(case when TOT.start_date=TOT.end_date then '1'
        when TOT.end_date=CURDATE() THEN 5 * (DATEDIFF(TOT.end_date, TOT.start_date) DIV 7) + MID('0123444401233334012222340111123400012345001234550', 7 * WEEKDAY(TOT.start_date) + WEEKDAY(TOT.end_date) + 1, 1) 
        ELSE 5 * (DATEDIFF(TOT.end_date, TOT.start_date) DIV 7) + MID('0123444401233334012222340111123400012345001234550', 7 * WEEKDAY(TOT.start_date) + WEEKDAY(TOT.end_date) + 1, 1) + 1
        end,'0') as days_off
        , 5 * (DATEDIFF(CURDATE() , CURDATE() - interval 7 day) DIV 7) + MID('0123444401233334012222340111123400012345001234550', 7 * WEEKDAY(CURDATE()) + WEEKDAY(CURDATE() - interval 7 day) + 1, 1) 
        as day_ww
        ,CURDATE() - interval 7 day
        ,CURDATE()
        FROM
        (SELECT Rr.repId,count(vs.syncTime) as count_sync
        ,max(B.start_date)
        ,max(B.end_date)
        ,CASE WHEN max(B.start_date)>CURDATE() - interval 7 day THEN max(B.start_date)
        	  when max(B.start_date)<=CURDATE() - interval 7 day THEN CURDATE() - interval 7 day
        	  else NULL end as start_date
        ,CASE WHEN max(B.end_date)<CURDATE() then max(B.end_date)
        	  when max(B.end_date)>=CURDATE() THEN CURDATE() 
        	  else NULL end as end_date
        from novartisesprod_stage.VeevaSync vs
        join novartisesprod.Rep Rr on vs.repUID=Rr.externalId 
        left join
        (select A.repId, min(A.startDate) as start_date, max(A.endDate) as end_date from
        (select Re.repId, run.startDate,run.endDate
        from RepUnavailablePeriod run
        join novartisesprod.Rep Re on Re.repId =run.repId 
        where  1=1
        -- and Re.repId='1496'
        and (((run.startDate BETWEEN CURDATE() - interval 7 day and CURDATE()) and  run.endDate>CURDATE())  -- W ,AW
        or ((run.startDate < CURDATE() - interval 7 day) and run.endDate>CURDATE())                    -- BW  AW
        or ((run.startDate < CURDATE() - interval 7 day) and run.endDate BETWEEN CURDATE() - interval 7 day and CURDATE()) -- BW W
        or ((run.startDate BETWEEN CURDATE() - interval 7 day and CURDATE()) and  run.endDate BETWEEN CURDATE() - interval 7 day and CURDATE())) -- W W
        ) as A
        group by A.repId) as B
        on Rr.repId=B.repId 
        where 
        DAYOFWEEK(SyncTime) NOT IN ('7', '1') 
        and (SyncTime BETWEEN CURDATE() - interval 7 day and CURDATE())
        group by Rr.repId) as TOT) AS SYNC) as da on da.repId=r.repId
        where r.seconfigid in ("''' + i + '''")
        order by 
         rt.repteamname,rgr.rungroupid;'''

    print(query)

    cur.execute(query)

    rows = cur.fetchall()

    df = pd.DataFrame(rows)
    cur.close()

    print(df)
    column_names = [i[0] for i in cur.description]
    print(column_names)

    total_df = total_df.append(df, ignore_index=True)

    print(total_df)

    print(i)

total_df.columns = column_names
total_df['suggested_Accounts_DSE'] = total_df['suggested_Accounts_DSE'].str.decode('ASCII')

export_csv = total_df.to_csv(
    "/Users/SergioPina/DesktopReport_novartisSP_MarketAccess" + today + ".csv",
    sep=';', index=None, header=True)

conn.close()
tunnel.close()
