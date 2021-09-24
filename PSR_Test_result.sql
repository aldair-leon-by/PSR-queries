/*

Author: ALDAIR 
SEP 7TH, 2021

 */


/*INGEST SERVICE*/

DECLARE @created_at datetime2;
DECLARE @created_at_end datetime2;

SET @created_at = '2021-08-20 14:10:00'
SET @created_at_end = '2021-08-20 15:00:00'


SELECT
    MSG_EVNT.MSG_EVNT_ID,
    MSG_EVNT.MSG_HDR_ID,
    MSG_EVNT.STATUS,
    MSG_EVNT.CRTD_AT,
    MSG_HDR.LST_UPDT_AT,
    MSG_HDR.DOC_TMSTMP ,
    MSG_HDR.CRNT_STATUS,
    MSG_HDR.END_PNT_TYPE,
    MSG_HDR.END_PNT_NAME,
    MSG_HDR.MDL_TYPE,
    MSG_HDR.MSG_TYPE,
    MSG_HDR.MSG_ID,
    MSG_HDR.MSG_SNDR,
    MSG_HDR.MSG_RCVRS,
    FORMAT( (MSG_HDR.LST_UPDT_AT) -  (MSG_EVNT.CRTD_AT), 'mm:ss.ff') AS TOTAL_TIME
FROM
    CONNECT_MS.MS_MSG_EVNT AS MSG_EVNT JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR ON MSG_EVNT.MSG_HDR_ID = MSG_HDR.MSG_HDR_ID
        AND MSG_EVNT.STATUS = 'Received ' AND MSG_HDR.MDL_TYPE LIKE '%BYDM%'
WHERE MSG_EVNT.CRTD_AT > @created_at and MSG_EVNT.CRTD_AT < @created_at_end
order by MSG_EVNT.CRTD_AT DESC
;

DECLARE @created_at datetime2;
DECLARE @created_at_end datetime2;

SET @created_at = '2021-08-20 14:10:00'
SET @created_at_end = '2021-08-20 15:00:00'


SELECT
    COUNT(MSG_HDR.MSG_TYPE) AS NUMBER_PROCESS_MESSAGES,
    MSG_HDR.MSG_TYPE,
    MIN(MSG_EVNT.CRTD_AT) AS 'Ingestion Service started',
    MAX(MSG_HDR.LST_UPDT_AT) AS 'Ingestion Service finished',
    FORMAT(MAX(MSG_HDR.LST_UPDT_AT) -  MIN(MSG_EVNT.CRTD_AT), 'mm:ss.ff') AS TOTAL_TIME
FROM CONNECT_MS.MS_MSG_EVNT AS MSG_EVNT JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR ON MSG_EVNT.MSG_HDR_ID = MSG_HDR.MSG_HDR_ID
WHERE MSG_EVNT.CRTD_AT >  @created_at and MSG_EVNT.CRTD_AT < @created_at_end AND MSG_EVNT.STATUS = 'Received' AND MSG_HDR.MDL_TYPE LIKE '%BYDM%'
group by MSG_HDR.MSG_TYPE,MSG_EVNT.STATUS
;

DECLARE @created_at datetime2;
DECLARE @created_at_end datetime2;

SET @created_at = '2021-08-20 14:10:00'
SET @created_at_end = '2021-08-20 15:00:00'


SELECT
    MSG_HDR.MSG_TYPE,
    MSG_HDR.CRNT_STATUS AS 'Ingestion Service Status',
    MSG_EVNT.CRTD_AT AS 'Ingest Service started',
    MSG_HDR.LST_UPDT_AT AS 'Ingest Service finished',
    MSG_HDR.MDL_TYPE AS 'Format File',
    MSG_HDR.MSG_ID,
    FORMAT( (MSG_HDR.LST_UPDT_AT) -  (MSG_EVNT.CRTD_AT), 'mm:ss.ff') AS TOTAL_TIME
FROM CONNECT_MS.MS_MSG_EVNT AS MSG_EVNT JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR ON MSG_EVNT.MSG_HDR_ID = MSG_HDR.MSG_HDR_ID
        AND MSG_EVNT.STATUS = 'Received ' AND MSG_HDR.MDL_TYPE LIKE '%BYDM%'
WHERE MSG_EVNT.CRTD_AT > @created_at and MSG_EVNT.CRTD_AT < @created_at_end
order by MSG_EVNT.CRTD_AT
;


-- /*MESSAGE BROKER*/

/*Total time*/

SELECT
    COUNT(MSG_HDR.MSG_TYPE)/2 AS 'Total of Message',
    MSG_HDR.MSG_TYPE,
    MIN(BULK_EVENT.CRTD_AT) AS 'Transform Started',
    MAX(BULK_EVENT.CRTD_AT) AS 'Transform Finished',
    FORMAT(MAX(BULK_EVENT.CRTD_AT) -  MIN(BULK_EVENT.CRTD_AT), 'HH:mm.ss.ff') AS TOTAL_TIME
FROM CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT JOIN CONNECT_MS.MS_BLK_HDR AS BLK_HDR ON  BULK_EVENT.BLK_HDR_ID =  BLK_HDR.BLK_HDR_ID JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR ON BLK_HDR.BLK_SRC_ID LIKE CONCAT('%', MSG_HDR.MSG_ID,'%')
WHERE BULK_EVENT.CRTD_AT > '2021-08-20 14:10:00' and BULK_EVENT.CRTD_AT <  '2021-08-20 15:00:00' AND BULK_EVENT.STATUS != 'Processed'
GROUP BY MSG_HDR.MSG_TYPE
ORDER BY  COUNT(MSG_HDR.MSG_TYPE)/2 ASC;


SELECT
    MSG_HDR.MSG_TYPE,
    BULK_EVENT.BLK_HDR_ID,
    MIN(BULK_EVENT.CRTD_AT) AS 'Transform Started',
    MAX(BULK_EVENT.CRTD_AT) AS 'Transform Finished',
    BLK_HDR.BLK_ID AS 'MSG_IG',
    FORMAT(MAX(BULK_EVENT.CRTD_AT) -  MIN(BULK_EVENT.CRTD_AT), 'HH:mm.ss.ff') AS TOTAL_TIME
FROM CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT JOIN CONNECT_MS.MS_BLK_HDR AS BLK_HDR ON  BULK_EVENT.BLK_HDR_ID =  BLK_HDR.BLK_HDR_ID JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR ON BLK_HDR.BLK_SRC_ID LIKE CONCAT('%', MSG_HDR.MSG_ID,'%')
WHERE BULK_EVENT.CRTD_AT > '2021-08-20 14:10:00' and BULK_EVENT.CRTD_AT <  '2021-08-20 15:00:00' AND BULK_EVENT.STATUS != 'Processed'
GROUP BY BULK_EVENT.BLK_HDR_ID,BLK_HDR.BLK_ID,MSG_HDR.MSG_TYPE
ORDER BY BULK_EVENT.BLK_HDR_ID;



SELECT
    BULK_EVENT.BLK_EVNT_ID,
    BULK_EVENT.BLK_HDR_ID,
    BULK_EVENT.STATUS,
    BULK_EVENT.CRTD_AT,
    BULK_EVENT.BTCH_TASK,
    BLK_HDR.BLK_HDR_ID,
    BLK_HDR.CRTD_AT,
    BLK_HDR.LST_UPDT_AT,
    BLK_HDR.CRNT_STATUS,
    BLK_HDR.BLK_ID,
    BLK_HDR.BLK_TYPE,
    BLK_HDR.BLK_LOC
FROM CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT JOIN CONNECT_MS.MS_BLK_HDR AS BLK_HDR ON  BULK_EVENT.BLK_HDR_ID =  BLK_HDR.BLK_HDR_ID
WHERE BULK_EVENT.CRTD_AT > '2021-08-20 14:19:00' and BULK_EVENT.CRTD_AT <  '2021-08-20 15:00:00'
order by BULK_EVENT.CRTD_AT ASC;



/*INGEST SERVICE AND MESSAGE BROKER time of execution*/


DECLARE @created_at datetime2;
DECLARE @created_at_end datetime2;

SET @created_at = '2021-08-20 14:10:00'
SET @created_at_end = '2021-08-20 15:00:00'

;WITH
    ingestService_to_MessageBroker
    as
    (
        SELECT MSG_HDR.MSG_TYPE AS 'Type of message',
            MSG_EVNT.CRTD_AT AS 'Ingestion Service Message started',
            MSG_HDR.LST_UPDT_AT AS 'Ingestion Service Message finished',
            BLK_HDR.BLK_HDR_ID,
            MSG_HDR.MSG_ID,
            BLK_HDR.BLK_ID
        FROM CONNECT_MS.MS_MSG_EVNT AS MSG_EVNT
            JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR
            ON MSG_EVNT.MSG_HDR_ID = MSG_HDR.MSG_HDR_ID
            JOIN CONNECT_MS.MS_BLK_HDR AS BLK_HDR
            ON BLK_HDR.BLK_ID LIKE CONCAT('%', MSG_HDR.MSG_ID,'%')
                AND MSG_EVNT .STATUS = 'Received '
                AND MSG_HDR.MDL_TYPE LIKE '%BYDM%'
        WHERE MSG_EVNT.CRTD_AT > @created_at and MSG_EVNT.CRTD_AT <  @created_at_end
    )
SELECT
    ingestService_to_MessageBroker.BLK_HDR_ID,
    ingestService_to_MessageBroker.[Type of message],
    ingestService_to_MessageBroker.MSG_ID AS 'Ingest Service MSG_ID',
    ingestService_to_MessageBroker.[Ingestion Service Message started],
    ingestService_to_MessageBroker.[Ingestion Service Message finished],
    ingestService_to_MessageBroker.BLK_ID AS 'Message Broker BLK_ID',
    MIN(BULK_EVENT.CRTD_AT) AS 'Message broker started',
    MAX(BULK_EVENT.CRTD_AT) AS 'Message broker finished',
    FORMAT(MAX(BULK_EVENT.CRTD_AT) - MIN(ingestService_to_MessageBroker.[Ingestion Service Message started]), 'HH:mm.ss.ff') AS TOTAL_TIME
FROM ingestService_to_MessageBroker JOIN CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT ON ingestService_to_MessageBroker.BLK_HDR_ID = BULK_EVENT.BLK_HDR_ID
WHERE  BULK_EVENT.STATUS != 'Processed'
GROUP BY 
    BULK_EVENT.BLK_HDR_ID,
    ingestService_to_MessageBroker.BLK_HDR_ID,
    ingestService_to_MessageBroker.[Type of message],
    ingestService_to_MessageBroker.[Ingestion Service Message started],
    ingestService_to_MessageBroker.[Ingestion Service Message finished],
    ingestService_to_MessageBroker.MSG_ID,ingestService_to_MessageBroker.BLK_ID;




DECLARE @created_at datetime2;
DECLARE @created_at_end datetime2;

SET @created_at = '2021-08-20 14:10:00'
SET @created_at_end = '2021-08-20 15:00:00'


;WITH
    ingestService_to_MessageBroker_Total
    as
    (
        SELECT MSG_HDR.MSG_TYPE AS 'Type of message',
            MSG_EVNT.CRTD_AT AS 'Ingestion Service Message started',
            MSG_HDR.LST_UPDT_AT AS 'Ingestion Service Message finished' ,
            BLK_HDR.BLK_HDR_ID
        FROM CONNECT_MS.MS_MSG_EVNT AS MSG_EVNT
            JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR
            ON MSG_EVNT.MSG_HDR_ID = MSG_HDR.MSG_HDR_ID
            JOIN CONNECT_MS.MS_BLK_HDR AS BLK_HDR
            ON BLK_HDR.BLK_ID LIKE CONCAT('%', MSG_HDR.MSG_ID,'%')
                AND MSG_EVNT .STATUS = 'Received '
                AND MSG_HDR.MDL_TYPE LIKE '%BYDM%'
        WHERE MSG_EVNT.CRTD_AT > @created_at and MSG_EVNT.CRTD_AT < @created_at_end
    )
SELECT
    COUNT(ingestService_to_MessageBroker_Total.[Type of message])/2 AS 'Total of Messages',
    ingestService_to_MessageBroker_Total.[Type of message],
    MIN(ingestService_to_MessageBroker_Total.[Ingestion Service Message started]) AS 'Ingestion Service Message started',
    MAX(ingestService_to_MessageBroker_Total.[Ingestion Service Message finished]) AS 'Ingestion Service Message finished',
    MIN(BULK_EVENT.CRTD_AT) AS 'Message broker started',
    MAX(BULK_EVENT.CRTD_AT) AS 'Message broker finished',
    FORMAT(MAX(BULK_EVENT.CRTD_AT) - MIN(ingestService_to_MessageBroker_Total.[Ingestion Service Message started]), 'HH:mm.ss.ff') AS TOTAL_TIME
FROM ingestService_to_MessageBroker_Total JOIN CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT ON ingestService_to_MessageBroker_Total.BLK_HDR_ID = BULK_EVENT.BLK_HDR_ID
WHERE  BULK_EVENT.STATUS != 'Processed'
GROUP BY ingestService_to_MessageBroker_Total.[Type of message]
ORDER BY COUNT(ingestService_to_MessageBroker_Total.[Type of message])/2;




/*LCT ADAPTER*/

/*Extract data with messageId comming from message_storage table*/

DECLARE @created_at datetime2;
DECLARE @created_at_end datetime2;

SET @created_at = '2021-08-20 14:10:00'
SET @created_at_end = '2021-08-20 15:00:00'


SELECT
    DATA_TBL.MSG_HDR_ID,
    DATA_TBL.MSG_STORE_REF_ID,
    DATA_TBL.MSG_STATUS,
    DATA_TBL.GS1_HEADER,
    DATA_TBL.CREATED_DATETIME,
    DATA_TBL.MODIFIED_DATETIME,
    AUDIT_TBL.MSG_TYPE,
    AUDIT_TBL.MSG_INGEST_PARAM,
    AUDIT_TBL.GROUPED_BY,
    AUDIT_TBL.INGESTION_ID,
    AUDIT_TBL.INGEST_STATUS_MSG,
    AUDIT_TBL.CREATED_DATETIME,
    AUDIT_TBL.MODIFIED_DATETIME,
    FORMAT(AUDIT_TBL.MODIFIED_DATETIME -  AUDIT_TBL.CREATED_DATETIME, 'HH:mm.ss.fff') AS TIME_DIFF
FROM dbo.LCTA_INBOUND_DATA_TBL AS DATA_TBL JOIN dbo.LCTA_MSG_AUDIT_TBL AS AUDIT_TBL ON AUDIT_TBL.MSG_HDR_REF_ID = DATA_TBL.MSG_HDR_ID
WHERE DATA_TBL.CREATED_DATETIME  > @created_at and DATA_TBL.CREATED_DATETIME < @created_at_end;


SELECT
    AUDIT_TBL.MSG_TYPE,
    AUDIT_TBL.CREATED_DATETIME,
    AUDIT_TBL.MODIFIED_DATETIME,
    FORMAT(AUDIT_TBL.MODIFIED_DATETIME -  AUDIT_TBL.CREATED_DATETIME, 'HH:mm.ss.fff') AS TIME_DIFF
FROM dbo.LCTA_INBOUND_DATA_TBL AS DATA_TBL JOIN dbo.LCTA_MSG_AUDIT_TBL AS AUDIT_TBL ON AUDIT_TBL.MSG_HDR_REF_ID = DATA_TBL.MSG_HDR_ID
WHERE DATA_TBL.CREATED_DATETIME  > @created_at and DATA_TBL.CREATED_DATETIME < @created_at_end;


SELECT
    'LCTA_MSG_AUDIT_TBL',
    count(*),
    min(CREATED_DATETIME) MIN_CREATED_DATETIME,
    max(CREATED_DATETIME) MAX_CREATED_DATETIME,
    min(MODIFIED_DATETIME) MIN_MODIFIED_DATETIME,
    max(MODIFIED_DATETIME) MAX_MODIFIED_DATETIME,
    MSG_STATUS, MSG_TYPE, FORMAT(max(MODIFIED_DATETIME) - min(CREATED_DATETIME),'HH:mm.ss.ff') AS TOTAL_TIME
FROM LCTA_MSG_AUDIT_TBL  with (nolock)
WHERE CREATED_DATETIME  > @created_at AND CREATED_DATETIME < @created_at_end
GROUP BY MSG_STATUS,msg_type;



SELECT
    count(MSG_STATUS) 'Number of messages',
    MSG_TYPE 'Type of messages',
    MSG_STATUS,
    min(CREATED_DATETIME) 'LCT Adapter Started',
    max(MODIFIED_DATETIME) 'LCT Adapter Finished',
    FORMAT(max(MODIFIED_DATETIME) - min(CREATED_DATETIME),'HH:mm.ss.ff') AS TOTAL_TIME
FROM LCTA_MSG_AUDIT_TBL  with (nolock)
WHERE CREATED_DATETIME  > @created_at AND CREATED_DATETIME < @created_at_end
GROUP BY MSG_STATUS,msg_type;


/*JSON API Message*/

DECLARE @created_at datetime2;
DECLARE @created_at_end datetime2;

SET @created_at = '2021-08-20 14:10:00'
SET @created_at_end = '2021-08-20 15:00:00'



SELECT
    AUDIT_TBL.GROUPED_BY,
    AUDIT_TBL.INGEST_STATUS_MSG,
    AUDIT_TBL.CREATED_DATETIME AS 'Ingestion Started SQL',
    CONVERT(datetime2(3), SUBSTRING(JSON_VALUE(value,'$.eventTime'),0,24)) AS 'API Ingestion Finished Time',
    AUDIT_TBL.MODIFIED_DATETIME AS 'Ingestion Finished SQL',
    FORMAT(CONVERT(datetime, SUBSTRING(JSON_VALUE(value,'$.eventTime'),0,24)) - AUDIT_TBL.CREATED_DATETIME , 'HH:mm.ss.ff') AS 'TOTAL TIME API VS SQL',
    FORMAT(AUDIT_TBL.MODIFIED_DATETIME -AUDIT_TBL.CREATED_DATETIME , 'HH:mm.ss.ff' ) AS 'TOTAL TIME SQL'
FROM  LCTA_MSG_AUDIT_TBL AS AUDIT_TBL CROSS APPLY OPENJSON(AUDIT_TBL.INGEST_STATUS_MSG, '$') 

WHERE AUDIT_TBL.CREATED_DATETIME  > @created_at AND AUDIT_TBL.CREATED_DATETIME < @created_at_end 
AND JSON_VALUE(value,'$.status') = 'COMPLETED' AND NOT JSON_VALUE(value,'$.service') = 'computation';

SELECT
    AUDIT_TBL.GROUPED_BY,
    MIN(AUDIT_TBL.CREATED_DATETIME) AS 'Ingestion Started SQL',
    MAX(CONVERT(datetime2(3), SUBSTRING(JSON_VALUE(value,'$.eventTime'),0,24))) AS 'API Ingestion Finished Time',
    MAX(AUDIT_TBL.MODIFIED_DATETIME) AS 'Ingestion Finished SQL',
    FORMAT(MAX(CONVERT(datetime, SUBSTRING(JSON_VALUE(value,'$.eventTime'),0,24))) - MIN(AUDIT_TBL.CREATED_DATETIME) , 'HH:mm.ss.ff') AS 'TOTAL TIME API VS SQL',
    FORMAT(MAX(AUDIT_TBL.MODIFIED_DATETIME) -MIN(AUDIT_TBL.CREATED_DATETIME) , 'HH:mm.ss.ff' ) AS 'TOTAL TIME SQL'
FROM  LCTA_MSG_AUDIT_TBL AS AUDIT_TBL CROSS APPLY OPENJSON(AUDIT_TBL.INGEST_STATUS_MSG, '$') 

WHERE AUDIT_TBL.CREATED_DATETIME  > @created_at AND AUDIT_TBL.CREATED_DATETIME < @created_at_end 
AND JSON_VALUE(value,'$.status') = 'COMPLETED' AND NOT JSON_VALUE(value,'$.service') = 'computation'
GROUP BY  AUDIT_TBL.GROUPED_BY;


