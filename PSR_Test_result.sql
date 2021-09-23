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

SELECT
    COUNT(MSG_HDR.MSG_TYPE) AS NUMBER_PROCESS_MESSAGES,
    MSG_HDR.MSG_TYPE, MIN(MSG_EVNT.CRTD_AT) AS FIRST_CREATED,
    MIN(MSG_HDR.LST_UPDT_AT) AS FIRST_FINISHED, MAX(MSG_EVNT.CRTD_AT) AS LAST_CREATED,
    MAX(MSG_HDR.LST_UPDT_AT) AS LAST_FINISHED,
    FORMAT(MAX(MSG_HDR.LST_UPDT_AT) -  MIN(MSG_EVNT.CRTD_AT), 'mm:ss.ff') AS TOTAL_TIME
FROM CONNECT_MS.MS_MSG_EVNT AS MSG_EVNT JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR ON MSG_EVNT.MSG_HDR_ID = MSG_HDR.MSG_HDR_ID
WHERE MSG_EVNT.CRTD_AT >  @created_at and MSG_EVNT.CRTD_AT < @created_at_end AND MSG_EVNT.STATUS = 'Received' AND MSG_HDR.MDL_TYPE LIKE '%BYDM%'
group by MSG_HDR.MSG_TYPE,MSG_EVNT.STATUS
;

SELECT
    MSG_EVNT.CRTD_AT,
    MSG_HDR.LST_UPDT_AT,
    MSG_HDR.MSG_TYPE,
    FORMAT( (MSG_HDR.LST_UPDT_AT) -  (MSG_EVNT.CRTD_AT), 'mm:ss.ff') AS TOTAL_TIME
FROM CONNECT_MS.MS_MSG_EVNT AS MSG_EVNT JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR ON MSG_EVNT.MSG_HDR_ID = MSG_HDR.MSG_HDR_ID
        AND MSG_EVNT.STATUS = 'Received ' AND MSG_HDR.MDL_TYPE LIKE '%BYDM%'
WHERE MSG_EVNT.CRTD_AT > @created_at and MSG_EVNT.CRTD_AT < @created_at_end
order by MSG_EVNT.CRTD_AT
;


-- /*MESSAGE BROKER*/

/*Total time*/

DECLARE @created_at datetime2;
DECLARE @created_at_end datetime2;

SET @created_at = '2021-08-20 14:10:00'
SET @created_at_end = '2021-08-20 15:00:00'


SELECT
    COUNT(MSG_HDR.MSG_TYPE)/2,
    MSG_HDR.MSG_TYPE,
    MIN(BULK_EVENT.CRTD_AT) AS 'Transform Started',
    MAX(BULK_EVENT.CRTD_AT) AS 'Transform Finished',
    FORMAT(MAX(BULK_EVENT.CRTD_AT) -  MIN(BULK_EVENT.CRTD_AT), 'HH:mm.ss.ff') AS TOTAL_TIME
FROM CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT JOIN CONNECT_MS.MS_BLK_HDR AS BLK_HDR ON  BULK_EVENT.BLK_HDR_ID =  BLK_HDR.BLK_HDR_ID JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR ON BLK_HDR.BLK_SRC_ID LIKE CONCAT('%', MSG_HDR.MSG_ID,'%')
WHERE BULK_EVENT.CRTD_AT > @created_at and BULK_EVENT.CRTD_AT <  @created_at_end AND BULK_EVENT.STATUS != 'Processed'
GROUP BY MSG_HDR.MSG_TYPE;


SELECT *
FROM CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT JOIN CONNECT_MS.MS_BLK_HDR AS BLK_HDR ON  BULK_EVENT.BLK_HDR_ID =  BLK_HDR.BLK_HDR_ID
WHERE BULK_EVENT.CRTD_AT > @created_at and BULK_EVENT.CRTD_AT < @created_at_end

SELECT
    MSG_HDR.MSG_TYPE,
    BULK_EVENT.BLK_HDR_ID,
    MIN(BULK_EVENT.CRTD_AT) AS 'Transform Started',
    MAX(BULK_EVENT.CRTD_AT) AS 'Transform Finished',
    BLK_HDR.BLK_ID, FORMAT(MAX(BULK_EVENT.CRTD_AT) -  MIN(BULK_EVENT.CRTD_AT), 'HH:mm.ss.ff') AS TOTAL_TIME
FROM CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT JOIN CONNECT_MS.MS_BLK_HDR AS BLK_HDR ON  BULK_EVENT.BLK_HDR_ID =  BLK_HDR.BLK_HDR_ID JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR ON BLK_HDR.BLK_SRC_ID LIKE CONCAT('%', MSG_HDR.MSG_ID,'%')
WHERE BULK_EVENT.CRTD_AT > @created_at and BULK_EVENT.CRTD_AT < @created_at_end AND BULK_EVENT.STATUS != 'Processed'
GROUP BY BULK_EVENT.BLK_HDR_ID,BLK_HDR.BLK_ID,MSG_HDR.MSG_TYPE;


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
            BLK_HDR.BLK_HDR_ID
        FROM CONNECT_MS.MS_MSG_EVNT AS MSG_EVNT
            JOIN CONNECT_MS.MS_MSG_HDR AS MSG_HDR
            ON MSG_EVNT.MSG_HDR_ID = MSG_HDR.MSG_HDR_ID
            JOIN CONNECT_MS.MS_BLK_HDR AS BLK_HDR
            ON BLK_HDR.BLK_ID LIKE CONCAT('%', MSG_HDR.MSG_ID,'%')
                AND MSG_EVNT .STATUS = 'Received '
                AND MSG_HDR.MDL_TYPE LIKE '%BYDM%'
        WHERE MSG_EVNT.CRTD_AT > @created_at and MSG_EVNT.CRTD_AT <  @created_at_end
    )
SELECT ingestService_to_MessageBroker.BLK_HDR_ID,
    ingestService_to_MessageBroker.[Type of message],
    ingestService_to_MessageBroker.[Ingestion Service Message started],
    ingestService_to_MessageBroker.[Ingestion Service Message finished],
    MIN(BULK_EVENT.CRTD_AT) AS 'Message broker started',
    MAX(BULK_EVENT.CRTD_AT) AS 'Message broker finished',
    FORMAT(MAX(BULK_EVENT.CRTD_AT) - MIN(ingestService_to_MessageBroker.[Ingestion Service Message started]), 'HH:mm.ss.ff') AS TOTAL_TIME
FROM ingestService_to_MessageBroker JOIN CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT ON ingestService_to_MessageBroker.BLK_HDR_ID = BULK_EVENT.BLK_HDR_ID
WHERE  BULK_EVENT.STATUS != 'Processed'
GROUP BY BULK_EVENT.BLK_HDR_ID,ingestService_to_MessageBroker.BLK_HDR_ID,ingestService_to_MessageBroker.[Type of message],ingestService_to_MessageBroker.[Ingestion Service Message started],ingestService_to_MessageBroker.[Ingestion Service Message finished];



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
SELECT COUNT(ingestService_to_MessageBroker_Total.[Type of message])/2 AS 'Total of Messages', ingestService_to_MessageBroker_Total.[Type of message], MIN(ingestService_to_MessageBroker_Total.[Ingestion Service Message started]) AS 'Ingestion Service Message started', MAX(ingestService_to_MessageBroker_Total.[Ingestion Service Message finished]) AS 'Ingestion Service Message finished',
    MIN(BULK_EVENT.CRTD_AT) AS 'Message broker started',
    MAX(BULK_EVENT.CRTD_AT) AS 'Message broker finished',
    FORMAT(MAX(BULK_EVENT.CRTD_AT) - MIN(ingestService_to_MessageBroker_Total.[Ingestion Service Message started]), 'HH:mm.ss.ff') AS TOTAL_TIME
FROM ingestService_to_MessageBroker_Total JOIN CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT ON ingestService_to_MessageBroker_Total.BLK_HDR_ID = BULK_EVENT.BLK_HDR_ID
WHERE  BULK_EVENT.STATUS != 'Processed'
GROUP BY ingestService_to_MessageBroker_Total.[Type of message];




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
    FORMAT(AUDIT_TBL.MODIFIED_DATETIME -  AUDIT_TBL.CREATED_DATETIME, 'mm.ss.fff') AS TIME_DIFF
FROM dbo.LCTA_INBOUND_DATA_TBL AS DATA_TBL JOIN dbo.LCTA_MSG_AUDIT_TBL AS AUDIT_TBL ON AUDIT_TBL.MSG_HDR_REF_ID = DATA_TBL.MSG_HDR_ID
WHERE DATA_TBL.CREATED_DATETIME  > @created_at and DATA_TBL.CREATED_DATETIME < @created_at_end;


SELECT
    AUDIT_TBL.MSG_TYPE,
    AUDIT_TBL.CREATED_DATETIME,
    AUDIT_TBL.MODIFIED_DATETIME,
    FORMAT(AUDIT_TBL.MODIFIED_DATETIME -  AUDIT_TBL.CREATED_DATETIME, 'mm.ss.fff') AS TIME_DIFF
FROM dbo.LCTA_INBOUND_DATA_TBL AS DATA_TBL JOIN dbo.LCTA_MSG_AUDIT_TBL AS AUDIT_TBL ON AUDIT_TBL.MSG_HDR_REF_ID = DATA_TBL.MSG_HDR_ID
WHERE DATA_TBL.CREATED_DATETIME  > @created_at and DATA_TBL.CREATED_DATETIME < @created_at_end;


SELECT
    'LCTA_MSG_AUDIT_TBL',
    count(*),
    min(CREATED_DATETIME) MIN_CREATED_DATETIME,
    max(CREATED_DATETIME) MAX_CREATED_DATETIME,
    min(MODIFIED_DATETIME) MIN_MODIFIED_DATETIME,
    max(MODIFIED_DATETIME) MAX_MODIFIED_DATETIME,
    MSG_STATUS, MSG_TYPE, FORMAT(max(MODIFIED_DATETIME) - min(CREATED_DATETIME),'mm.ss.ff') AS TOTAL_TIME
FROM LCTA_MSG_AUDIT_TBL  with (nolock)
WHERE CREATED_DATETIME  > @created_at AND CREATED_DATETIME < @created_at_end
GROUP BY MSG_STATUS,msg_type;



SELECT
    count(MSG_STATUS) 'Number of messages',
    MSG_TYPE 'Type of messages',
    MSG_STATUS,
    min(CREATED_DATETIME) 'LCT Adapter Started',
    max(MODIFIED_DATETIME) 'LCT Adapter Finished',
    FORMAT(max(MODIFIED_DATETIME) - min(CREATED_DATETIME),'mm.ss.ff') AS TOTAL_TIME
FROM LCTA_MSG_AUDIT_TBL  with (nolock)
WHERE CREATED_DATETIME  > @created_at AND CREATED_DATETIME < @created_at_end
GROUP BY MSG_STATUS,msg_type;





