/*

Author: ALDAIR 

v1 SEP 7TH, 2021
v2 OCT 1ST, 2021


 */

/*INGEST SERVICE AND MESSAGE BROKER time of execution*/



DECLARE @created_at datetime2;
DECLARE @created_at_end datetime2;

SET @created_at = '2021-10-01 00:00:00.000'
SET @created_at_end = '2021-10-01 01:00:00.000'

;WITH
    ingestService_to_MessageBroker
    as
    (
        SELECT MSG_HDR.MSG_TYPE AS 'Type of message',
            MSG_EVNT.CRTD_AT AS 'Ingestion Service Message started',
            MSG_HDR.LST_UPDT_AT AS 'Ingestion Service Message finished',
            BLK_HDR.BLK_HDR_ID,
            MSG_HDR.MSG_ID,
            BLK_HDR.BLK_ID,
            BLK_HDR.CRNT_STATUS
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
    ingestService_to_MessageBroker.CRNT_STATUS,
    ingestService_to_MessageBroker.MSG_ID AS 'Ingest Service MSG_ID',
    ingestService_to_MessageBroker.[Ingestion Service Message started],
    ingestService_to_MessageBroker.[Ingestion Service Message finished],
    ingestService_to_MessageBroker.BLK_ID AS 'Message Broker BLK_ID',
    MIN(BULK_EVENT.CRTD_AT) AS 'Message broker started',
    MAX(BULK_EVENT.CRTD_AT) AS 'Message broker finished'
FROM ingestService_to_MessageBroker JOIN CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT ON ingestService_to_MessageBroker.BLK_HDR_ID = BULK_EVENT.BLK_HDR_ID
WHERE  BULK_EVENT.STATUS != 'Processed'
GROUP BY 
    BULK_EVENT.BLK_HDR_ID,
    ingestService_to_MessageBroker.BLK_HDR_ID,
    ingestService_to_MessageBroker.[Type of message],
    ingestService_to_MessageBroker.[Ingestion Service Message started],
    ingestService_to_MessageBroker.[Ingestion Service Message finished],
    ingestService_to_MessageBroker.MSG_ID,ingestService_to_MessageBroker.BLK_ID,
    ingestService_to_MessageBroker.CRNT_STATUS
ORDER BY ingestService_to_MessageBroker.[Ingestion Service Message started] ASC;




DECLARE @created_at datetime2;
DECLARE @created_at_end datetime2;

SET @created_at = '2021-10-01 00:00:00.000'
SET @created_at_end = '2021-10-01 01:00:00.000'


;WITH
    ingestService_to_MessageBroker_Total
    as
    (
        SELECT MSG_HDR.MSG_TYPE AS 'Type of message',
            MSG_EVNT.CRTD_AT AS 'Ingestion Service Message started',
            MSG_HDR.LST_UPDT_AT AS 'Ingestion Service Message finished' ,
            BLK_HDR.BLK_HDR_ID,
            BLK_HDR.CRNT_STATUS
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
    ingestService_to_MessageBroker_Total.CRNT_STATUS,
    MIN(ingestService_to_MessageBroker_Total.[Ingestion Service Message started]) AS 'Ingestion Service Message started',
    MAX(ingestService_to_MessageBroker_Total.[Ingestion Service Message finished]) AS 'Ingestion Service Message finished',
    MIN(BULK_EVENT.CRTD_AT) AS 'Message broker started',
    MAX(BULK_EVENT.CRTD_AT) AS 'Message broker finished'
FROM ingestService_to_MessageBroker_Total JOIN CONNECT_MS.MS_BLK_EVNT AS BULK_EVENT ON ingestService_to_MessageBroker_Total.BLK_HDR_ID = BULK_EVENT.BLK_HDR_ID
WHERE  BULK_EVENT.STATUS != 'Processed'
GROUP BY ingestService_to_MessageBroker_Total.[Type of message],ingestService_to_MessageBroker_Total.CRNT_STATUS
ORDER BY MIN(ingestService_to_MessageBroker_Total.[Ingestion Service Message started]) ASC;



/*LCT ADAPTER*/

/*Extract data with messageId comming from message_storage table*/

DECLARE @created_at datetime2;
DECLARE @created_at_end datetime2;

SET @created_at = '2021-10-01 00:00:00.000'
SET @created_at_end = '2021-10-01 01:00:00.000'



-- SELECT
--     DATA_TBL.MSG_HDR_ID,
--     DATA_TBL.MSG_STORE_REF_ID,
--     DATA_TBL.MSG_STATUS,
--     DATA_TBL.GS1_HEADER,
--     DATA_TBL.CREATED_DATETIME,
--     DATA_TBL.MODIFIED_DATETIME,
--     AUDIT_TBL.MSG_TYPE,
--     AUDIT_TBL.MSG_INGEST_PARAM,
--     AUDIT_TBL.GROUPED_BY,
--     AUDIT_TBL.INGESTION_ID,
--     AUDIT_TBL.INGEST_STATUS_MSG,
--     AUDIT_TBL.CREATED_DATETIME,
--     AUDIT_TBL.MODIFIED_DATETIME,
--     FORMAT(AUDIT_TBL.MODIFIED_DATETIME -  AUDIT_TBL.CREATED_DATETIME, 'HH:mm.ss.fff') AS TIME_DIFF
-- FROM dbo.LCTA_INBOUND_DATA_TBL AS DATA_TBL JOIN dbo.LCTA_MSG_AUDIT_TBL AS AUDIT_TBL ON AUDIT_TBL.MSG_HDR_REF_ID = DATA_TBL.MSG_HDR_ID
-- WHERE DATA_TBL.CREATED_DATETIME  > @created_at and DATA_TBL.CREATED_DATETIME < @created_at_end;


SELECT
    AUDIT_TBL.MSG_TYPE,
    AUDIT_TBL.MSG_STATUS,
    AUDIT_TBL.CREATED_DATETIME AS 'LCT Adapter Started',
    AUDIT_TBL.MODIFIED_DATETIME as 'LCT Adapter Finished',
    JSON_Value(DATA_TBL.GS1_header, '$.messageId') AS 'Message Broker_ID',
    FORMAT(AUDIT_TBL.MODIFIED_DATETIME -  AUDIT_TBL.CREATED_DATETIME, 'HH:mm.ss.fff') AS TIME_DIFF
FROM dbo.LCTA_INBOUND_DATA_TBL AS DATA_TBL JOIN dbo.LCTA_MSG_AUDIT_TBL AS AUDIT_TBL ON AUDIT_TBL.MSG_HDR_REF_ID = DATA_TBL.MSG_HDR_ID AND JSON_Value(GS1_header, '$.messageId') != 'NULL'
WHERE DATA_TBL.CREATED_DATETIME  > @created_at and DATA_TBL.CREATED_DATETIME <  @created_at_end 
ORDER BY AUDIT_TBL.MODIFIED_DATETIME ASC;


-- SELECT
--     'LCTA_MSG_AUDIT_TBL',
--     count(*),
--     min(CREATED_DATETIME) MIN_CREATED_DATETIME,
--     max(CREATED_DATETIME) MAX_CREATED_DATETIME,
--     min(MODIFIED_DATETIME) MIN_MODIFIED_DATETIME,
--     max(MODIFIED_DATETIME) MAX_MODIFIED_DATETIME,
--     MSG_STATUS, MSG_TYPE, FORMAT(max(MODIFIED_DATETIME) - min(CREATED_DATETIME),'HH:mm.ss.ff') AS TOTAL_TIME
-- FROM LCTA_MSG_AUDIT_TBL  with (nolock)
-- WHERE CREATED_DATETIME  > @created_at AND CREATED_DATETIME < @created_at_end
-- GROUP BY MSG_STATUS,msg_type;



SELECT
    count(AUDIT_TBL.MSG_STATUS) 'Number of messages',
    AUDIT_TBL.MSG_TYPE 'Type of messages',
    AUDIT_TBL.MSG_STATUS,
    min(AUDIT_TBL.CREATED_DATETIME) 'LCT Adapter Started',
    max(AUDIT_TBL.MODIFIED_DATETIME) 'LCT Adapter Finished',
    FORMAT(max(AUDIT_TBL.MODIFIED_DATETIME) - min(AUDIT_TBL.CREATED_DATETIME),'HH:mm.ss.ff') AS TOTAL_TIME
FROM dbo.LCTA_INBOUND_DATA_TBL AS DATA_TBL JOIN dbo.LCTA_MSG_AUDIT_TBL AS AUDIT_TBL ON AUDIT_TBL.MSG_HDR_REF_ID = DATA_TBL.MSG_HDR_ID AND JSON_Value(GS1_header, '$.messageId') != 'NULL'
WHERE DATA_TBL.CREATED_DATETIME  > @created_at and DATA_TBL.CREATED_DATETIME <  @created_at_end
GROUP BY AUDIT_TBL.MSG_STATUS,AUDIT_TBL.msg_type
ORDER BY min(AUDIT_TBL.CREATED_DATETIME);
