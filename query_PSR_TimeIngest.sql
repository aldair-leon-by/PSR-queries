--MESSAGE STORAGE


/*MS_MSG_HDR--> En esta tabla podremos obtener los MSG_ID de cada MSG_TYPE establecion el inicio de la ingestion como parametro para obtener solo los mensajes 
creados en ese tiempo 
MS_BLK_HDR--> Podemor ver el estatus del mensaje*/

SELECT * from connect_ms.ms_blk_hdr where BLK_ID LIKE '%execution-fb731185-6ee3-45cd-aa48-c8e6f88fe61a-2021.08.19%';

SELECT top 100 * from connect_ms.MS_MSG_HDR with (nolock)  where 
--MSG_TYPE  like '%itemLocationMessage%' and 
MSG_TYPE in ('customerOrder','transportInstruction','transportChain') and
mdl_type = 'BYDM' and
msg_sndr in('SW', 'HOST.GLOBAL') and 
--ref_msg_sndr not in ('HOST.GLOBAL') and
--msg_hdr_id not in (6579,6530,6398,6397,6657) and
--doc_id = 'c1ea4ef7-a59c-4975-91e7-73e7cff0e41b' and 
crtd_at > '2021-08-19 14:00:07.623'  and crtd_at < '2021-08-19 15:10:07.623' order by CRTD_AT desc; --e633276c-0e4e-4cfc-971a-260d7b92d984  96585








--LCT ADAPTER STORAGE

DECLARE @created_at datetime2;
DECLARE @created_at_end datetime2;
 
SET @created_at = '2021-08-20 14:10:00'
SET @created_at_end = '2021-08-20 15:00:00'



/*Numero total de ingestions en 1 hora*/
Select 'LCTA_MSG_AUDIT_TBL', count(*), min(CREATED_DATETIME) MIN_CREATED_DATETIME, max(CREATED_DATETIME) MAX_CREATED_DATETIME, min(MODIFIED_DATETIME) MIN_MODIFIED_DATETIME, max(MODIFIED_DATETIME) MAX_MODIFIED_DATETIME, MSG_STATUS, MSG_TYPE
from LCTA_MSG_AUDIT_TBL  with (nolock)
where CREATED_DATETIME  > @created_at and CREATED_DATETIME < @created_at_end
group by MSG_STATUS,msg_type;

/*Numero total de ingestions en 1 hora ingestion_id*/
select  * from LCTA_MSG_AUDIT_TBL  with (nolock) where CREATED_DATETIME  > @created_at and CREATED_DATETIME < @created_at_end --where MSG_STATUS = 'COMPLETED' order by CREATED_DATETIME --and MSG_HDR_REF_ID = '20'



/*Mas detalles sobre la ingestion*/
select TOP(10) * from LCTA_INBOUND_DATA_TBL with (nolock) where  gs1_header like '%execution-4b2aac5e-5078-44e0-9c9b-0092e137d187-2021.08.20-b1fb08b4-4646-41f5-b2ea-29b0583f7329%' order by CREATED_DATETIME -- and MSG_STATUS != 'COMPLETED' --and GS1_HEADER like '%execution-e42c1345-0f23-4c44-b9ce-07c970acec28-2021.08.06%';
--select * from LCTA_MSG_AUDIT_TBL with (nolock) where CREATED_DATETIME  > @created_at 
--and msg_status not in ('COMPLETED1', 'INGESTED','COMPLETED_WITH_ERROR1')
--and msg_type like '%CustomerOrder%'
--and msg_hdr_ref_id in ( '96277')

/*"numProcessed" is the number of lines or header*/








