WITH
/* ������� ������� � ������������ ����� ����� � ������ ����� �� ���� �������  */
bills2 AS (
SELECT b.*, bc.*
	  ,sum(bc.cost) OVER (PARTITION BY b.num) AS bill_cost
	  ,sum(bc.payed) OVER (PARTITION BY b.num) AS bill_payed
FROM bills b
JOIN bill_content bc ON b.id = bc.bID
),
/* 
 * ������� ������� � ���������, � ������� ���� ���� � �������� ��������� �� �������� �������-�������.
 * ���� ����� ������ ��������� - ������� ����, �� �������� ������� �������� � ������������ ����� ���������
 */ 
open_supply_KE AS (
SELECT r.cid
	 , r.num
	 , r.bdate
	 , r.pdate
	 , r.bill_cost
	 , r.bill_payed
	 , '�������� �������� � ������������ ����� ���������' AS "type"
FROM (
	SELECT 
		 b2.*, rp.*
		,ROW_NUMBER() over (partition by b2.cid order by rp.upto desc) as "rank1"
	FROM bills2 b2
	LEFT JOIN retail_packs rp ON b2.bcID = rp.bcID
		WHERE b2.product = '������-�������' --����� ������������ like '%������-�������%', ���� �������� �������� �� ������� ����� ������ ������������
		  AND rp.sice <= sysdate
		  AND rp.upto > sysdate

) r
WHERE r.rank1 = 1
),
/*
 * ������� ������� � ���������, � ������� ���� ���� � ������������ ����� ������,
 * � ������� ���� ������ �� �������-������� �� ����������� ��� ���������
 */
connection_KE AS (
SELECT n.cid
	 , n.num
	 , n.bdate
	 , n.pdate
	 , n.bill_cost
	 , n.bill_payed
	 , '���� � ������������ ����� ������ �� �����������/���������' AS "type"
FROM (
	SELECT 
		 b2.*,
		,ROW_NUMBER() over (partition by b2.cid order by b2.pdate desc) as "rank2"
	FROM bills2 b2
		WHERE b2.product = '������-�������'
		  AND b2.tip IN (1, 2)
) n
WHERE n.rank2 = 1
) 
/*
 * � ������� � ��������� � �������� ��������� open_supply_KE ������������ ��������������� ��������
 * �� ������� connection_KE (�.�. ���, ���� ��� � open_supply_KE)
 */
SELECT * FROM open_supply_KE os
UNION ALL
SELECT * FROM connection_KE os c
WHERE c.cid NOT IN (SELECT os2.cid FROM open_supply_KE os2) -- ����� ����� ������������ not exists
