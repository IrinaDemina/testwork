WITH
/* создаем таблицу с предрасчетом суммы счета и оплаты счета по всем строкам  */
bills2 AS (
SELECT b.*, bc.*
	  ,sum(bc.cost) OVER (PARTITION BY b.num) AS bill_cost
	  ,sum(bc.payed) OVER (PARTITION BY b.num) AS bill_payed
FROM bills b
JOIN bill_content bc ON b.id = bc.bID
),
/* 
 * Создаем таблицу с клиентами, у которых есть счет с открытой поставкой по продукту «Контур-Экстерн».
 * Если таких счетов несколько - выведен счет, по которому создана поставка с максимальной датой окончания
 */ 
open_supply_KE AS (
SELECT r.cid
	 , r.num
	 , r.bdate
	 , r.pdate
	 , r.bill_cost
	 , r.bill_payed
	 , 'открытая поставка с максимальной датой окончания' AS "type"
FROM (
	SELECT 
		 b2.*, rp.*
		,ROW_NUMBER() over (partition by b2.cid order by rp.upto desc) as "rank1"
	FROM bills2 b2
	LEFT JOIN retail_packs rp ON b2.bcID = rp.bcID
		WHERE b2.product = 'Контур-Экстерн' --можно использовать like '%Контур-Экстерн%', если название продукта не всегода имеет точное соответствие
		  AND rp.sice <= sysdate
		  AND rp.upto > sysdate

) r
WHERE r.rank1 = 1
),
/*
 * Создаем таблицу с клиентами, у которых есть счет с максимальной датой оплаты,
 * в котором есть строки на «Контур-Экстерн» на подключение или продление
 */
connection_KE AS (
SELECT n.cid
	 , n.num
	 , n.bdate
	 , n.pdate
	 , n.bill_cost
	 , n.bill_payed
	 , 'счет с максимальной датой оплаты на подключение/продление' AS "type"
FROM (
	SELECT 
		 b2.*,
		,ROW_NUMBER() over (partition by b2.cid order by b2.pdate desc) as "rank2"
	FROM bills2 b2
		WHERE b2.product = 'Контур-Экстерн'
		  AND b2.tip IN (1, 2)
) n
WHERE n.rank2 = 1
) 
/*
 * К таблице с клиентами с открытой поставкой open_supply_KE присоединяем непересекющихся клиентов
 * из таблицы connection_KE (т.е. тех, кого нет в open_supply_KE)
 */
SELECT * FROM open_supply_KE os
UNION ALL
SELECT * FROM connection_KE os c
WHERE c.cid NOT IN (SELECT os2.cid FROM open_supply_KE os2) -- можно также использовать not exists
