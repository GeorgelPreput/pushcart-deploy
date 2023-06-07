SELECT month, item_id, avg(price) AS average_price
FROM vw__silver__item_cost
GROUP BY item_id, month