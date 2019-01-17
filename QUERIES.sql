/* top 10 popular categories */

SELECT category, count(*) as t FROM product_purchase GROUP BY category SORT BY t DESC LIMIT 10;

/* top 10 popular products for each category */

SELECT category, product, freq FROM (
    SELECT category, product, COUNT(*) AS freq, ROW_NUMBER() OVER (PARTITION BY category ORDER BY COUNT(*) DESC) as seqnum
FROM product_purchase GROUP BY category, product) ci 
WHERE seqnum <= 10;

/*  top 10 countries with the highest money spending */

SELECT getcountry(ip), SUM (price) as s FROM product_purchase GROUP BY getcountry(ip) SORT BY s desc limit 10;
