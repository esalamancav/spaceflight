SELECT
name, 
EXTRACT(MONTH FROM published_at) as mes,
count(1) as total
FROM spaceflight.public.fact_article
where published_at is not NULL
GROUP BY name, mes
Order by mes, total DESC