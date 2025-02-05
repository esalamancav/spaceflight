CREATE DATABASE spaceflight;

CREATE TABLE spaceflight.public.dim_news_source (
    source_id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(255),
    url VARCHAR(255),
    reliability_score FLOAT
);

CREATE TABLE spaceflight.public.dim_topic (
    topic_id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(255),
    load_date VARCHAR(8)
);

CREATE TABLE spaceflight.public.fact_article (
    article_id INT PRIMARY KEY,
    reliability_score FLOAT4,
    entities VARCHAR(255),
	name VARCHAR(200)
    published_at TIMESTAMP,
    title VARCHAR(MAX),
    summary VARCHAR(MAX)
);