CREATE EXTERNAL TABLE IF NOT EXISTS source_social.reddit_scores (
    id STRING,
    time TIMESTAMP,
    topic_1 FLOAT,
    topic_2 FLOAT,
    topic_3 FLOAT,
    topic_4 FLOAT,
    topic_5 FLOAT,
    topic_6 FLOAT,
    topic INT,
    vader_neg FLOAT,
    vader_neu FLOAT,
    vader_pos FLOAT,
    vader_compount FLOAT,
    flair_sentiment STRING,
    flair_confidence FLOAT
)
PARTITIONED BY (process_date DATE)
STORED AS parquet
LOCATION "s3://stash-de-source-edge/source_social.db/reddit_scores/";