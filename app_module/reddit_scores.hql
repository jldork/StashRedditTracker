CREATE EXTERNAL TABLE IF NOT EXISTS source_social.reddit_scores (
    id STRING,
    title STRING,
    submission_time TIMESTAMP,
    author STRING,
    selfpost BOOLEAN,
    submission_text STRING,
    link STRING,
    flair STRING,
    num_comments INT,
    permalink STRING,
    upvotes INT,
    upvote_ratio FLOAT,
)
PARTITIONED BY (process_date DATE)
STORED AS parquet
LOCATION "s3://stash-de-source-edge/source_social.db/reddit_scores/";