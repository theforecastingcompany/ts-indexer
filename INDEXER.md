We are building a time-series indexer. The indexer operates on an S3 bucket/prefix that contains 3-10 TB of time-series data in long format.
The data is in @[https://eu-west-3.console.aws.amazon.com/s3/buckets/tfc-modeling-data-eu-west-3?region=eu-west-3&bucketType=general&prefix=lotsa_long_format/&showversions=false].
The metadata is in @[https://eu-west-3.console.aws.amazon.com/s3/buckets/tfc-modeling-data-eu-west-3?region=eu-west-3&bucketType=general&prefix=metadata/lotsa_long_format/&showversions=false].

Each long-format data set contains multiple time-series. 


The goal of this project is to make the folder searchable blazingly fast.
The user should be able to fuzzy search time-series by theme, specific time-series id, dataset name etc. And the user should be able to plot a single time-series after searching within seconds. The goal is that our team, at The Forecasting Company, can easily look at the most time-series data. Ideally we also want the user to be able to talk to the DB with a chat interface powered by Claude 4 and other frontier models.

We use duckDB, Vercel, modal. React frontend with pnpm package manager, hosted on Vercel, echarts for plotting. Modal if we need a Python backend. 

The metadata schemas are defined in our github: @https://github.com/theforecastingcompany/navi/blob/main/data/training_datasets/src/metadata/common.py. You should have access via Github MCP.

The goal for now is just to build the backend/search/retrieval system. A simple CLI interface will be enough. We'll build a front-end with fast plotting after this is tested and robust.

Let's go :)
