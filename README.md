# CosmosGBBDemo

This repo demonstrates a simple Cosmos DB console application that issues queries, point reads, and writes to a configured Cosmos account. It demonstrates use of:

- regional configuration
- retry logic with Polly
- pre-aggregation to conserve RUs by using point reads instead of expensive queries
