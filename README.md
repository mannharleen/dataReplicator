# dataReplicator
-----------------

## This scala pet-project attempts to create a data replication application that will help replicate base and incremental data from source to target data stores

### Motivation:
- Most of the data replication tools out there are damn expensive. The ones that are open source arent reliable or arent complete (they may not include comprehensive CDC). 
- Take for instance - Talend ETL. It has an excellent built in capability to configure and capture CDC from Oracle databases. But it uses Oracle Streams, which is both difficult to get to work and drains the DB performance
- Take for isntance - AWS DMS. It is greatly simple to setup and captures CDC from Oracle directly from the logs. However, it doesnt expose the internals and we lose a lot of control. Further, in case of replication errors/issues, it is very hard to perform recons. It is even tougher to restart the replication from the point of error

### Guiding principles:
1. Make it simple
2. Keep it simple

> Reference blog posts:
1. https://bigdatamann.wordpress.com/2018/02/04/part-0-building-your-own-oracle-database-replication-solution/
