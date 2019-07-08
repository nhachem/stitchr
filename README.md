 # (Data)Stitchr #

####  WIP This software is in a POC/MVP phase (Feb 25, 2019.)
 
## What is Stitchr? ###

Stitchr is a tool that helps move and transform data along all phases of a data processing pipeline.
It can be placed at any place in the pipeline to help in the data loading, transformation and extraction process. 

The objective is to develop an open, extensible  and "simple-to-use" tool for data engineers

The focus is first on simplicity of use through "stitching" SQL constructs in a DAG of data transformations and derivations.
The use cases we intend to handle include (but are not limited to)

1. File based SQL. In this case data sources and data targets are files. 
2. A database engine is the source and target and data objects are Table, Views, Temporary tables, etc.
3. Data sources and targets are heterogeneous.

Currently, the code handles uses cases 1 and 2. While use case 3's functionality can be easily added, performance and optimization is another story.
Also the focus of this iteration is to model all our transformation in SQL.

## Stitchr Architecture and Patterns ###
Data loading, transformation, integration and extraction is based on computational and data processing patterns around composable functional transformations, lazy evaluation and immutable data. The processing layer of the architecture is really a micro-batch layer which can be extended to a streaming layer.

We decided to base this implementation around well-established distributed data processing features of  [Apache Spark](https://spark.apache.org/ "Spark").

... a lot coming here ... 

### How to setup and demo the tool? ###
    
* Summary of set up to include different Database drivers. 

The database use case demo relies on Postgres which you would need to install. 
    If you need other DB backends you will need to add them to the pom file or as jars
     
     Download the code and run (no tests are included at this point)   
            mvn package -DskipTests
    
    You will need to run the shell script addJars2MavenRepo.sh under bash once to install the vertica jdbc adapters as they are referenced by the core pom file to add support for Vertica JDBC

* Configuration
    
    under
    config/default.properties
   
    you need to add the database backend info if you are testing the database source/target use case. The current demo is based on files and so this may not be critical
    
    
* Dependencies
   
        needs Spark 2.4 and Scala 2.11 installed
        No data registration functions yet ... so you would need to manually edit the registry files if needed.
    
* How to run the demo

    place the data under demo/data/tpcd (downloaded and unzipped from [stitchr demo data](https://github.com/nhachem/stitchr-demo "stitchr-demo"))
    
    Edit the env.sh file and source it. This will setup the references to registry objects, data folder and root directory of the code. Specifically
          
          export STITCHR_ROOT=<path to code base>
          export USER_ROOT="<user home>"
          export USER_PERSIST_ROOT="<usually hdfs>"
          export CONFIG_DIR="<path to where the config directory is>"
          export DATA_DIR="<path-to-root--data directory>" ## usually $USER_PERSIST_ROOT/data/...
          export REGISTRY_DIR="<path to where the registry directory is>"
          export baseRegistryFolder=$REGISTRY_DIR/registry/
          export baseConfigFolder=$CONFIG_DIR/config/
          ## using tpcds generated and adjusted data
          export baseDataFolder=$DATA_DIR/tpcds/ ## for the demo
          
          Issue: we add the user/pwd to the registry. This is not safe and will have to be taken care of for a production ready use of the system        

    
     
 cd to the demo directory and then invoke spark-shell as follows 
      
        spark-shell --jars $STITCHR_ROOT/core/target/stitchr-core-0.1-SNAPSHOT-jar-with-dependencies.jar
    
        in the shell
        :load <path-to-root-code>/app/scripts/demoBasicStitchr.scala
    
 It will run against a sample demo tpcds data. Those tables metadata are stored in the registry. 
    The registry files datasets.csv and schema_columns.csv  are key and are found under demo/registry/
    (future versions we will focus on the metadata registry and add metadata support through a normal RDBMS, such as Postgres)

### Pyspark/python support

We added a simple implementation that wraps the scala runner in python. This is run_demo.py under the demo directory
after you set up your environment properly (python 3.7)
    
    to run interactively using pyspark: in the directory where the run_demo.py is located (pyspark-app/app/) run
    
    pyspark --jars $STITCHR_ROOT/core/target/stitchr-core-0.1-SNAPSHOT-jar-with-dependencies.jar
    
    then at the prompt type
    
    import run_demo
    
    This will run the queries q2 and q4 based on the tpcds data files. 
    Issue: 7/8/2019. broke regression here as we lose the session based database instantiation at the end of the run. 
    
    
### How does it work? ###

Basically all your data objects are mapped to tables or views and  nested SQL constructs are logically parsed to determine dependencies.
The derivation service applies the computations in order (following a DAG structure stored as a table of edges).

The derived objects can be views, tables or temporary tables and are configuration based through a metadata registry.

in the current version, at each stage of the flow, the derivation works sequentially through a list of objects that can be instantiated (obviously using distributed processing on each object), future version will target concurrent derivation of independent objects.

... more to come ...

## The registry ##
Current key registry objects include the "dataset" and "schema_column" and "data_source." The schema of those registry objects are in files but we will move to a postgres RDBMS in the next iteration.

... to fill in ...

## Contribution guidelines ###
 
* Pending Features and Fixes
    
    * moveFromSource2Target functionality. Single source loading and basic transformation
    * JDBC metadata crawler
    * crawler/module to enable auto registration of self-describing files and jdbc sources
    * Extending to using a real RDBMS for the Metadata Catalog (7/8/2019: basic implementation in progres)
    * Formalize the basic metadata schema and document it. We will start at the lowest levels of cataloging all instances of data.
    * Support the mixed source-target use case. We already have a SparkJdbc Driver wrapper to perform dataframe reads/writes and need to extend the metadata and the derivation service to handle the mixed use case seamlessly
    * adding dataframe/dataset support fot fixed-delimited files (for staging data from data sources with such files)
    * test deployment to a cluster (currently it has been tested as a standalone). Note that a single node system will work well if the source/target are a database as it relies on the database engine for processing.
    * add non-sql functions that are "stitched" in the computation/derivation graph

* General
    * Adding logging to provide debugging info
    * Writing unit tests
    * formalizing tpcds as a regression test use case
    * Code review
    * Other guidelines?!
    
### Send requests/comments  to ###
    
Repo owner/admin: Nabil Hachem (nabilihachem@gmail.com)

## Trademarks

Apache®, Apache Spark are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
See guidance on use of Apache Spark trademarks. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

Copyright ©2019 The Apache Software Foundation. All rights reserved.
