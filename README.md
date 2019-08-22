 # (Data)Stitchr #

####  (8/20/2019) Version 0.1 SNAPSHOT. Prepping for 0.1 release 

## What is (Data)Stitchr? ###

DataStitchr or Stitchr is a tool that helps move and transform data along all phases of a data processing pipeline.
It can be placed at any place in the pipeline to help in the data loading, transformation and extraction process. 

The objective is to develop an open, extensible  and "simple-to-use" tool for data engineers.

The focus is first on simplicity of use through "stitching" SQL constructs in a DAG of data transformations and derivations.
The use cases we intend to handle include (but are not limited to)

1. File based SQL. In this case data sources and data targets are files. 
2. A database engine is the source and target and data objects are Table, Views, Temporary tables, etc.
3. Data sources and targets are heterogeneous.

Currently, the code handles uses cases 1 and 2. While use case 3's functionality can be easily added, performance and optimization is another story.
Also the focus of this iteration is to model all our transformation in SQL.

For more on the Architecture 

## Preamble Jargon

Data objects in Stitchr are referred to as DataSets. Currently, a DataSet is either a file or an (RDBMS) database object such as a table or a view.
Each DataSet is associated with a data persistence layer or persistence zone. 
A persistence zone or container  may be an Amazon S3 folder, google cloud storage, HDFS, etc or a database such as postgres.
DataSets exist in containers and are manipulated and transformed and potentially new objects generated and saved in target persistence containers.

Code logic is driven by metadata stored in the Master Data Catalog System or simple the Data Catalog (DC). The DC holds all metadata specifying the objects and their association with the persistence containers.
 DataSets are either self-describing or are described with a schema definition stored in the data catalog.

DataSets can be concrete and hold information of what we refer to as primitive-base objects (stored in a container) or derived objects (specified by a query). 
    
Base DataSet objects are directly evaluated and assume that they do not have unresolved dependencies.
Derived DataSet objects depend on other objects and this is captured in the query attribute associated with the DataSet. The query is processed into a DAG of dependent objects and determine the recursive) sequence of processing towards the instantiation of the target derived DataSet object.

The process has 2 major steps: an initialization step in the inSessionDB (effectively Spark) and a materialization step to a target persistence container if required.

The simplest function would be to move objects from source containers to destination containers (passing through the initialization step as illustrated in Figure Y).

As expected we inherit from Spark's APIs and do not re-invent the wheel.

## What Features do we currently support

### Supported Persistence Containers
* objects stores such as S3 and GS
* HDFS
* files (not in cluster mode)
* JDBC: here we extend on Spark's parallel JDBC support and make it more generic. One specifies the partition key column (currently assumed to be Long) but does not need to provide a lower and upper limit. 
We do not compute the min/max but resort to runtime round robin bucketing based on the level of specified parallelism, and wrap the driver accordingly.


### Data Movement Use Cases ###
#### Move a group of Data Objects from a Source Container to a Destination Container
* Move a group of DataSets

     $STITCHR_ROOT/bash/runMoveDatSetGroup.sh
     
takes one argument which is the name of the group to run. The system reads the set of DataSet objects associated with the group and moves them to the target destination. Finally it will register or update the registry with the new moved objects
   
* Move a list of object references
  
     $STITCHR_ROOT/bash/runMoveDatSetList.sh 

is passed a comma-delimited list of object references and performs the same function of moving those objects to individually specified target containers. All of that is supported by registering the proper metadata information in the DC registry
    
     $STITCHR_ROOT/bash/runExamples.sh
runs a battery of tests moving a group of objects as well as a list of objects

#### Move objects from a JDBC source persistence by specifying the object as a select query.

This is supported by providing the select query in the  DataSet.query attribute and tagging the DataSet.mode as "base". The system will then push dpwn the query to the source JDBC persistence and return a handle to the result initialized in the runtime in session database. The materialization of the result to any target can be associated as well.
### DataSet object and Data Persistence container registration 
     
#### Automated Registration of a set of DataSet from a JDBC persistence source

This is supported through `com.stitchr.app.AutoRegisterService`. Given that you register the data persistence container information in the DC data_persistence table, invoking this function will pull the metadata associated with the tables/views in the persistence container and auto-register them in the Data Catalog.
The scala script  `$STITCHR_ROOT/app/scripts/testSchemaDiscovery` can be invoked from an interactive spark-shell to demo this feature.

#### Registration of DataSet and DataPersistence with Json input
This is supported with getters and putters associated with the DataSet, DataPersistence and Schema in the RegistryService. An Example is provided in `$STITCHR_ROOR/app/scripts/testJson.scala` and associated functions are found in `com.stitchr.core.registry.RegistryService`, 
such as `putJsonDataset` and `putJsonDataPersistence`.

### Data Transformation 
In the current version, The constraint is that all objects that we transform come from the same source persistence layer. This constraint will be removed in upcoming versions so that one could use federated queries. 
The constraint right now is more tied to keep the complexity burden on the user low. To enable it properly we need to, either constrain the object references globally or assume the query writer can specify object references across different persistence containers.
This would put an extra burden on the developer of such queries as we would need to introduce template support for query rewrites (with tolls such as JinJa). 
We do use Jinja in a very specific case and constrain the query writers to always alias the main table/view objects and enclose the dependency objects with `{{ }}`  (such as `{{ person }} as p`)
 
Taking into account the above restriction, any transformation that can be supported through (Spark)SQL can be specified. This covers Use cases 1 and 2 we outlined in the overview section.
 We are also working on adding UDF support through the Data Catalog metadata. 

### Miscelaneous Features
#### tracking run_time
The parameter `global.addRunTimeRef` from global.properties when set to true implies adding a column `run_time_ref` of type timestamp to all objects. 
It holds a session based timestamp which enables to compare the data moved during a session. This is strictly __EXPERIMENTAL__ as it has implications on the queries that are attached to the DataSet. 
#### tracking incrementally or reloading
The way data is moved is controlled globally at runtime using the parameter `global.defaultWriteMode`. If it is set to append then data is added otherwise it is a full reload (overwrite).

## Stitchr Architecture and Patterns ##

### where does it fit
Data loading, transformation, integration and extraction is based on computational and data processing patterns around  composable functional transformations, lazy evaluation and  immutable data. 
The DataStitchr (or just Stitchr) architecture is viewed as a set of data transformations from the data  zone to another zone, going though any intermediate and necessary persistence zones. 
It can serve as the tool used to implement any of the data processing phases in a Data and Analytics platform.
 
    (Figure to come).

We decided to base this implementation around well-established distributed data processing features of  [Apache Spark](https://spark.apache.org/ "Spark").


### Software Components

    figure to come

### Data Transformation Patterns ###
Stitchr relies on generating a DAG of data processing steps to derive the expected output DataSets. 
The patterns for the different transformations are simple and follow a well-defined and uniform set of transformations steps that we describe next. 

        Figures and write ups to come

### SQL constructs as Composable Functional Transformations ###
    
    ... more to  come here ... 

## How to setup and demo the tool? ###

* Configuration
    
    covered by 
    `$STITCHR_ROOT/demo/config/default.properties`
    
    
* Dependencies
   
        needs Spark 2.4 and Scala 2.11 installed. Although the system has been tested to work on Spark 2.2.3
    
* How to run the demo

    Place the data under `demo/data/tpcd` (downloaded and unzipped from [stitchr demo data](https://github.com/nhachem/stitchr-demo "stitchr-demo"))
    
    Edit the `bash/stitchr_env.sh` file and source it. This will setup the references to file-based registry objects, data folder and root directory of the code. Specifically   
          
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
          
          export VERSION=<current version>
          export MASTER=local[4]
             
    Issue: we add the user/pwd of data persistence zones to the registry. This is not safe and will have to be taken care of for a production ready use of the system        

    
     
 `cd` to the demo directory and then invoke interactively spark-shell as follows 
      
        spark-shell --jars $STITCHR_ROOT/app/target/stitchr-app-<$VERSION>-jar-with-dependencies.jar
    
        in the shell
        :load <path-to-root-code>/app/scripts/demoBasicStitchr.scala
    
 It will run against a sample demo tpcds data. Those tables metadata are stored in the registry. 
    The registry files datasets.csv and schema_columns.csv  are key and are found under demo/registry/
    (we do support and recommend using metadata support through a normal RDBMS, such as Postgres)

### Pyspark/python support

We added a simple implementation that wraps the scala runner in python. This is run_demo.py under the demo directory
after you set up your environment properly (python 3.7)
    
    to run interactively using pyspark: in the directory where the run_demo.py is located (pyspark-app/app/) run
    
    pyspark --jars $STITCHR_ROOT/app/target/stitchr-app-<$VERSION>-jar-with-dependencies.jar
    
    then at the prompt type
    
    import run_demo
    
    This will run the queries q2 and q4 based on the tpcds data files. Also the example code shows how to get back a handle to the scala spark session to list and work with the databadse tables.


In summary, all your data objects are mapped to tables or views and nested SQL constructs are logically parsed to determine dependencies.
The derivation service applies the computations in the order of dependencies (following a DAG structure built as a table of edges).

The derived objects can be views, tables or temporary tables and are configuration based through a metadata registry.

in the current version, at each stage of the flow, the derivation works sequentially through a list of objects that can be instantiated (obviously using distributed processing on each object), future versions will target concurrent derivation of independent objects.

... more to come ...

## The registry ##
Current key registry objects include the "dataset" and "schema_column" and "data_persistence." The schema of those registry objects may be in files but, for full functionality the RDBMS Postgres-based implementation is needed.

A data object (table or file, etc) is registered in the "dataset" table and is associated with a data persistence as its source. Metadata needed includes
data persistence source (such as jdbc source database and associated schema as the container, or a file with associated target folder and data source root file system. For "files" we lump sum object stores such as GS and S3).

in the sql folder, create scripts and comments documenting each attribute are included.
important attributes include
* We use PKs as sequences but have alternate keys
* data_persistence driver and connections info... A weakness in the current here is that the user/pwd attributes are in the clear
* `data_persistence.name` is unique. For `persistence_type = "file"` the driver is assembled as storage_type/host/db combination
* The `schema_column` table holds the schema info and is usually needed when we manipulate objects that are inherently not self-describing (such as CSV or delimited files in general)
* The `data_persistence.object_name` is assumed unique within its `data_persistence_src_id` persistence. 
    * `format` is database format such as postgresql, vertica, ... , parquet, avro, csv, pipeDelimited... 
    * `storage_type` is either databse or file for now and mode is either base or derived. 
* In the current version we focus on base objects and data movement between persistence, although the transformation engine (for derived objects) is the key to all data movement. 
* object type is table, view, file for now.
* The `dataset.query` attribute is essential for derived objects and holds the transformation that derives the object. For base objects, query files is the `object_name`
* `dataset_partition_key` and `number_partitions` is relevant for parallel jdbc. `schema_id` is the handle to join with the `schema_column` table  to get a list of column metadata associated with the dataset.
* `dataset.data_persietnce_src_id` is not null and should hold the association of the DataSet object to the source persistence container. The `data_persistence_dest_id` holds the target persistence of the moved object. We handle one target in this version. 
* Note that when an object is moved to a target a new dataset is created with the new object associated with the destination persistence as its source persistence.
* Finbally, In the `schema_column` table, PK is `(id, position)`, but we also have alternate unique constraints on `(id, column_name)`. 
    
## Contribution guidelines ###
 
* Known Issues
    * mvn scala:doc fails if the dependent jar files are not added to the local repo. It is a build config issue. 
    Although this is not critical and the docs actually are built, for now the work around is to add the jar after the build by running ./addJars2MavenRepo.sh $VERSION
    * currently the DataSet name is unique within a persistence container. But it should be tighter and include the container (technically schema in database terminology) should be part of that unique constraint
    * true federation queries are not supported yet but will be in upcoming versions
    * Catalog data deletes are currently manual
    * reference_time is experimental and we will support if we find it useful
    * "BUG": when reading data from a JDBC source we add a `bucket` column and it needs to be dropped and not carried over

* Pending Features and Fixes
    
    * Full crawler/module to enable auto registration of self-describing files and jdbc sources. We currently support only JDBC
    * Formalize the basic metadata schema and document it.
    * adding support for other containrs/formats, such as sftp, fixed-length-delimited files, spreadsheets, etc...
    * add non-sql functions that are "stitched" in the computation/derivation graph

* General
    * Writing unit tests
    * Code review
    * Other guidelines?!
    
### Send requests/comments  to ###
    
Repo owner/admin: Nabil Hachem (nabilihachem@gmail.com)

## Trademarks

Apache®, Apache Spark are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
See guidance on use of Apache Spark trademarks. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

Copyright ©2019 The Apache Software Foundation. All rights reserved.
