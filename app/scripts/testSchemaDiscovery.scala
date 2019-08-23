
// make sure you have the env variables set to your environment in env.sh and that env.sh is sourced
// we use avro as a default for materialization and need to add the package
// spark-shell --jars $STITCHR_ROOT/app/target/stitchr-app-0.1-SNAPSHOT-jar-with-dependencies.jar --packages org.apache.spark:spark-avro_2.11:2.4.3

import com.stitchr.sparkutil.SharedSession.spark
import com.stitchr.core.registry.RegistryService.{getDataPersistence}
import com.stitchr.app.AutoRegisterService._

spark.sparkContext.setLogLevel("INFO")

// just list the session info
val configMap:Map[String, String] = spark.conf.getAll

val dp = getDataPersistence(1)

// it seems information_schema and pg_catalog are hidden from jdbc?! could be driver related
getJdbcDataSets(getDataPersistence(1), "pg_catalog").show(false)


getJdbcDataSets(getDataPersistence(1), "public").show(false)
getJdbcDataSets(getDataPersistence(6), "public").show(false)
getJdbcDataSets(getDataPersistence(5), "public").show(false)

// commented out so not to overwrite the data catalog
// registerJdbcDataSets(getJdbcDataSets(dp, "public"))