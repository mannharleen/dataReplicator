package dbReplicator

import org.apache.spark.sql.SparkSession

object sparkUtils {

  def createSparkSession(local: Boolean = false): SparkSession = {
    val spark = if (local == true)
      SparkSession.builder.appName("dbReplicator").enableHiveSupport().master("local[*]").getOrCreate() else
      SparkSession.builder.appName("dbReplicator").enableHiveSupport().getOrCreate()
    spark.conf.get("spark.sql.hive.metastore.version")
    spark
  }

  def setSparkJDBCProp(spark: SparkSession, userDB:String, passwordDB:String, jdbcDriverDB:String): java.util.Properties = {
    val prop = new java.util.Properties()
    prop.setProperty("user",userDB)
    prop.setProperty("password",passwordDB)
    prop.setProperty("driver",jdbcDriverDB)
    prop
  }

}
