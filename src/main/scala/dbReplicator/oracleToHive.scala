package dbReplicator

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object oracleToHive {
  val conf = ConfigFactory.load
  val jdbcDB: String = s"""jdbc:oracle:thin:@//${conf.getString("prod.db.ip")}/${conf.getString("prod.db.sid")}"""
  val userDB: String = conf.getString("prod.db.username")
  val passwordDB: String = conf.getString("prod.db.password")
  val jdbcDriverDB:String = conf.getString("prod.db.driver")
  //val jdbcHive: String = s"""jdbc:hive2://${conf.getString("prod.hive.ip")}:${conf.getString("prod.hive.port")}"""
  //val userHive: String = conf.getString("prod.hive.username")
  //val passwordHive: String = conf.getString("prod.hive.password")
  val tableListDB: List[String] = conf.getStringList("prod.db.tables").asScala.toList.map(x=> x.toUpperCase())
  val tableListHive: List[String] = conf.getStringList("prod.hive.tables").asScala.toList.map(x=> x.toUpperCase())
  val tableZipList: List[(String, String)] = tableListDB zip tableListHive
  val replicationType: String = conf.getString("userInput.replicationType")
  var prop: java.util.Properties = _

  def checkDBConnection = {
    db.checkConnection(jdbcDB, userDB, passwordDB)
  }

  //todo: do we need DL connection at all?
  def checkDLConnection = {
    dl.checkConnection(conf.getString("prod.dl.bucket"))
  }

  def checkHiveConnection(spark: SparkSession) = {
    //hive.checkConnection(jdbcHive, userHive, passwordHive)
    hive.checkConnectionV2(spark)
  }

  def checkDBTables(connDB: java.sql.Connection, tableListDB: List[String]): Unit = {
    tableListDB.foreach(table => {
      assert(db.checkTables(connDB, table), s"ERROR: table not found in Oracle database: $table")
    })
    ()
  }

  def checkHiveTables(spark: SparkSession, tableListHive: List[String]): Unit = {
    tableListHive.foreach(table => {
      //assert(hive.checkTables(connHive, table), s"ERROR: table not found in Hive database: $table")
      assert(hive.checkTablesV2(spark, table), s"ERROR: table not found in Hive database: $table")
    })
    ()
  }

  def checkTableCounts(tableListDB: List[String], tableListHive: List[String]): Unit = {
    assert(tableListDB.length == tableListHive.length, s"ERROR: Number of input tables for DB and Hive are not equal. tableCountDB = ${tableListDB.length} tableCountHive = ${tableListHive.length}")
  }

  def replicationTypeBaseOnly(spark: SparkSession, tableZip: (String, String), limit: Int = 0): Unit = {
    val dfHive = spark.read.table(s"${tableZip._2}")
    val dfDB = spark.read.jdbc(jdbcDB,tableZip._1, prop)
    if (dfHive.schema.fields.length != dfDB.schema.fields.length) {
      println(s"logger: WARNING: DB and Hive number of columns do not match for DB,Hive: $tableZip - skipping this table !! ")
    }
    else {
      //SPARK BUG
      //the next line does not work due to a spark bug: https://www.cloudera.com/documentation/enterprise/release-notes/topics/cdh_rn_spark_ki.html#ki_sparksql_dataframe_saveastable
      //dfDB.write.mode("append").format("parquet").saveAsTable(tableZip._2)
      //use workaround:
      if (limit > 0) dfDB.limit(limit).createOrReplaceTempView("temp_table") else dfDB.createOrReplaceTempView("temp_table")
      spark.sql(s"insert into table ${tableZip._2} select * from temp_table")
      spark.catalog.dropTempView("temp_table")
      println(s"logger: INFO: DB to Hive for ${tableZip._1} completed")
    }

    ()
  }

  def startSpark(local: Boolean = false): SparkSession = {
    val spark = sparkUtils.createSparkSession(local)
    prop = sparkUtils.setSparkJDBCProp(spark, userDB, passwordDB, jdbcDriverDB)
    spark
  }

  def main(args: Array[String]): Unit = {
    //check connection with Oracle DB
    checkDBConnection

    //check connection with S3
    checkDLConnection

    //initialize spark
    lazy val spark = startSpark()

    //check connection with hive
    checkHiveConnection(spark)

    //create conn to DB and hive
    val connDB = jdbcCommon.createConnection(jdbcDB, userDB, passwordDB)
    //val connHive = jdbcCommon.createConnection(jdbcHive, userHive, passwordHive)

    //check all tables exist on DB
    checkDBTables(connDB, tableListDB)

    // check all tables exist on hive
    checkHiveTables(spark, tableListHive)

    //check that number of input tables is the same
    checkTableCounts(tableListDB, tableListHive)

    // action begins here

    //baseOnly
    if (replicationType == "baseOnly") {
      //for each table begin replication
      tableZipList.foreach(tableZip => {
        val future_replicationTypeBaseOnly = Future(replicationTypeBaseOnly(spark, tableZip))
        Await.result(future_replicationTypeBaseOnly, 24 hours)
      })
    }
    //TODO Master list:
    // next task is to use futures and parallelize the baseOnly replication for 2 tables


  }

}
