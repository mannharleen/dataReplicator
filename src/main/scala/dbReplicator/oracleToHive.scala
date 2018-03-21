package dbReplicator

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import scala.util.{Success,Failure}

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

  def replicateBase(spark: SparkSession, tableZip: (String, String), limit: Int = 0): Unit =  {
    val dfHive = spark.read.table(s"${tableZip._2}")
    // Must cast as Number(30,0) or else spark schema reads as Number(30,10). this is due to an Oracle buh
    // details of the Oracle bug can be found at: https://support.oracle.com/knowledge/Oracle%20Database%20Products/1266785_1.html
    val dfDB = spark.read.jdbc(jdbcDB,s"(select A.*, CAST(A.ora_rowscn AS NUMBER(30,0)) as rowscn from ${tableZip._1} A)", prop)
    //
    var maxRowScn: java.math.BigDecimal = java.math.BigDecimal.valueOf(0)
    assert(dfHive.schema.fields.length == dfDB.schema.fields.length, s"logger: WARNING: DB and Hive number of columns do not match for DB,Hive: $tableZip - skipping this table !! ")
    //
    //SPARK BUG
    //the next line does not work due to a spark bug: https://www.cloudera.com/documentation/enterprise/release-notes/topics/cdh_rn_spark_ki.html#ki_sparksql_dataframe_saveastable
    //dfDB.write.mode("append").format("parquet").saveAsTable(tableZip._2)
    //use WORKAROUND:
    if (limit > 0) dfDB.limit(limit).createOrReplaceTempView("temp_table") else dfDB.createOrReplaceTempView("temp_table")
    spark.sql(s"insert into table ${tableZip._2} select * from temp_table")
    //
    maxRowScn = Option(spark.sql("select max(rowscn) from temp_table").first().getAs[java.math.BigDecimal](0) ).getOrElse(java.math.BigDecimal.valueOf(0))
    spark.sql("create table if not exists default.scn_hist (table_name string, rowscn bigint, ts string) row format delimited fields terminated by ',' stored as textfile")
    spark.sql(s"insert into default.scn_hist select '${tableZip._1}', max(rowscn), from_unixtime(unix_timestamp()) from temp_table")
    //
    spark.catalog.dropTempView("temp_table")
    //
    println(s"logger: INFO: max SCN for ${tableZip._1} is $maxRowScn")
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
      tableZipList.foreach(tableZip => {
        println(s"logger: INFO: Starting replicateBase for ${tableZip._1}")
        //val future_replicateBase = replicateBase(spark, tableZip)
        val future_replicateBase = Future(replicateBase(spark, ("a","b")))
        Await.result(future_replicateBase, 24 hours)
        future_replicateBase.onComplete {
          case Success(s) => println(s"logger: INFO: replicateBase for ${tableZip._1} completed")
          case Failure(e) => println(s"logger: WARNING: replicateBase for ${tableZip._1} FAILED !!"); e.printStackTrace()
        }
      })
    } /*else if (replicationType == "cdcOnly") {
      tableZipList.foreach(tableZip => {
        val future_replicateCdc = replicateCdc(spark, tableZip)
        Await.result(future_replicateCdc, 24 hours)
      })
    }*/
    //TODO Master list:
    //


  }

}
