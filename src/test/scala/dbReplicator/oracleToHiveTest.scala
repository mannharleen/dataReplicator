package dbReplicator
import dbReplicator.oracleToHive._
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.JavaConverters._

class oracleToHiveTest extends FunSuite with BeforeAndAfter {
  val spark = startSpark(true)

  //check connection with Oracle DB
  test("!!! T001: connection to Oracle DB") {
    oracleToHive.checkDBConnection
  }
  //check connection with S3
  test("!!! T002: connection to AWS S3") {
    pending
    oracleToHive.checkDLConnection
  }
  //check connection with hive
  test("!!! T003: connection to hive metastore jdbc") {
    oracleToHive.checkHiveConnection(spark)
  }

  //check all tables exist on DB
  test("!!! T004: tables exists on Oracle DB") {
    oracleToHive.checkDBTables(jdbcCommon.createConnection(jdbcDB, userDB, passwordDB), tableListDB)
  }

  // check all tables exist on hive
  test("!!! T005: tables exists on hive DB") {
    //oracleToS3.checkHiveTables(jdbcCommon.createConnection(jdbcHive, userHive, passwordHive), tableListHive)
    oracleToHive.checkHiveTables(spark, tableListHive)
  }

  //check a table that doesnt exit on DB
  test("!!! T006: tables DOES NOT exist on Oracle DB") {
    intercept[AssertionError] {
      oracleToHive.checkDBTables(jdbcCommon.createConnection(jdbcDB, userDB, passwordDB), List("schema.table"))
    }
  }

  // check all tables exist on hive
  test("!!! T007: tables DOES NOT exist on hive DB (intercept exception)") {
    intercept[AssertionError] {
      //oracleToS3.checkHiveTables(jdbcCommon.createConnection(jdbcHive, userHive, passwordHive), List("schema.table"))
      oracleToHive.checkHiveTables(spark, List("schema.table"))
    }
  }

  //check that number of input tables is the same
  test("!!! T008: number of input tables is the same (intercept exception)") {
    intercept[AssertionError] {
      oracleToHive.checkTableCounts(List(), List("abc"))
    }
  }

  //baseOnly
  test("!!! T009: Replicate 1 row: replicationTypeBaseOnly") {
    tableZipList.foreach( tableZip => {
      replicationTypeBaseOnly(spark, (tableZip._1 ,tableZip._2), limit = 1)
    })

  }
}
