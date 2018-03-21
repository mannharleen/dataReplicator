package dbReplicator
import dbReplicator.oracleToHive._
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success}

class oracleToHiveTest extends FunSuite with BeforeAndAfter with ScalaFutures{
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
      replicateBase(spark, (tableZip._1 ,tableZip._2), limit = 1)
    })
  }
  //baseOnly Exception
  test("!!! T010: error in baseOnly Future (intercept exception)") {
    tableZipList.foreach( tableZip => {
      intercept[Exception] {
        val future = Future(replicateBase(spark, (tableZip._1 ,"nonExisitngTable"), limit = 1))
        future.onFailure{case e => throw e}
        //ScalaFutures.whenReady(future.failed) { e: Exception => assert(1 == 1)}
      }
    })
  }

}
