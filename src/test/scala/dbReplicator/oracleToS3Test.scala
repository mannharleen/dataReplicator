package dbReplicator
import dbReplicator.oracleToS3._
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class oracleToS3Test extends FunSuite{

  //check connection with Oracle DB
  test("!!! T001: connection to Oracle DB") {
    oracleToS3.checkDBConnection
  }
  //check connection with S3
  test("!!! T002: connection to AWS S3") {
    pending
    oracleToS3.checkDLConnection
  }
  //check connection with hive
  test("!!! T003: connection to hive metastore jdbc") {
    oracleToS3.checkHiveConnection
  }

  //check all tables exist on DB
  test("!!! T004: tables exists on Oracle DB") {
    oracleToS3.checkDBTables(jdbcCommon.createConnection(jdbcDB, userDB, passwordDB), tableListDB)
  }

  // check all tables exist on hive
  test("!!! T005: tables exists on hive DB") {
    oracleToS3.checkHiveTables(jdbcCommon.createConnection(jdbcHive, userHive, passwordHive), tableListHive)
  }

  //check a table that doesnt exit on DB
  test("!!! T006: tables DOES NOT exist on Oracle DB") {
    intercept[AssertionError] {
      oracleToS3.checkDBTables(jdbcCommon.createConnection(jdbcDB, userDB, passwordDB), List("schema.table"))
    }
  }

  // check all tables exist on hive
  test("!!! T007: tables DOES NOT exist on hive DB") {
    intercept[AssertionError] {
      oracleToS3.checkHiveTables(jdbcCommon.createConnection(jdbcHive, userHive, passwordHive), List("schema.table"))
    }
  }

}
