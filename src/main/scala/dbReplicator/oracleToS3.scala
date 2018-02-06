package dbReplicator

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._

object oracleToS3 {
  val conf = ConfigFactory.load
  val jdbcDB: String = s"""jdbc:oracle:thin:@//${conf.getString("prod.db.ip")}/${conf.getString("prod.db.sid")}"""
  val userDB: String = conf.getString("prod.db.username")
  val passwordDB: String = conf.getString("prod.db.password")
  val jdbcHive: String = s"""jdbc:hive2://${conf.getString("prod.hive.ip")}:${conf.getString("prod.hive.port")}"""
  val userHive: String = conf.getString("prod.hive.username")
  val passwordHive: String = conf.getString("prod.hive.password")
  val tableListDB: List[String] = conf.getStringList("prod.db.tables").asScala.toList
  val tableListHive: List[String] = conf.getStringList("prod.hive.tables").asScala.toList

  def checkDBConnection = {
    db.checkConnection(jdbcDB, userDB, passwordDB)
  }

  //todo: do we need DL connection at all?
  def checkDLConnection = {
    dl.checkConnection(conf.getString("prod.dl.bucket"))
  }

  def checkHiveConnection = {
    hive.checkConnection(jdbcHive, userHive, passwordHive)
  }

  def checkDBTables(connDB: java.sql.Connection, tableListDB: List[String]): Unit = {
    tableListDB.foreach(table => {
      assert(db.checkTables(connDB, table), s"ERROR: table not found in Oracle database: $table")
    })
    ()
  }

  def checkHiveTables(connHive: java.sql.Connection, tableListHive: List[String]): Unit = {
    tableListHive.foreach(table => {
      assert(hive.checkTables(connHive, table), s"ERROR: table not found in Hive database: $table")
    })
    ()
  }

  def main(args: Array[String]): Unit = {
    //check connection with Oracle DB
    checkDBConnection

    //check connection with S3
    checkDLConnection

    //check connection with hive
    checkHiveConnection

    //create conn to DB and hive
    val connDB = jdbcCommon.createConnection(jdbcDB, userDB, passwordDB)
    val connHive = jdbcCommon.createConnection(jdbcHive, userHive, passwordHive)

    //check all tables exist on DB
    checkDBTables(connDB, tableListDB)

    // check all tables exist on hive
    checkHiveTables(connHive, tableListHive)

    //for each table
      //get all column names from hive
      //begin replication
  }

}
