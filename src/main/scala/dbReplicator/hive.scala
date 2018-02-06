package dbReplicator

import java.sql.DriverManager

import org.apache.spark.sql.SparkSession

object hive {

  def checkTablesV2(spark: SparkSession, table: String): Boolean = {
    val tableName = table.split('.')(1)
    val schemaName = table.split('.')(0)

    try {
      spark.sql(s"select * from $schemaName.$tableName limit 1").take(1)
      true
    } catch {
      case e: Exception => {
        println(s"ERROR: table not found in Hive database: $table")
        false
      }
    }
  }

  def checkConnectionV2(spark: SparkSession): Unit = {
    try {
      spark.sql("show databases").take(1)
      true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        System.exit(-2)
      }
    }
  }

  def checkTables(conn: java.sql.Connection, table: String): Boolean = {
    val tableName = table.split('.')(1)
    val schemaName = table.split('.')(0)

    try {
      val rs = conn.createStatement().executeQuery(s"""SELECT * FROM $schemaName.$tableName limit 1""")
      //rs.next()
      true
    } catch {
      case e: Exception => {
        println(s"ERROR: table not found in Hive database: $table")
        false
      }
    }
  }

  def checkConnection(jdbc: String, user: String, password: String): Unit = {
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver")
      val conn = DriverManager.getConnection(jdbc,user,password)
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery("show databases")
      while(rs.next()) {
        rs.getString(1)
      }
      conn.close()
    } catch {
      case e: Exception => {
        e.printStackTrace()
        System.exit(-2)
      }
    }


  }

}
