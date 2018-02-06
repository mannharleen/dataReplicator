package dbReplicator

import java.sql.DriverManager

object hive {

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
