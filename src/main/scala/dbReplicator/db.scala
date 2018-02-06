package dbReplicator

import java.sql.DriverManager

object db {

  def checkTables(conn: java.sql.Connection, table: String): Boolean = {
    val tableName = table.split('.')(1)
    val schemaName = table.split('.')(0)

    try {
      val rs = conn.createStatement().executeQuery(s"""SELECT * FROM dba_tables where table_name = '$tableName' and owner = '$schemaName'""")
      rs.next()
    } catch {
      case e: Exception => {
        println(s"ERROR: table DUAL not found in Oracle database: $table")
        false
      }
    }

  }

  def checkConnection(jdbc: String, user: String, password: String): Unit = {
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver")
      val conn = DriverManager.getConnection(jdbc,user,password)
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery("select * from dual")
      while(rs.next()) {
        rs.getString(1)
      }
      conn.close()
    } catch {
      case e: Exception => {
        e.printStackTrace()
        System.exit(-1)
      }
    }


  }
}
