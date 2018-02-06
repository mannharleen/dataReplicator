package dbReplicator

import java.sql.DriverManager

object jdbcCommon {

  def createConnection(jdbc: String, user: String, password: String) = {
    Class.forName("oracle.jdbc.driver.OracleDriver")
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    DriverManager.getConnection(jdbc,user,password)
  }

}
