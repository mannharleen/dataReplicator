/*
-Dspark.master=local[*]
-Dspark.sql.hive.metastore.version=0.12.0
-Dspark.sql.hive.metastore.jars=maven
-Dspark.app.name=xyzapp

 */

object trytry {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder.enableHiveSupport().getOrCreate() //appName("app1").master("local[*]").

    spark.sql("show databases").show
    spark.conf.get("spark.sql.hive.metastore.version")
    spark.sql("create table t1(a Int)").show
  }

}
