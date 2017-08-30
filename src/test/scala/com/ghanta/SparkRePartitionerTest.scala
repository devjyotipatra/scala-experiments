package ghanta

import com.ghanta.NoTransformRepartitionETL
import com.ghanta.ColumnInfoFetcher.{Columns, PartitionColumn}
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.SparkSession

/**
  * Created by devjyotip on 8/22/17.
  */
class SparkRePartitionerTest extends FlatSpec with Matchers {
  val redisEndpoint: String = "mojave-redis.imzqhl.0001.use1.cache.amazonaws.com"
  val apiUrl: String = "qa3.qubole.net"
  val apiToken: String = "9VYjFpwMoe3hmzqPfpaS2mFFN7wr4srC8vNT5wXnAxYCGFH3pvtJ4B56ZSPCeu2v"
  val accountId = 1208
  val sourceSchema = "processed"
  val sourceTable = "mrTez"
  val targetSchema = "processed_interm"
  val targetTable = "mrTez"

  val sourceColumns = Seq("event_id", "command_id", "job_id", "event_data", "source", "cluster_tag" ,
    "cluster_id", "env_url", "processed_date", "processed_hour", "event_time", "event_date", "event_hour")
  val sourceFilters = Map("event_date" -> "='2017-08-01'")

  val targetColumns = Seq("event_id", "command_id", "job_id", "event_data", "source", "cluster_tag" ,
                              "cluster_id", "env_url", "event_date", "event_hour", "event_time")
  val targetPartitions = Seq("processed_date", "processed_hour")


  val session = SparkSession.builder()
    .appName("Ghanta Session")
    .master("local")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()

  val etl = new NoTransformRepartitionETL(session, redisEndpoint, apiUrl, apiToken)
  etl.apply(accountId, sourceSchema, sourceTable, targetSchema, targetTable)


  // Tests start here

  "MetaStoreClient GET Source Partition Columns" should "return True" in {
    val s = etl.getSourceColumns() match {
      case (p: Columns, np: Columns) => p match {
        case PartitionColumn(Seq(a: FieldSchema, b: FieldSchema), _) => {
          Set(a.getName(), b.getName())
        }
      }
    }

    s forall(Set("event_date", "event_hour") contains)
  }

  "MetaStoreClient GET Target Partition Columns" should "return True" in {
    val s = etl.getSourceColumns() match {
      case (p: Columns, np: Columns) => p match {
        case PartitionColumn(Seq(a: FieldSchema, b: FieldSchema), _) => {
          Set(a.getName(), b.getName())
        }
      }
    }

    s forall(Set("processed_date", "processed_hour") contains)
  }


  val sourceQuery = etl.getExtractQuery(sourceFilters)
  "Source Query" should "return True" in {
    val predicateStr = if (sourceFilters.isEmpty) "1=1" else sourceFilters.map(p => s"${p._1}=${p._2}").mkString(" AND ")
    val projectStr = sourceColumns.sortWith(_ < _).mkString(", ")
    assert(sourceQuery == s"SELECT $projectStr FROM $sourceSchema.$sourceTable WHERE $predicateStr")
  }

  val tgtQuery = etl.getLoadQuery(s"temp.$sourceTable")
  "Target Query" should "return True" in {
    val projectColsStr =  (targetColumns.sorted ++ targetPartitions).mkString(", ")
    val partitionColsStr = targetPartitions.mkString(", ")

    assert(tgtQuery == s"INSERT OVERWRITE TABLE $targetSchema.$targetTable PARTITION($partitionColsStr) SELECT $projectColsStr FROM temp.$sourceTable")
  }

}
