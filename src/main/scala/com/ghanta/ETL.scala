package com.ghanta

import com.ghanta.SparkRePartitioner.{Columns, MetaStoreClient, NonPartitionColumn, PartitionColumn}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

/**
  * Created by devjyotip on 8/28/17.
  */
abstract class ETL(redisEndpoint: String, apiUrl: String, apiToken: String) {
  var sourceColumns: (Columns, Columns) = null;
  var targetColumns: (Columns, Columns) = null;

  def getSourceColumns() = sourceColumns

  def getTargetColumns() = targetColumns
}


class RepartitionerETL(redisEndpoint: String, apiUrl: String, apiToken: String)
  extends ETL(redisEndpoint, apiUrl, apiToken)  {
  var (sourceSchema, sourceTable, targetSchema, targetTable) = ("", "", "", "");

  val session = SparkSession.builder()
    .appName("Ghanta Session")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()

  import session.implicits._

  def apply(accountId: Int, srcSchema: String, srcTable: String,
            tgtSchema: String, tgtTable: String): Unit = {
    sourceSchema = srcSchema;
    sourceTable = srcTable;
    targetSchema = tgtSchema;
    targetTable = tgtTable;
    val client = new MetaStoreClient(redisEndpoint, apiUrl, apiToken)


    sourceColumns = client.getTableSchema(accountId, sourceTable, sourceSchema)
    targetColumns = client.getTableSchema(accountId, targetTable, targetSchema)
  }


  def validate(): Boolean = {
    sourceColumns match {
      case (pc: PartitionColumn, npc: NonPartitionColumn) => {
        val allSourceColumns = pc.columns ++ npc.columns

        targetColumns match {
          case (pct: PartitionColumn, npct: NonPartitionColumn) => {
            (pct.columns forall (allSourceColumns contains)) && (npct.columns forall (allSourceColumns contains))
          }
          case _ => {
            println("Error in getting target columns")
            false
          }
        }
      }
      case _ => {
        println("Error in getting source columns")
        false
      }
    }
  }

  def extractAndLoad(extractPredicates: Map[String, String]): Unit = {
    //Extract
    val extractQuery = getExtractQuery(extractPredicates)
    val sourceDF = session.sql(extractQuery)

    val tempTable = s"temp.$sourceTable"
    sourceDF.createOrReplaceTempView(tempTable)

    //Load
    val loadQuery = getLoadQuery(tempTable)
    session.sql(loadQuery)
  }


  def getExtractQuery(predicates: Map[String, String]): String = {
    val predicateStr = if (predicates.isEmpty) "1=1" else predicates.map(p => s"${p._1}=${p._2}").mkString(" AND ")

    var projectColsStr = sourceColumns match {
      case (pc: PartitionColumn, npc: NonPartitionColumn) => (pc.columns ++ npc.columns).mkString(", ")
    }

    s"SELECT $projectColsStr FROM $sourceSchema.$sourceTable WHERE $predicateStr"
  }


  def getLoadQuery(tempTable: String): String = {
    val projectColsStr = targetColumns match {
      case (pc: PartitionColumn, npc: NonPartitionColumn) => (pc.columns ++ npc.columns).mkString(", ")
    }

    val partitionColsStr = targetColumns match {
      case (pc: PartitionColumn, _) => pc.columns.mkString(", ")
    }

    s"INSERT OVERWRITE TABLE $targetSchema.$targetTable PARTITION($partitionColsStr) SELECT $projectColsStr FROM $tempTable"
  }

}
