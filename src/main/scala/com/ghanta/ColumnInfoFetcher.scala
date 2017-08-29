package com.ghanta

/**
  * Created by devjyotip on 8/22/17.
  */

import com.ghanta.ColumnInfoFetcher.ColumnType.ColumnType
import com.qubole.tenali.metastore.APIMetastoreClient
import com.qubole.tenali.metastore.CachingMetastoreClient
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api._
import org.apache.thrift.TException


import collection.JavaConversions


object ColumnInfoFetcher {

  object ColumnType extends Enumeration {
    type ColumnType = Value
    val Partitioned, NonPartitioned, None = Value
  }

  class Columns(columns: Seq[FieldSchema], _type: ColumnType)

  object Columns {
    def apply(columns: Seq[FieldSchema], _type: ColumnType): Columns = {
        _type match {
          case ColumnType.Partitioned => PartitionColumn(columns)
          case ColumnType.NonPartitioned => NonPartitionColumn(columns)
          case _ => Columns(Seq(), ColumnType.None)
        }
    }
  }

  case class PartitionColumn(columns: Seq[FieldSchema], val _type: ColumnType = ColumnType.Partitioned)
    extends Columns(columns, _type)

  case class NonPartitionColumn(columns: Seq[FieldSchema], val _type: ColumnType = ColumnType.NonPartitioned)
    extends Columns(columns, _type)


  private[ghanta] class MetaStoreClient(redisEndpoint: String, apiUrl: String, apiToken: String,
                        TTL_MINS: Int = 1000, MISSINGTTL_MINS: Int = 10000) {

    def getColumns(tableInfo: Table): Seq[FieldSchema] = {
      JavaConversions.asScalaBuffer(tableInfo.getSd().getCols)
    }

    def getPartitionColumns(tableInfo: Table): Seq[FieldSchema] = {
      JavaConversions.asScalaBuffer(tableInfo.getPartitionKeys())
    }

    @throws(classOf[MetaException])
    @throws(classOf[TException])
    @throws(classOf[UnknownTableException])
    @throws(classOf[UnknownDBException])
    def getTableSchema(accountId: Int, schema: String, table: String): (Columns, Columns) = {
      val apimetastoreClient = new APIMetastoreClient(accountId, apiUrl, apiToken)
      /*val metastoreClient: IMetaStoreClient = new CachingMetastoreClient(
                    redisEndpoint, String.valueOf(accountId), TTL_MINS, apimetastoreClient, MISSINGTTL_MINS, true)

      val tableInfo: Table = metastoreClient.getTable(schema, table)*/
      val tableInfo: Table = apimetastoreClient.getTable(schema, table)
      val partitionsColumns: Seq[FieldSchema] = getPartitionColumns(tableInfo)
      val otherColumns: Seq[FieldSchema] = getColumns(tableInfo)

      (Columns(partitionsColumns, ColumnType.Partitioned), Columns(otherColumns, ColumnType.NonPartitioned))
    }
  }
}


