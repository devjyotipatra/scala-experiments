package ghanta

import com.ghanta.RepartitionerETL
import com.ghanta.SparkRePartitioner.{Columns, NonPartitionColumn, PartitionColumn}
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.Assertions._
import org.scalatest.junit.JUnitRunner
import org.scalactic._
import TypeCheckedTripleEquals._

/**
  * Created by devjyotip on 8/22/17.
  */
class SparkRePartitionerTest extends FlatSpec with Matchers {
  val redisEndpoint: String = "mojave-redis.imzqhl.0001.use1.cache.amazonaws.com"
  val apiUrl: String = "qa3.qubole.net"
  val apiToken: String = "9VYjFpwMoe3hmzqPfpaS2mFFN7wr4srC8vNT5wXnAxYCGFH3pvtJ4B56ZSPCeu2v"
  val accountId = 5911
  val sourceSchema = "tenaliv2"
  val sourceTable = "usagemap"
  val targetSchema = "tenaliv2"
  val targetTable = "usagemap_test"

  val etl = new RepartitionerETL(redisEndpoint, apiUrl, apiToken)
  etl.apply(accountId, sourceSchema, sourceTable, targetSchema, targetTable)

  "MetaStoreClient" should "return Partition Keys" in {
    val s = etl.getSourceColumns() match {
      case (p: Columns, np: Columns) => p match {
        case PartitionColumn(Seq(a: FieldSchema, b: FieldSchema, c: FieldSchema), _) => {
          Set(a.getName(), b.getName(), c.getName())
        }
      }
    }

    /*s should contain("submit_time")
    s should contain("account_id")
    s should contain("source")*/

    s forall(Set("submit_time", "account_id", "source") contains)
  }




  val sourceQuery = etl.getExtractQuery(Map("submit_time" -> ">='2017-08-01'"))
  "Query" should "return True" in {
    sourceQuery should equal("SELECT * FROM tenaliv2.usagemap WHERE submit_time>='2017-08-01'")
  }

  val tgtQuery = etl.getLoadQuery("temp.usagemap")
  "Query" should "return True" in {
    tgtQuery should equal("")
  }

}
