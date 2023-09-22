package com.github.music.of.the.ainur.almaren.bigquery
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SaveMode, SparkSession}
import org.scalatest._
import org.apache.spark.sql.functions._
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.bigquery.BigQuery.BigQueryImplicit
import org.scalatest.funsuite.AnyFunSuite

class Test extends AnyFunSuite with BeforeAndAfter {
  val almaren = Almaren("bigQuery-almaren")
  val spark: SparkSession = almaren.spark
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val bigQueryDf: DataFrame = spark.read.parquet("src/test/resources/data/bigQueryTestTable.parquet")

//  val gcpToken: String = sys.env.getOrElse("GCP_TOKEN", throw new Exception("GCP_TOKEN environment variable is not set"))
//  spark.conf.set("gcpAccessToken", gcpToken)
  val credentials : String = sys.env.getOrElse("GCP_STRING", throw new Exception("GCP_TOKEN environment variable is not set"))
  spark.conf.set("credentials","ew0KICAidHlwZSI6ICJzZXJ2aWNlX2FjY291bnQiLA0KICAicHJvamVjdF9pZCI6ICJtb2Rhay1uYWJ1IiwNCiAgInByaXZhdGVfa2V5X2lkIjogIjk4NDlmZTFmZjVkZGU5NjA2NmZmODRjYTJlMDRmNzBmMzQ5ZTkzNGQiLA0KICAicHJpdmF0ZV9rZXkiOiAiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXG5NSUlFdlFJQkFEQU5CZ2txaGtpRzl3MEJBUUVGQUFTQ0JLY3dnZ1NqQWdFQUFvSUJBUURPZ0lOejdrbjZmRjErXG54R2dvMUVicWJSUWdNVjFiNnJUbzJLRk5IWnJMeUE5YndIaW45cEVSTTd0YjA1VXE0TTAxelA4WHlGWTVpSW9jXG5OQSs2dzkzeU9ORkxyazN2bnBRQzhjT1pqSzZRL3ZWaVQzNG4zSVZGVmFmWkovTWhnNzJmTDNGQTRMaklNQVVRXG42dEducG9XR0xQVCszeHVQR1pNdmxOOEJZanBuR0FmV1BLNmxZMWozZjVNbXVodm9BV2xpbEFiajVTZVIyVEx0XG5VWFhZdU5nRUlvblBaZ2xjUmFRczl5UTByU1lMdU9BZmU4NjB4dWdKNTZ6eEZOUFUrMWVDVFV3YTlqYzRDV1lXXG53Tjl5cnZ4VnU4YWw1ZnpWVktwYy94V3hEQ1YzNitjeFFzOFpQM3phTll5cU9DYnNGUTNwdGVFSktqMEg3RFI1XG5sTGo4cFlQUkFnTUJBQUVDZ2dFQU0xbS8xbzlWOUNVQzl1Z09Zc0RPMWJMRVEzRWpIeDFSV1dtdFNzam0ybS9vXG5hYytGOFhhZUFtSVQ0RFRRTGpaeXNVTkYxL3NvZmV3WE5BWTZOeDVIemRrVktyV3dpYnFzWWNNTytkc0MrZWw4XG55YktzMDJyQjlVamtrVnFlMmlHeGdLbENoM2lhV3FXZVRPT251UUh1eUR2YnZKbHNkL3hMSHBUdGs2R0NsbW52XG4reHhSZzNvYlZlUXk4ZWpVTEs0dDJyUWFNcFlLb2lHOC84RUwvSG5ONVZiMlV4My95djhvTTRiWTIwdjByZUplXG5La1ErQzgvYmdENzBGdDZjZnp6b2Q1S044STNuYi9wSElXMzgvWEpqaVV0VWRuTHNEZXoxV3ZDNE51QmowWEdrXG52UnpBSmZxbzYwcGk3NXl6ZlhIZzN0VFJHUXZNQXU4K3B1amlIYm5YOFFLQmdRRHNtZDhuMHAxSFl6TmdWZVh5XG40dlVHQ0Jqa1VjR21ZSnA4Q0NiU0RVZXNvNU95d2RTeHd6Q0V3aEhxQVFQdWMrM09CSllBdTZ0QWplbVdKU3FvXG5WWFRSUnNYSDNyamVmWWJYemR1dFVwWU80VmdMOHdnQVozRi9sNXdmNi8yN0VBdFUvbDc5bzlOOURTNDh6dUZ4XG5KQjMzSHlrVncrbzQxNnVGSkhxODI5OWVmUUtCZ1FEZmJ1RC9Dd1FLa3NmL3d2NUI2TmlpM1VrZStvQnFuZHpoXG5ZUEs0MGU3VXRpdkp2eWwydjNGNEtNc3RRWHpEUWtxblZ6b3ZiVjRVaDlCWFJFTDJ2R2VwZFg0NWprbUJTaDdxXG5aMG9KaHl1R3poNDlOU21SS0swSURwb3dPaU1uMGdjRnZPMkJsZXp6Z0lJUjB3VGFHTDl0M2hTWUtGRWJNSmdmXG4yVys4bC9oVzVRS0JnRGhyaVBkcUZlOU1ESmhRWTRGRDljSDJkRmtkMWR2aTlYUFdUY3VSTnlKSDc1U1UvQzlYXG5xRjVBR1IyQUdIdm9VVERyandtbHR2U3g0cDNYUnhEVk1BSW5xa283SmtLSUZIdXByMVRwZUxjYnJXOU1DUUJ2XG5xblVPcGtCR2VqNzlXSFp3SWg3QnpsRG5yN3I0YW5JY2RyalRTV3pUUnlzKzRydmhNeE9PS3ZuUkFvR0JBS29IXG54Zjh3a2QyOS82Y1o2OFdhZmFuSy9rY3QyS1hLQm5vWS9mMDUvU2N3anJnVDJtajhuVXdVdHpGMFlZNXlGeityXG5lS0w0OXB4bkVsd21uVk1JNDFZcERHcWVaaitXZVZwbVNnaGdMZmFEU3EzSGxCOWNOZmtvcTV2QjBsa09VcnZDXG5IM2Y2OFB3Sk1uS0FCSFE0V3cwdjhMb3VERExGMHk2Qm1LK0xjcmdsQW9HQUpvMGMxdnpIQU1VOERadVVEZGVqXG5EMXplUmwyNzQ3RkdRL0xvSm9ERlZxR1gyRjg5NnFOOVdDWjhseVh5ZGl6bXdJQWhTM1lRYWhnNjQyWXB5dHIxXG5IaUUyYjdseTlhdnZ6cCtob0JPRWx2THhZMGNVb1c5cXBhcU5HMW12b3p2WUh1eC9XSkF2Z2VOOTJ5cXRtRlFoXG5QU2lqOEIzY1p4NFBDblU4QklXS0tDdz1cbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cbiIsDQogICJjbGllbnRfZW1haWwiOiAiYmlncXVlcnktc2FAbW9kYWstbmFidS5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsDQogICJjbGllbnRfaWQiOiAiMTAzOTkyNDU4NzMxMjgzODgxODk2IiwNCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwNCiAgInRva2VuX3VyaSI6ICJodHRwczovL29hdXRoMi5nb29nbGVhcGlzLmNvbS90b2tlbiIsDQogICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwNCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvYmlncXVlcnktc2ElNDBtb2Rhay1uYWJ1LmlhbS5nc2VydmljZWFjY291bnQuY29tIg0KfQ")
  spark.conf.set("viewsEnabled","true")
  spark.conf.set("materializationDataset","nabu_spark")
  //creating config map

  val configMap: Map[String, String] = Map("parentProject" -> "modak-nabu",
    "project" -> "modak-nabu",
    "dataset" -> "nabu_spark")
  val df: DataFrame = almaren.builder
    .sourceBigQuery("customer", configMap)
    .batch

  df.show()

  val configMap1: Map[String, String] = Map("parentProject" -> "modak-nabu",
    "project" -> "modak-nabu")
  val df1: DataFrame = almaren.builder
    .sourceBigQuery("SELECT * FROM `nabu_spark.customer` ", configMap1)
    .batch

  test(bigQueryDf, df, "Read bigQuery Test")
  def test(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    testCount(df1, df2, name)
    testCompare(df1, df2, name)
  }
  def testCount(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    val count1 = df1.count()
    val count2 = df2.count()
    val count3 = spark.emptyDataFrame.count()
    test(s"Count Test:$name should match") {
      assert(count1 == count2)
    }
    test(s"Count Test:$name should not match") {
      assert(count1 != count3)
    }
  }
  // Doesn't support nested type and we don't need it :)
  def testCompare(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    val diff = compare(df1, df2)
    test(s"Compare Test:$name should be zero") {
      assert(diff == 0)
    }
    test(s"Compare Test:$name, should not be able to join") {
      assertThrows[AnalysisException] {
        compare(df2, spark.emptyDataFrame)
      }
    }
  }
  private def compare(df1: DataFrame, df2: DataFrame): Long =
    df1.as("df1").join(df2.as("df2"), joinExpression(df1), "leftanti").count()
  private def joinExpression(df1: DataFrame): Column =
    df1.schema.fields
      .map(field => col(s"df1.${field.name}") <=> col(s"df2.${field.name}"))
      .reduce((col1, col2) => col1.and(col2))
  after {
    spark.stop
  }
}
