import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


object HLLTest{

  val HLLCardinality = udf{
    hll: HLLBufferData => hll.cardinality
  }

  val HLLUnionCardinality = udf{
    (hll1: HLLBufferData, hll2: HLLBufferData) => hll1.merge(hll2).cardinality
  }

  val HLLIntersect2Cardinality = udf{
    (hll1: HLLBufferData, hll2: HLLBufferData) => hll1.cardinality + hll2.cardinality - hll1.merge(hll2).cardinality
  }
  // for intersection of 3 hll, just need to write another UDF, the formula for intersection of 3 is a bit more complicated

  def main(args: Array[String]) {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test").setMaster("local[4]"))
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val df = Seq(
      (1, "hello", "world"),
      (2, "hello1", "world"),
      (1, "hello", "world"),
      (2, "world1", "hello"),
      (1, "world", "hello"),
      (2, "world", "hello")
    ).toDF("id", "text", "text2")

    df.show()

    val hllCountById = df.groupBy("id").agg(UpdateHLL(col("text")).as("hll"))
    hllCountById.printSchema()
    hllCountById.show(truncate = false)// id=1 cardinality = 2 (hello, world), id=2 cardinality = 3 (hello1, world1, world)

    val allHll = hllCountById.agg(MergeHLL(col("hll")).as("hll")).withColumn("cardinality", HLLCardinality(col("hll")))
    allHll.show(truncate = false) // cardinality = 4 (hello, world, hello1, world1)

    val allHllBoth = df.agg(UpdateHLL(col("text")).as("hll1"), UpdateHLL(col("text2")).as("hll2"))
      .withColumn("hll_union", HLLUnionCardinality(col("hll1"), col("hll2"))) // union cardinality=4(hello, world, hello1, world1)
      .withColumn("hll_intersection", HLLIntersect2Cardinality(col("hll1"), col("hll2"))) //intersect cardinality=2(hello, world)
    allHllBoth.show(truncate = false)
  }
}
