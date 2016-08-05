import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}


object UpdateHLL extends UserDefinedAggregateFunction{

  override def inputSchema: StructType = {
    new StructType()
      .add("input", DataTypes.StringType) // all inputs are casted to string through the UDAF API, this won't affect the identity. StringType is the only SQL DataType compatible with all other types in UDAF API, binary in incompatible with primitive types
  }

  override def bufferSchema: StructType = {
    new StructType()
      .add("HLLBuffer", HLLBufferType)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[HLLBufferData](0).update(input.getString(0).getBytes)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[HLLBufferData](0).merge(buffer2.getAs[HLLBufferData](0))
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new HLLBufferData(0.05)
  }

  override def deterministic: Boolean = true

  override def evaluate(buffer: Row): Any = {
    buffer(0)
  }

  override def dataType: DataType = HLLBufferType
}

object MergeHLL extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    new StructType()
      .add("input", HLLBufferType)
  }

  override def bufferSchema: StructType = {
    new StructType()
      .add("HLLBuffer", HLLBufferType)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[HLLBufferData](0).merge(input.getAs[HLLBufferData](0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    update(buffer1, buffer2)
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new HLLBufferData(0.05)
  }

  override def deterministic: Boolean = true

  override def evaluate(buffer: Row): Any = buffer(0)

  override def dataType: DataType = HLLBufferType
}