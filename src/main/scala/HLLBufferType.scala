import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.catalyst.expressions.aggregate.HyperLogLogPlusPlus
import org.apache.spark.sql.catalyst.expressions.{BoundReference, SpecificMutableRow}
import org.apache.spark.sql.types.{DataType, DataTypes, UserDefinedType}

// HLL counters are supposed to be saved and reused for a long period of time
// Set a fixed UID, in case the class needs to be modified for some reason without breaking the serialization contract

@SerialVersionUID(1L)
class HLLBufferData(val relativeSD: Double = 0.05) extends Serializable{

  // If the rsd (error) is too small, the counter will be too big to fit in the BinaryType which defaults to 4096 bytes
  // rsd=0.1 has 13 words
  // rsd=0.05 has 52 words
  // rsd=0.01 has 1639 words
  // rsd=0.001 has 209716 words
  // Serialization, HLL++ object can be recreated and takes more space than the buffer, so don't serialize it (make it def no val)
  // rsd=0.1, 3962B, 583B buffer only
  // at rsd=0.05, HLL++ object takes 7KB, while the buffer  only 1.4KB
  // rds=0.01, 196KB total, 25KB buffer only
  def hllPlusPlus = HyperLogLogPlusPlus(new BoundReference(0, DataTypes.BinaryType, true), relativeSD)
  val bufferData = {
    val hllpp = hllPlusPlus
    val buffer = new SpecificMutableRow(hllpp.aggBufferAttributes.map(_.dataType))
    hllpp.initialize(buffer)
    buffer
  }

  def cardinality: Long = hllPlusPlus.eval(bufferData).asInstanceOf[Long]

  def bufferSize = bufferData.numFields

  // set string directly will result in casting error when converting to UTF8String, byte array is universal
  def update(obj: Array[Byte]): HLLBufferData = {
    val inputBuffer = new SpecificMutableRow(Seq(DataTypes.BinaryType))
    inputBuffer.update(0, obj)
    //inputBuffer.setLong(0, MurmurHash.hash64(obj)) // another option is to use LongType and use MurmurHash, but it's not as general as using bytes
    hllPlusPlus.update(bufferData, inputBuffer)
    this
  }

  def merge(other: HLLBufferData): HLLBufferData = {
    if(other.relativeSD != this.relativeSD){
      throw new Exception(s"Cannot merge HLL++ buffer with different relativeSD ${this.relativeSD}!=${other.relativeSD}")
    }
    else{
      hllPlusPlus.merge(bufferData, other.bufferData)
    }
    this
  }

  override def toString = {
      s"HLL++(cardinality=${cardinality}, rsd=${relativeSD}, size=${bufferSize}): ${bufferData.toString}"
  }

}

// Using user defined type is not necessary (can always use Binary type), but this greatly simplifies the HLL operation code, and using custom toString is nicer
class HLLBufferType extends UserDefinedType[HLLBufferData]{
  override def sqlType: DataType = DataTypes.BinaryType

  override def serialize(obj: Any): Any = {
    val baos = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(baos)
    o.writeObject(obj)
    val bytes = baos.toByteArray
    bytes
  }

  override def userClass: Class[HLLBufferData] = classOf[HLLBufferData]

  override def deserialize(datum: Any): HLLBufferData = {
    val bis = new ByteArrayInputStream(datum.asInstanceOf[Array[Byte]])
    val in = new ObjectInputStream(bis)
    in.readObject().asInstanceOf[HLLBufferData]
  }
}

case object HLLBufferType extends HLLBufferType