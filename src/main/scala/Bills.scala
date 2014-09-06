import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{Text, Writable, WritableUtils}

import scala.collection.mutable.ListBuffer

class Bills extends Writable {
  var offered = new ListBuffer[String]()

  private lazy val text = new Text()

  override def write(out: DataOutput) = {
    WritableUtils.writeVInt(out, offered.size)
    for (o <- offered) {
      text.set(o)
      text.write(out)
    }
  }

  override def readFields(in: DataInput) = {
    offered.clear()
    val len = WritableUtils.readVInt(in)
    (1 to len).toList.map { id =>
      text.readFields(in)
      offered += text.toString
    }
  }

  override def toString: String = offered.mkString(", ")
}

