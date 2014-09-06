import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{WritableUtils, LongWritable, Text, Writable}

class Weights extends Writable {
  var weights: Map[String, Long] = Map()

  private lazy val text = new Text()
  private lazy val long = new LongWritable()

  override def write(out: DataOutput) = {
    WritableUtils.writeVInt(out, weights.size)
    for ((target, weight) <- weights) {
      text.set(target)
      long.set(weight)
      text.write(out)
      long.write(out)
    }
  }

  override def readFields(in: DataInput) = {
    val len = WritableUtils.readVInt(in)
    weights = Map()
    (1 to len).toList.map { id =>
      text.readFields(in)
      long.readFields(in)
      weights += (text.toString -> long.get())
    }
  }

  override def toString: String = weights.mkString(", ")
}
