import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{Writable, Text, WritableComparable}

class TextPair extends WritableComparable[TextPair] {

  val left = new Text()
  val right = new Text()

  override def compareTo(o: TextPair): Int = {
    val res = left.compareTo(o.left)
    if (res != 0) res
    else right.compareTo(o.right)
  }

  override def write(out: DataOutput): Unit = {
    left.write(out)
    right.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    left.readFields(in)
    right.readFields(in)
  }

  override def toString: String =
    f"left: ${left.toString}, right: ${right.toString}"
}

class TextWeightPair extends Writable {

  val left = new Text()
  val right = new Weights()

  override def write(out: DataOutput): Unit = {
    left.write(out)
    right.write(out)
  }

  override def readFields(in: DataInput): Unit = {
    left.readFields(in)
    right.readFields(in)
  }

  override def toString: String =
    f"left: ${left.toString}, right: ${right.toString}"
}