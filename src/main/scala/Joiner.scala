import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{WritableComparator, Text, WritableUtils, WritableComparable}

object Joiner {

  object Type extends Enumeration {
    val Left, Right = Value
  }

  class TypeWritable(tp: Type.Value) extends WritableComparable[TypeWritable] {
    var data: Type.Value = tp

    override def compareTo(o: TypeWritable) =
      data.compare(o.data)

    override def write(out: DataOutput) =
      WritableUtils.writeString(out, data.toString)

    override def readFields(in: DataInput) =
      data = Type.withName(WritableUtils.readString(in))
  }

  case class Value(tw: TypeWritable, key: Text) extends WritableComparable[Value] {

    def this() =
      this(new TypeWritable(Joiner.Type.Left), new Text())

    def this(tp: Type.Value) =
      this(new TypeWritable(tp), new Text())

    override def write(out: DataOutput) = {
      tw.write(out)
      key.write(out)
    }

    override def readFields(in: DataInput) = {
      tw.readFields(in)
      key.readFields(in)
    }

    override def compareTo(o: Value) =
      key.compareTo(o.key)

    override def hashCode() = key.hashCode()
  }

  class Comparator extends WritableComparator(classOf[Value], true) {
    override def compare(a: WritableComparable[_], b: WritableComparable[_]) =
      (a.asInstanceOf[Value], b.asInstanceOf[Value]) match {
        case (kv1, kv2) =>
          kv1.compareTo(kv2) match {
            case 0 => kv2.tw.compareTo(kv1.tw)
            case res => res
          }
      }
  }

  class Grouping extends WritableComparator(classOf[Value], true) {
    override def compare(a: WritableComparable[_], b: WritableComparable[_]) =
      (a.asInstanceOf[Value], b.asInstanceOf[Value]) match {
        case (kv1, kv2) => kv1.compareTo(kv2)
      }
  }

}
