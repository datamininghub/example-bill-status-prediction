import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat, SequenceFileInputFormat, MultipleInputs}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Reducer, Mapper}

object Enrich {

  class RightMap extends Mapper[TextPair, NullWritable, Joiner.Value, Text] {
    val join_key = new Joiner.Value(Joiner.Type.Right)

    override def map(key: TextPair, value: NullWritable, context: Mapper[TextPair, NullWritable, Joiner.Value, Text]#Context) = {
      join_key.key.set(key.left)
      context.write(join_key, key.right)
    }
  }

  class LeftMap extends Mapper[LongWritable, Text, Joiner.Value, Text] {
    val join_key = new Joiner.Value(Joiner.Type.Left)
    val text = new Text()

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Joiner.Value, Text]#Context) = {
      val line = new CSVReader(new StringReader(value.toString)).readNext()
      if (line != null && line.size == 2) {
        join_key.key.set(line(0))
        text.set(line(1))
        context.write(join_key, text)
      }
    }
  }

  class Reduce extends Reducer[Joiner.Value, Text, Text, Text] {
    override def reduce(key: Joiner.Value, values: java.lang.Iterable[Text], context: Reducer[Joiner.Value, Text, Text, Text]#Context) = {
      val it = values.iterator()
      if (key.tw.data == Joiner.Type.Right && it.hasNext) {

        val target = new Text(it.next().toString)

        while (it.hasNext) {
          val key = it.next()
          context.write(key, target)
        }
      }
    }
  }

  def run(cfg: Configuration, events: Path, bill_deputy: Path, enrich: Path, debug: Boolean) = {
    val job: Job = new Job(cfg, f"Enrich events: $events use bill_deputy: $bill_deputy")
    job setJarByClass getClass

    MultipleInputs addInputPath(job, events, classOf[SequenceFileInputFormat[TextPair, NullWritable]], classOf[RightMap])
    MultipleInputs addInputPath(job, bill_deputy, classOf[TextInputFormat], classOf[LeftMap])

    job setOutputFormatClass classOf[SequenceFileOutputFormat[Text, Text]]

    FileInputFormat addInputPath(job, events)
    FileInputFormat addInputPath(job, bill_deputy)

    FileOutputFormat setOutputPath(job, enrich)

    job setMapOutputKeyClass classOf[Joiner.Value]
    job setMapOutputValueClass classOf[Text]

    job setOutputKeyClass classOf[Text]
    job setOutputValueClass classOf[Text]

    job setReducerClass classOf[Reduce]

    job setSortComparatorClass classOf[Joiner.Comparator]
    job setGroupingComparatorClass classOf[Joiner.Grouping]

    job waitForCompletion debug
  }
}
