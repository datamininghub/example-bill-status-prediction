import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, MultipleInputs, SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Reducer}

object AlgorithmEnrich {

  class Reduce extends Reducer[Joiner.Value, Text, Text, Text] {
    override def reduce(key: Joiner.Value, values: java.lang.Iterable[Text], context: Reducer[Joiner.Value, Text, Text, Text]#Context) = {
      val it = values.iterator()
      if (key.tw.data == Joiner.Type.Right && it.hasNext) {
        //skip target
        it.next()

        while (it.hasNext) {
          context.write(key.key, it.next())
        }
      }
    }
  }

  def run(cfg: Configuration, events: Path, bill_deputy: Path, enrich: Path, debug: Boolean) = {
    val job: Job = new Job(cfg, f"Algorithm enrich events: $events use bill_deputy: $bill_deputy")
    job setJarByClass getClass

    MultipleInputs addInputPath(job, events, classOf[SequenceFileInputFormat[TextPair, NullWritable]], classOf[Enrich.RightMap])
    MultipleInputs addInputPath(job, bill_deputy, classOf[TextInputFormat], classOf[Enrich.LeftMap])

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
