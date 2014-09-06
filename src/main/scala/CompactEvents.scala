import java.io.StringReader
import java.lang.Iterable

import au.com.bytecode.opencsv.CSVReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Reducer, Job, Mapper}

object CompactEvents {
  class Map extends Mapper[LongWritable, Text, TextPair, NullWritable] {
    val pair = new TextPair()

    var empty_target: Boolean = false


    override def setup(context: Mapper[LongWritable, Text, TextPair, NullWritable]#Context): Unit = {
      super.setup(context)

      empty_target = context.getConfiguration.getBoolean("empty_target", empty_target)
    }

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, TextPair, NullWritable]#Context): Unit = {
      val line = new CSVReader(new StringReader(value.toString)).readNext()
      if (line != null && line.size == 8) {
        val target = line(7)
        pair.right.set(target)
        if ("".equals(target) == empty_target) {
          pair.left.set(line(1))
          context.write(pair, NullWritable.get())
        }
      }
    }
  }

  class Reduce extends Reducer[TextPair, NullWritable, TextPair, NullWritable] {
    override def reduce(key: TextPair, values: Iterable[NullWritable], context: Reducer[TextPair, NullWritable, TextPair, NullWritable]#Context): Unit = {
      context.write(key, NullWritable.get())
    }
  }

  def run(cfg: Configuration, events: Path, compact_event: Path, empty_target: Boolean, debug: Boolean) = {
    val job: Job = new Job(cfg, f"Compact event over events: $events")
    job setJarByClass getClass

    job setInputFormatClass classOf[TextInputFormat]
    job setOutputFormatClass classOf[SequenceFileOutputFormat[Text, Text]]

    FileInputFormat addInputPath(job, events)
    FileOutputFormat setOutputPath(job, compact_event)

    job setOutputKeyClass classOf[TextPair]
    job setOutputValueClass classOf[NullWritable]

    job setMapperClass classOf[Map]
    job setReducerClass classOf[Reduce]

    job.getConfiguration.setBoolean("empty_target", empty_target)

    job waitForCompletion debug
  }
}
