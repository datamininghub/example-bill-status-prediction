import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Reducer}

import scala.collection.mutable.ListBuffer

object Weight {

  class Reduce extends Reducer[Text, Text, Text, Weights] {
    val weights = new Weights()

    override def reduce(key: Text, values: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, Weights]#Context) = {
      val it = values.iterator()
      val targets = new ListBuffer[String]()

      while (it.hasNext) {
        targets += it.next().toString
      }

      if (targets.nonEmpty) {
        weights.weights = targets.toList.groupBy(a => a).mapValues(_.length)

        context.write(key, weights)
      }
    }
  }

  def run(cfg: Configuration, enrich: Path, weight: Path, debug: Boolean): Boolean = {
    val job: Job = new Job(cfg, f"Weight over enrich events: $enrich")
    job setJarByClass getClass

    job setInputFormatClass classOf[SequenceFileInputFormat[Text, Text]]
    job setOutputFormatClass classOf[SequenceFileOutputFormat[Text, Weights]]

    FileInputFormat addInputPath(job, enrich)
    FileOutputFormat setOutputPath(job, weight)

    job setMapOutputKeyClass classOf[Text]
    job setMapOutputValueClass classOf[Text]

    job setOutputKeyClass classOf[Text]
    job setOutputValueClass classOf[Weights]

    job setReducerClass classOf[Reduce]

    job waitForCompletion debug
  }

  def weight_path(temp: Path) = new Path(temp, "weight")

  def run(cfg: Configuration, driverConfig: Driver.Config): Boolean = {
    val compact_event = new Path(driverConfig.temp, "compact_event")
    val enrich = new Path(driverConfig.temp, "enrich")
    val weight = weight_path(driverConfig.temp)

    if (driverConfig.force) {
      compact_event.getFileSystem(cfg).delete(compact_event, true)
      enrich.getFileSystem(cfg).delete(enrich, true)
      weight.getFileSystem(cfg).delete(weight, true)
    }


    if (!CompactEvents.run(cfg, driverConfig.events, compact_event, empty_target = false, debug = driverConfig.debug))
      false
    else {
      if (!Enrich.run(cfg, compact_event, driverConfig.bill_deputy, enrich, driverConfig.debug))
        false
      else
        run(cfg, enrich, weight, driverConfig.debug)
    }
  }

}
