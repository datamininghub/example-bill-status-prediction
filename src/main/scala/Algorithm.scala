import java.io.StringWriter

import au.com.bytecode.opencsv.CSVWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, MultipleInputs, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.mutable.ListBuffer


object Algorithm {

  class LeftMap extends Mapper[Text, Text, Joiner.Value, TextWeightPair] {
    val join_key = new Joiner.Value(Joiner.Type.Left)
    val text_weight = new TextWeightPair()

    override def map(key: Text, value: Text, context: Mapper[Text, Text, Joiner.Value, TextWeightPair]#Context) = {
      join_key.key.set(value)
      text_weight.left.set(key)
      context.write(join_key, text_weight)
    }
  }

  class RightMap extends Mapper[Text, Weights, Joiner.Value, TextWeightPair] {
    val join_key = new Joiner.Value(Joiner.Type.Right)
    val text_weight = new TextWeightPair()

    override def map(key: Text, value: Weights, context: Mapper[Text, Weights, Joiner.Value, TextWeightPair]#Context) = {
      join_key.key.set(key)
      text_weight.right.weights = value.weights
      context.write(join_key, text_weight)
    }
  }


  class EnrichReduce extends Reducer[Joiner.Value, TextWeightPair, Text, Weights] {
    override def reduce(key: Joiner.Value, values: java.lang.Iterable[TextWeightPair], context: Reducer[Joiner.Value, TextWeightPair, Text, Weights]#Context) = {
      val it = values.iterator()
      val weights = new Weights()
      if (key.tw.data == Joiner.Type.Right && it.hasNext) {

        weights.weights = it.next().right.weights

        while (it.hasNext) {
          context.write(it.next().left, weights)
        }
      }
    }
  }

  def enrich(cfg: Configuration, enrich_bills: Path, weight: Path, enrich_algorithm: Path, debug: Boolean): Boolean = {
    val job: Job = new Job(cfg, f"Algorithm enrich job over enrich_bills: $enrich_bills use weight: $weight")
    job setJarByClass getClass

    MultipleInputs addInputPath(job, weight, classOf[SequenceFileInputFormat[Text, Weights]], classOf[RightMap])
    MultipleInputs addInputPath(job, enrich_bills, classOf[SequenceFileInputFormat[Text, Text]], classOf[LeftMap])

    job setOutputFormatClass classOf[SequenceFileOutputFormat[Text, Text]]

    FileInputFormat addInputPath(job, weight)
    FileInputFormat addInputPath(job, enrich_bills)

    FileOutputFormat setOutputPath(job, enrich_algorithm)

    job setMapOutputKeyClass classOf[Joiner.Value]
    job setMapOutputValueClass classOf[TextWeightPair]

    job setOutputKeyClass classOf[Text]
    job setOutputValueClass classOf[Weights]

    job setReducerClass classOf[EnrichReduce]

    job setSortComparatorClass classOf[Joiner.Comparator]
    job setGroupingComparatorClass classOf[Joiner.Grouping]

    job waitForCompletion debug
  }

  class Reduce extends Reducer[Text, Weights, Text, NullWritable] {
    override def reduce(key: Text, values: java.lang.Iterable[Weights], context: Reducer[Text, Weights, Text, NullWritable]#Context) = {
      val it = values.iterator()
      val weights = new ListBuffer[(String, Long)]()

      while (it.hasNext) {
        for ((target, weight) <- it.next().weights) {
          weights += ((target, weight))
        }
      }

      if (weights.nonEmpty) {
        val w = weights.groupBy(_._1).mapValues(_.map(_._2).sum)
        val s = w.values.sum.toDouble
        val target = w.mapValues(weight => weight.toDouble / s).maxBy(_._2)._1

        val writer = new StringWriter()
        new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, "")
          .writeNext(List(key.toString, target).toArray)

        writer.flush()
        writer.close()

        context.write(new Text(writer.toString), NullWritable.get())
      }
    }
  }


  def run(cfg: Configuration, enrich_algorithm: Path, output: Path, debug: Boolean): Boolean = {
    val job: Job = new Job(cfg, f"Algorithm job over enrich_algorithm: $enrich_algorithm ")
    job setJarByClass getClass

    job setInputFormatClass classOf[SequenceFileInputFormat[Text, Weights]]
    job setOutputFormatClass classOf[TextOutputFormat[Text, Text]]

    FileInputFormat addInputPath(job, enrich_algorithm)
    FileOutputFormat setOutputPath(job, output)

    job setMapOutputKeyClass classOf[Text]
    job setMapOutputValueClass classOf[Weights]

    job setOutputKeyClass classOf[Text]
    job setOutputValueClass classOf[NullWritable]

    job setReducerClass classOf[Reduce]

    job waitForCompletion debug
  }

  def run(cfg: Configuration, driverConfig: Driver.Config): Boolean = {
    val prediction_bills = new Path(driverConfig.temp, "prediction_bills")
    val enrich_bills = new Path(driverConfig.temp, "enrich_bills")
    val enrich_algorithm = new Path(driverConfig.temp, "enrich_algorithm")
    val weight = Weight.weight_path(driverConfig.temp)

    if (driverConfig.force) {
      prediction_bills.getFileSystem(cfg).delete(prediction_bills, true)
      enrich_bills.getFileSystem(cfg).delete(enrich_bills, true)
      enrich_algorithm.getFileSystem(cfg).delete(enrich_algorithm, true)
      driverConfig.output.getFileSystem(cfg).delete(driverConfig.output, true)
    }

    if (!CompactEvents.run(cfg, driverConfig.events, prediction_bills, empty_target = true, debug = driverConfig.debug))
      false
    else {
      if (!AlgorithmEnrich.run(cfg, prediction_bills, driverConfig.bill_deputy, enrich_bills, driverConfig.debug)) false
      else {
        if (!enrich(cfg, enrich_bills, weight, enrich_algorithm, driverConfig.debug)) false
        else run(cfg, enrich_algorithm, driverConfig.output, driverConfig.debug)
      }
    }
  }
}

