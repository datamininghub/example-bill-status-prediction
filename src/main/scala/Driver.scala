import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.{Tool, ToolRunner}

object Driver extends Configured with Tool {

  val default_output = "output"
  val default_temp = "temp"


  case class Config(events: Path = new Path("fake"), bill_deputy: Path = new Path("fake"),
                    output: Path = new Path(default_output), temp: Path = new Path(default_temp),
                    debug: Boolean = false, force: Boolean = false)

  val parser = new scopt.OptionParser[Config]("bill-status-prediction.jar") {
    head("DMH example Prediction Russian State Duma bills future status", "1.0")
    opt[String]("events") required() action {
      (events, config) => config.copy(events = new Path(events))
    } text "path to events"
    opt[String]("bill_deputy") required() action {
      (bill_deputy, config) => config.copy(bill_deputy = new Path(bill_deputy))
    } text "path to bill_deputy"
    opt[String]('o', "output") action {
      (output, config) => config.copy(output = new Path(output))
    } text f"output folder, default: $default_output%s"
    opt[String]("temp") action {
      (temp, config) => config.copy(temp = new Path(temp))
    } text f"temp folder, default: $default_temp%s"
    opt[Unit]('d', "debug") action {
      (_, config) => config.copy(debug = true)
    }
    opt[Unit]('f', "force") action {
      (_, config) => config.copy(force = true)
    } text "force remove output when exists"
  }

  def run(args: Array[String]) = {
    parser.parse(args, Config()).fold(-1)({
      config =>
        if (!Weight.run(getConf, config)) -2
        else {
          if (!Algorithm.run(getConf, config)) -3
          else 0
        }
    })
  }

  def main(args: Array[String]) {
    System.exit(ToolRunner.run(new Configuration(), this, args))
  }

}