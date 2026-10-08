import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object Day24 {

  def updateRunningCount(
      newValues: Seq[Int],
      runningCount: Option[Int]
  ): Option[Int] = {
    Some(newValues.sum + runningCount.getOrElse(0))
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Scala Spark Day 24 - Stateful and Stateless Streaming")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))

    println("===== DAY 24: STATELESS VS STATEFUL STREAMING =====")
    println("Batch interval: 5 seconds")
    println("Streaming source: TCP socket")
    println("Stateless: current-batch transaction counts")
    println("Stateful: running transaction counts by bank account")
    println("Send data in format: ACCOUNT_ID,AMOUNT")
    println("Example: ACC101,500")
    println("Press Ctrl+C to stop.")

    ssc.checkpoint("/tmp/scala-day24-checkpoint")

    val lines = ssc.socketTextStream("localhost", 9998)

    val transactions = lines.flatMap { line =>
      val parts = line.split(",").map(_.trim)

      if (parts.length == 2 && parts(1).toDoubleOption.isDefined) {
        Some((parts(0), parts(1).toDouble))
      } else {
        None
      }
    }

    // Stateless: counts only transactions received in the current batch.
    val currentBatchCounts = transactions
      .map { case (account, _) => (account, 1) }
      .reduceByKey(_ + _)

    currentBatchCounts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("----- STATELESS: CURRENT BATCH COUNTS -----")

        rdd.sortByKey().collect().foreach {
          case (account, count) =>
            println(s"$account -> $count transaction(s) in current batch")
        }
      }
    }

    // Stateful: maintains transaction counts across batches.
    val runningCounts: DStream[(String, Int)] =
      transactions
        .map { case (account, _) => (account, 1) }
        .updateStateByKey[Int](updateRunningCount _)

    runningCounts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("----- STATEFUL: RUNNING COUNTS -----")

        rdd.sortByKey().collect().foreach {
          case (account, count) =>
            println(s"$account -> $count total transaction(s)")
        }
      }
    }

    // Stateful: maintains running transaction amount by account.
    val runningAmounts: DStream[(String, Double)] =
      transactions.updateStateByKey[Double] {
        (newValues: Seq[Double], previous: Option[Double]) =>
          Some(newValues.sum + previous.getOrElse(0.0))
      }

    runningAmounts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("----- STATEFUL: RUNNING TRANSACTION AMOUNT -----")

        rdd.sortByKey().collect().foreach {
          case (account, amount) =>
            println(f"$account -> ₹$amount%.2f total amount")
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
