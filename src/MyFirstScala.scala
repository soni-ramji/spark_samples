import org.apache.spark.SparkContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object MyFirstScala {
  def main(args: Array[String]) {
    def log: Logger = LoggerFactory.getLogger(MyFirstScala.getClass);
    println("hello");

    log.info("hello again");

    System.setProperty("hadoop.home.dir", "D:\\scala\\hadoop-winutils-2.6.0");
    val sc = new SparkContext("local[*]", "myFirst");
    val txtFile = sc.textFile("d:\\scala_work\\samples\\name.txt", 2);
    println(txtFile.count())

    // Map Example

    // Map method work on each row and by a method.

    val ageRDD = txtFile.map((x) => (x.toString().split(",")(2)));
    println(ageRDD.countByValue());

    // MapbyValue and ReducebyKeyExample

    /*val ageFriendRDD = txtFile.map((lines) => {

      val fields = lines.split(",");
      val age = fields(2).toInt;
      val friend = fields(3).toInt;
      (age, friend);
    });*/

    def returnTuple(lines: String) = {
      val fields = lines.split(",");
      val age = fields(2).toInt;
      val friend = fields(3).toInt;
      (age, friend);
    }

    //val ageFriendRDD = txtFile.map(x => returnTuple(x));

    val ageFriendRDD = txtFile.map(returnTuple);
    val ageFriendMapbyValueRDD = ageFriendRDD.mapValues((x) => (x, 1));

    val ageFriendReducebyKey = ageFriendMapbyValueRDD.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2));
    val finalresult = ageFriendReducebyKey.mapValues((x) => (x._1 / x._2));

    finalresult.foreach(println);

    sc.stop();
  }
}