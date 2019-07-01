import org.apache.spark.SparkContext

object MyFilter {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\scala\\hadoop-winutils-2.6.0");
    val sc = new SparkContext("local[*]", "filterExample");

    val txtFile = sc.textFile("d:\\scala_work\\samples\\name.txt", 2);

    val nameAge = txtFile.map((x) => {
      val splitted = x.toString().split(",");
      val name = splitted(1);
      val age = splitted(2).toInt;
      (name, age);
    });

    val greaterthan25 = nameAge.filter((x) => (x._2 > 25));
    greaterthan25.foreach(println);
    
    for(x<-greaterthan25){
      val name = x._1;
      val age = x._2;
      
      println("name is " + name + " age is "+ age );
    }

  }
}