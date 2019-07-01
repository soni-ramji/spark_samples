import scala.collection.Map
import scala.io.Source

import org.apache.spark.SparkContext

object BroadcastExample {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "D:\\scala\\hadoop-winutils-2.6.0");
    val sc = new SparkContext("local[*]", "filterExample");

   

    val department = Source.fromFile("d:\\scala_work\\samples\\dept.txt").getLines();
    var deptMap: Map[Int, String] = Map();

   val abc =  department.map(x => {
      val deptName = x.toString().split(",");
     (deptName(0).toInt, deptName(1));
    })

    for(m <- abc){
      deptMap += (m._1.toInt->m._2)
    }
    val broadcastVal = sc.broadcast(deptMap)
    
    
     val txtFile = sc.textFile("d:\\scala_work\\samples\\namedept.txt", 2).map(x=> {
      val y = x.split(",")
       (y(1),broadcastVal.value(y(2).toInt))
     });
    
    for(x<-txtFile){
      println ("Name s "+ x._1+ " dept is "+ x._2);
    }
 sc.stop()
  }

}