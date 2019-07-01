import org.apache.spark.SparkContext
import scala.math.min

object MinAgeName {
  
  def main(args:Array[String]){
      System.setProperty("hadoop.home.dir", "D:\\scala\\hadoop-winutils-2.6.0");
      val sc = new SparkContext("local[*]","filterExample");
      
      val txtFile = sc.textFile("d:\\scala_work\\samples\\name.txt", 2);
      def ageName(lines:String) = {
        val line = lines.split(",");
        (line(1).toString(),line(2).toInt);
      }
      
      val nameAge = txtFile.map(x=>ageName(x));
      
      def minAge(age1:Int, age2:Int):Int = {
        var minAge : Int = age1;
        if(age1>age2){
          minAge == age2;
        }
        return minAge;
      }
      val minAges = nameAge.reduceByKey((x,y) => min(x,y));
      
      minAges.foreach(println);
      
      //val minAges = nameAge.reduceByKey((x,y) => minAge(x,y));
     /* val minAges = nameAge.filter((x)=>minAge(x, y));
      minAges.foreach(println);*/
      
      val reverse = nameAge.map(x=> (x._2,x._1))
      val minAgeVal = reverse.max();
      
     println(minAgeVal);
  }
}