import org.apache.spark.SparkContext


object TotalAmntSpent {
  
   def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\scala\\hadoop-winutils-2.6.0");
    val sc = new SparkContext("local[*]", "TotalAmountSpent");
    
     val txtFile = sc.textFile("d:\\scala_work\\samples\\nameCustomer.txt", 2);
      def nameAmount(lines:String) = {
        val line = lines.split(",");
        (line(1).toString(),line(2).toInt);
      }
      
     val data = txtFile.map((x=> nameAmount(x)));
     val finalData = data.reduceByKey((x,y)=> (x+y)).collect();
     finalData.foreach(println);
     
     
    
     
     
   }
}