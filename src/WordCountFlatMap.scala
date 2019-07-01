import org.apache.spark.SparkContext
import org.slf4j.Logger


object WordCountFlatMap {
  
   def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\scala\\hadoop-winutils-2.6.0");
    val sc = new SparkContext("local[*]", "filterExample");

   
    val txtFile = sc.textFile("d:\\scala_work\\samples\\name.txt", 2);
    val totalWord = txtFile.flatMap((x)=> x.split(","));
    
    val countbyWord = totalWord.countByValue();
    println(totalWord.count());
    
  
    println("Count by Value is " + totalWord.countByValue());
    // sort by key example
    val countPerWord = totalWord.map((x)=>(x,1));
    val reduceByKey =  countPerWord.reduceByKey((x,y)=> (x+y));
    val reverse = reduceByKey.map(x => (x._2,x._1));
    val sort = reverse.sortByKey(true, 2);
    
    sort.foreach(println);
    
    
   }
}