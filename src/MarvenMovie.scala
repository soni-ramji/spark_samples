import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MarvenMovie {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "D:\\scala\\hadoop-winutils-2.6.0");
    val sc = new SparkContext("local[*]", "OptionExample");
    Logger.getLogger("MarvenMovie").setLevel(Level.ERROR);

    def parseName(lines: String): Option[(Int, String)] = {
      var line = lines.split('\"');
      if (line.length > 1) {
        return Some(line(0).trim().toInt, line(1).toString());
      } else {
        return None;
      }
    }

    def countCoOccurences(line: String) = {
      var elements = line.split("\\s+")
      (elements(0).toInt, elements.length - 1)
    }
    val txtFile = sc.textFile("D:\\scala_work\\samples\\SparkScala\\SparkScala\\Marvel-names.txt", 2)
    var heroName = txtFile.flatMap(parseName);
    
     val connection = sc.textFile("D:\\scala_work\\samples\\SparkScala\\SparkScala\\Marvel-graph.txt", 2)
    
    var heroconnection = connection.map(x => countCoOccurences(x));
     
     var totalFriend = heroconnection.reduceByKey((x,y)=> x+y).map(x => (x._2,x._1));
      var famous = totalFriend.max();
      
      //println("" + famous._1);
      
      println("name is " + heroName.lookup(famous._1)(0));
     
    
   /* for(hero <- heroName){
      println(hero);
    }*/

  }
}