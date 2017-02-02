

import org.apache.spark.{SparkContext, SparkConf}


object SparkWordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\Users\\vbmxb\\Documents\\hadoopwin\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input=sc.textFile("input")

    val wc=input.flatMap(line=>{line.split(" ")}).map(word=>(word,1)).cache()

    val results=wc.reduceByKey(_+_)

    val output=results.sortBy(_._2,false)

    output.saveAsTextFile("output2")

    val o=output.collect()

    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{

      s+=word+" : "+count+"\n"

    }}

  }

}
