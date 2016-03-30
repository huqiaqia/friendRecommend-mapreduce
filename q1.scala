def tuple(str:String) : Array[((Int,Int),Int)] = {
	val line=str.split("\t")
	val userId = line(0).toInt
	val friendId = line(1).split(",").map(x=>x.toInt)
	val notRecommend = friendId.map( x =>(userId,x)).map(x => (x,0))
	val recommend = friendId.map(x => friendId.map(y =>(x,y))).flatten.filter(x=>x._1 != x._2).map(x => (x,1))  
	Array(notRecommend,recommend).flatten
}
val input = Set(924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993)
val in =sc.textFile("hdfs://cshadoop1/xxh142030/input/1/soc-LiveJournal1Adj.txt")
val result=(in.flatMap(line=>line.split("\n")).filter(x=>x.split("\t").length>=2).map(x =>tuple(x))
	
	.flatMap(x=>x).reduceByKey((a,b) => if(a!=0 && b!=0) a+b else 0)
	.filter(k=>k._2>0).map(k=>(k._1._1,(k._1._2,k._2))).groupByKey().filter(x=>input.contains(x._1))
	.mapValues(x=>x.toList.sortWith(_._2>_._2).take(10)).collect.foreach(println)	
	) 



