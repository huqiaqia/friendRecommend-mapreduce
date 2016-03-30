def tuple(str:String) : Array[(Int,Int)] = {
	val line=str.split("\t")
	val userId = line(0).toInt
	val friendId = line(1).split(",").map(x=>x.toInt)
	val notRecommend = friendId.map( x =>(x,userId))
	notRecommend 
}

def ageGet(str:String,today:String) : (Int,Int) = {
	val line=str.split(",")
	val userId = line(0).toInt
	val age = agecacula(line(9),today)
	(userId,age)
}
def agecacula(str1:String, str2:String) : Int = {
	val date1 = str1.split("/").map(x=>x.toInt)
	val date2 = str2.split("/").map(x=>x.toInt)
	val today= if(date1(2) >date2(2)) date1 else date2
	val birthday = if(date1(2) <=date2(2)) date1 else date2	
	val age = today(2)-birthday(2)
	if(today(0)<birthday(0)||today(0)==birthday(0)&&today(1)<birthday(1)){
		return age-1		
	}	
	age
}
def addressGet(str:String) : (Int,String) = {
	val line=str.split(",")
	val userId = line(0).toInt
	val address=line(3)+","+line(4)+","+line(5)
	(userId,address)
}

import java.util.Calendar
import java.text.SimpleDateFormat
val date = Calendar.getInstance.getTime
val Format = new SimpleDateFormat("MM/dd/yyyy")
val todayDate = Format.format(date)
val in =sc.textFile("hdfs://cshadoop1/xxh142030/input/1/soc-LiveJournal1Adj.txt")
val user = sc.textFile("hdfs://cshadoop1/xxh142030/input/2/userdata.txt")

val ageInf = user.flatMap(line=>line.split("\n")).map(x=>ageGet(x,todayDate))
val addressInf = user.flatMap(line=>line.split("\n")).map(x => addressGet(x) )
val friendUser=( in.flatMap(line=>line.split("\n")).filter(x=>x.split("\t").length>=2).map(x =>tuple(x)).flatMap(x=>x)
		.join(ageInf).map(k=>(k._2._1,(k._2._2,1))).reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
		.map(x=>(x._1,x._2._1/x._2._2))
		.map(x=>(1,(x._1,x._2))).groupByKey().mapValues(x=>x.toList.sortWith(_._2>_._2).take(20)) 
		.flatMap(x=>x._2.map(y=>(y._1,y._2)))
		)
val addressInf =( user.flatMap(line=>line.split("\n")).map(x => addreGet(x) ).join(friendUser)
		.collect.foreach(x=>println(x._1+","+x._2._1+","+x._2._2))
		)
