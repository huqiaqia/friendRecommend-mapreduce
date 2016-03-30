println("Please input two users' Id \n")
println("id1:")
val id1=readLine().toInt
println("id2:")
val id2=readLine().toInt

val in =sc.textFile("hdfs://cshadoop1/xxh142030/input/soc-LiveJournal1Adj.txt")
val set = Set(id1,id2)
val list=in.flatMap(line=>line.split("\n")).filter(x=>set.contains(x.split("\t")(0).toInt)).filter(x=>x.split("\t").length>1)
	
if(list.count<2){
	println("No mutual friend")
}else{
	val first = list.flatMap(x=>x.split("\n")).map(x=>x.split("\t")(1)).map(x=>x.split(",").map(x=>x.toInt)).collect
	val result = first(1).filter(x=>first(0).contains(x))
	result
}




