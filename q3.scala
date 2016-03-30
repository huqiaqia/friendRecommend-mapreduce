println("please input two users' Id \n")
println("id1:")
val id1=readLine().toInt  
println("id2:")
val id2=readLine().toInt      
val in =sc.textFile("hdfs://cshadoop1/xxh142030/input/soc-LiveJournal1Adj.txt")
val set = Set(id1,id2)
val user = sc.textFile("hdfs://cshadoop1/xxh142030/input2/userdata.txt")


val list=in.flatMap(line=>line.split("\n")).filter(x=>set.contains(x.split("\t")(0).toInt)).filter(x=>x.split("\t").length>1)
	
if(list.count<2){
	println("No mutual friend")
}else{
	val ll = (list.flatMap(x=>x.split("\n")).map(x=>x.split("\t")(1)).flatMap(x=>x.split(",").map(x=>x.toInt))
		)
	val filter = ll.distinct.collect.toSet
	val result= (ll.collect.toSet--filter)
	if(result.size==0){
		println("No mutual friend")
	}else{
	val inf =( user.flatMap(line=>line.split("\n")).filter(x=>result.contains(x.split(",")(0).toInt))
		.map(x=>(x.split(","))).map(x=>(x(1),x(2),x(6))).collect.toList  
		)
	println(id1+":"+id2+" "+inf(0)+inf(1))  
	}
}
