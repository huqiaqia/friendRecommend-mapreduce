package FriendsRecommendation;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class Question2 {
	public static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String twoFriend = context.getConfiguration().get("ARGUMENT");
			String []friendItem = twoFriend.split(",");
			
			//readline doc
			String line = value.toString();
			String []item = line.split("\\t");
			
			String user = item[0];
			String others = item.length>1?item[1]:"";
			
			//finish match with input
			if(user.equals(friendItem[0]) || user.equals(friendItem[1])){
					context.write(new Text(twoFriend), new Text(others));
			}
		}
	}
	
	
	public static class FriendReducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			Text result = new Text();
			Iterator<Text> valueItem = values.iterator();
			StringBuilder sb = new StringBuilder();
		    Map<String,Integer> count = new HashMap<String,Integer>();
		    
		    while(valueItem.hasNext()){
		    	String []level = valueItem.next().toString().split(",");
		    	for(String s:level){
		    		if(count.containsKey(s)){
		    			count.put(s, count.get(s)+1);
		    		}
		    		else{
		    			count.put(s, 1);
		    		}
		    		
		    		if(count.get(s)!=1){
		    			sb.append(s+",");
		    		}
		    	}
		    }
		    result.set(sb.toString());
			context.write(key, result);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		FileSystem FS = FileSystem.get(conf);
		FS.delete(new Path(args[1]),true);
		String twoFriends = args[2];
		conf.set("ARGUMENT",twoFriends);
		
		Job job = new Job(conf, "Friend_Mutation");
		job.setJarByClass(Question2.class);
		job.setMapperClass(FriendMapper.class);
		job.setReducerClass(FriendReducer.class);
		
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
