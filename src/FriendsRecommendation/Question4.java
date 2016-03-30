package FriendsRecommendation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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



public class Question4 {
	
	private static int N = 20 ;
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		HashMap<String,String> map = null;
		HashMap<String,String> address = null;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//String inputUser = context.getConfiguration().get("ARGUMENT");
			//String []friendItem = twoFriend.split(",");
			
			//readline doc
			String line = value.toString();
			String []item = line.split("\\t");
			
			String user = item[0];//userid
			String others = item.length>1?item[1]:"";
			if(others.length()>0){
				String ids[] = others.split(",");
				String output = "";
				for(String s:ids){
					 output+=map.get(s)+",";
				}
				if(output.length()>0){
					 output=output.substring(0,output.length()-1);
					 if(!address.get(user).equals(null)){
						 user = user+","+address.get(user);
						 context.write(new Text(user), new Text(output)); 
					 }
					 else{
						 context.write(new Text(user), new Text(output)); 
					 }
				}
			}
			else{
					context.write(new Text(user), new Text("999")); 
			}
			
			
		}
		
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			//read data to memory on the mapper.
			map = new HashMap<String,String>();
			address = new HashMap<String,String>();
			Configuration conf = context.getConfiguration();
			//e.g /user/hue/input/
			Path part=new Path("/xxh142030/input/2/userdata.txt");//Location of file in HDFS
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] arr=line.split(",");
		        	
		        	String ageValue[] = arr[9].split("/");
		        	int age = 2016 - Integer.parseInt(ageValue[ageValue.length-1]);
		        	map.put(arr[0], String.valueOf(age));
		        	
		        	String addressValue = arr[3]+","+arr[4]+","+arr[5];
		        	address.put(arr[0], addressValue);
		        	 line=br.readLine();
		        }
		    }
		}
	}	
	
	

	
	


public static class Reduce extends Reducer<Text,Text,Text,Text> {
	private TreeMap<String, String> treeMap = new TreeMap<>();
	public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
		//Text result = new Text();
		String result = new String();
		int meanAge = 0;
		Iterator<Text> valueItem = values.iterator();
		while(valueItem.hasNext()){
			String level[] = valueItem.next().toString().split(",");
			meanAge = CacaulaMean(level);
		}

		result = String.valueOf(meanAge);
		//context.write(key, result);
		treeMap.put(result, key.toString());
	}
	@Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {
		NavigableMap<String, String> descMap=treeMap.descendingMap();
		
		int count=0;
        for (String key:descMap.keySet()) {
        	if (count++ == 20) {
                break;
            }
        	context.write(new Text(descMap.get(key)),new Text(key));
        }
        //super.cleanup(context); // 
	}
	
	
	public static int CacaulaMean(String []s){
			int sum = 0;
			for(String i:s){
				sum+= Integer.parseInt(i);
			}
			return sum/s.length;
	}
}



//Driver program
public static void main(String[] args) throws Exception {
	// Standard Job setup procedure.
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();
			// get all args
			if (otherArgs.length != 2) {
				System.err.println("Usage: WordCount <in> <out>");
				System.exit(2);
			}
			FileSystem FS = FileSystem.get(conf);
			FS.delete(new Path(args[1]),true);
			
			//String inputUser = args[2];
			//conf.set("ARGUMENT", inputUser);

			Job job = Job.getInstance(conf, "friendpost");
			job.setJarByClass(Question4.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
