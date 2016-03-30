package FriendsRecommendation;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

 
public class FriendshipRecommender {
 
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] userAndFriends = line.split("\t");
            if (userAndFriends.length == 2) {
                String user = userAndFriends[0];
                IntWritable userKey = new IntWritable(Integer.parseInt(user));
                String[] friends = userAndFriends[1].split(",");
                String friend1;
                IntWritable friend1Key = new IntWritable();
                Text friend1Value = new Text();
                String friend2;
                IntWritable friend2Key = new IntWritable();
                Text friend2Value = new Text();
                for (int i = 0; i < friends.length; i++) {
                    friend1 = friends[i];
                    friend1Value.set("1," + friend1);
                    context.write(userKey, friend1Value);   // Paths of length 1.
                    friend1Key.set(Integer.parseInt(friend1));
                    friend1Value.set("2," + friend1);
                    for (int j = i+1; j < friends.length; j++) {
                        friend2 = friends[j];
                        friend2Key.set(Integer.parseInt(friend2));
                        friend2Value.set("2," + friend2);
                        context.write(friend1Key, friend2Value);   // Paths of length 2.
                        context.write(friend2Key, friend1Value);   // Paths of length 2.
                    }
                }
            }
        }
    } 
 
    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
            String userID = context.getConfiguration().get("arg");
            String[] userIDSplit = userID.split(",");
            
            HashMap<String, Integer> hash = new HashMap<String, Integer>();
            for (Text val : values) {
                value = (val.toString()).split(",");
                if (value[0].equals("1")) { // Paths of length 1.
                    hash.put(value[1], -1);
                } else if (value[0].equals("2")) {  // Paths of length 2.
                    if (hash.containsKey(value[1])) {
                        if (hash.get(value[1]) != -1) {
                            hash.put(value[1], hash.get(value[1]) + 1);
                        }
                    } else {
                        hash.put(value[1], 1);
                    }
                }
            }
            // Convert hash to list and remove paths of length 1.
            ArrayList<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>();
            for (Entry<String, Integer> entry : hash.entrySet()) {
                if (entry.getValue() != -1) {   // Exclude paths of length 1.
                    list.add(entry);
                }
            }
            // Sort key-value pairs in the list by values (number of common friends).
            Collections.sort(list, new Comparator<Entry<String, Integer>>() {
                public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {
                    return e2.getValue().compareTo(e1.getValue());
                }
            });
            int MAX_RECOMMENDATION_COUNT = 10;
            if (MAX_RECOMMENDATION_COUNT < 1) {
                // Output all key-value pairs in the list.
                context.write(key, new Text(StringUtils.join(",", list)));
            } else {
                // Output at most MAX_RECOMMENDATION_COUNT keys with the highest values (number of common friends).
                ArrayList<String> top = new ArrayList<String>();
                for (int i = 0; i < Math.min(MAX_RECOMMENDATION_COUNT, list.size()); i++) {
                	top.add(list.get(i).getKey());
                }
                for(int j = 0;j<userIDSplit.length;j++){
                	int item = Integer.parseInt(userIDSplit[j]);
                	if(key.get() == item){
                		context.write(key, new Text(StringUtils.join(",", top)));
                	}
                }
            }
        }
    }
 
    public static void main(String[] args) throws Exception { 
    	Configuration conf = new Configuration(); 
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args 
    	if (otherArgs.length != 3) { 
    		System.err.println("Usage: FriendshipRecommender <in> <out>"); 
    		System.exit(2); 
    		} // create a job with name "FriendshipRecommender" 
    	//delete output
    	FileSystem FS = FileSystem.get(conf);
    	FS.delete(new Path(otherArgs[1]),true);
    	
    	//save userId to get specific value
    	String userID = otherArgs[2];
    	conf.set("arg", userID);
    	
    	Job job = new Job  (conf,   "FriendshipRecommender"  ); 
    	job.setJarByClass(FriendshipRecommender.class); 
    	job.setMapperClass(Map.class); 
    	job.setReducerClass(Reduce.class); // uncomment the following line to add the Combiner 
  //  	job.setCombinerClass(Reduce.class);
    //set output key type 
    	job.setOutputKeyClass(IntWritable.class); // set output value type 
    	job.setOutputValueClass(Text.class); //set the HDFS path of the input data 
    	FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // set the HDFS path for the output 
    	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); //Wait till job completion 
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
    	}
}