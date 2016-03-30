package FriendsRecommendation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class friendpost {

	public static class FriendsMapper extends
	Mapper<LongWritable, Text, Text, Text> {

		HashMap<String,String> map=null;
public void setup(Context context) throws IOException, InterruptedException {
	super.setup(context);
	//read data to memory on the mapper.
	map = new HashMap<>();
	Configuration conf = context.getConfiguration();
//	String myfilepath = conf.get("/socNetData/networkdata");
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
            //System.out.println(line);
            //do what you want with the line read
        	String[] strs=line.split(",");
        	String value=strs[1]+" "+strs[2]+":"+strs[6];
        	map.put(strs[0], value);
            line=br.readLine();
        }
       
    }
 
}
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	String twoFriend = context.getConfiguration().get("ARGUMENT");
	String[] twoFriends = twoFriend.split(",");
	
	String line = value.toString();
	String[] kv = line.split("\\t");
	
	String k = kv[0];
	String valuess = kv.length > 1 ? kv[1] : "";
	
	if (k.equals(twoFriends[0]) || k.equals(twoFriends[1])){
		String[] ids=valuess.split(",");
		String output="";
		for(String s:ids){
			output+=map.get(s)+",";
		}
		if(output.length()>0){
			output=output.substring(0,output.length()-1);
		}
		context.write(new Text(twoFriend), new Text(output));
		}
	}
}

	
public static class FriendsReducer extends Reducer<Text, Text, Text, Text> {

public void reduce(Text key, Iterable<Text> valuess, Context context)
		throws IOException, InterruptedException {

	Iterator<Text> values1 = valuess.iterator();
	StringBuilder sb = new StringBuilder();
	HashMap<String, Integer> count = new HashMap<String, Integer>();

	while (values1.hasNext()) {

		String[] values2 = values1.next().toString().split(",");
		for (String t : values2) {
			String id = t.toString();

			if (count.containsKey(id)) {
				count.put(id, count.get(id) + 1);
			} else {
				count.put(id, 1);
			}

			if (count.get(id) >= 2) {
				sb = sb.append(id);
				sb = sb.append(",");
			}
		}

	}
	context.write(key, new Text(sb.toString()));
}

}
	
	
	public static void main(String args[]) throws Exception {
		// Standard Job setup procedure.
		Configuration conf = new Configuration();
		FileSystem FS = FileSystem.get(conf);
		FS.delete(new Path(args[1]),true);
//		String[] otherArgs = new GenericOptionsParser(conf, args)
//				.getRemainingArgs();
		String twoFriend = args[2];
		conf.set("ARGUMENT", twoFriend);

		Job job = Job.getInstance(conf, "friendpost");
		job.setJarByClass(friendpost.class);
		job.setMapperClass(FriendsMapper.class);
		job.setReducerClass(FriendsReducer.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}