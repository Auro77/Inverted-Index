import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;

/**
 * Created by Aurobind on 3/20/18.
 */

public class WordCount{
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>{
		
        private Text word = new Text();
        
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String docID="";
            
			if(line.contains("\t")){
                    docID = tokenizer.nextToken().trim();
                }
				
            while(tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                context.write(word,new Text(docID));
            }
        }
	}
	
	public static class WordCountReducer extends Reducer<Text, Text, Text, Text>{
		
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			HashMap<String, Integer>  hm = new HashMap<String, Integer>();
			String result="";
        
			for(Text text : values){
				String entry = a.toString();
            
				if(hm.containsKey(entry)){ 
					int incr = hm.get(entry) + 1;
                    hm.put(entry,incr);
                }
				else{
					hm.put(entry,1);
				}
			}
			
			for(String s : hm.keySet()){
				result = result + k + ":" + hm.get(k).toString() + " ";
			}
			
			context.write(key, new Text(result));
        }
	}
	
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        
		if (args.length != 2){
            System.err.println("Usage: Word Count <input path> <output path>");
            System.exit(-1);
        }
        
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}