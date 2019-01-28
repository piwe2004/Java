package sub2;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * ³¯Â¥ : 2019/01/28
 * °³¹ß : ±è¹Î±Ô
 * ³»¿ë : ÇÏµÓ ¸Ê¸®µà½º ´Ü¾î Ä«¿îÆ® ½Ç½ÀÇÏ±â
 */

public class WordCountMain {

	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		
		try {
			Job job = new Job(conf, "wordcount");
			
			job.setJarByClass(WordCountMain.class);
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		System.out.println("¸Ê¸®µà½º Á¾·á...");
	}
}
