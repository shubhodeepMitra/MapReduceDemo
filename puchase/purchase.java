package PackageDemo;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class purchase {
	
	/**
	 * constants for the purchase text file type
	 */
	public static final int DATE=0;
	public static final int TIME=1;
	public static final int CITY=2;
	public static final int ITEM=3;
	public static final int PRICE=4;
	public static final int TRANSACTION=5;
	

public static void main(String [] args) throws Exception

{

Configuration c=new Configuration();

String[] files=new GenericOptionsParser(c,args).getRemainingArgs();

Path input=new Path(files[0]);

Path output=new Path(files[1]);

Job j=new Job(c,"purchase");

j.setJarByClass(purchase.class);

j.setMapperClass(MapForPurchase.class);

j.setReducerClass(ReduceForPurchase.class);

j.setOutputKeyClass(Text.class);

j.setOutputValueClass(FloatWritable.class);

FileInputFormat.addInputPath(j, input);

FileOutputFormat.setOutputPath(j, output);

System.exit(j.waitForCompletion(true)?0:1);

}

public static class MapForPurchase extends Mapper<LongWritable, Text, Text, FloatWritable>{

public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException

{

String line = value.toString();

String[] words=line.split("\t");

	float v=Float.parseFloat(words[PRICE]);
	//extracting the city
    Text outputKey = new Text(words[CITY].trim());

  FloatWritable outputValue = new FloatWritable(v);

  con.write(outputKey, outputValue);



}

}

public static class ReduceForPurchase extends Reducer<Text, FloatWritable, Text, FloatWritable>

{

public void reduce(Text city, Iterable<FloatWritable> values, Context con) throws IOException, InterruptedException

{

float sum = 0;

   for(FloatWritable value : values)

   {

   sum +=(float) value.get();

   }

   con.write(city, new FloatWritable(sum));

}

}

}
