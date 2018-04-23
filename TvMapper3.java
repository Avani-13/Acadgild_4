import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;


public class TvMapper3
  extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	 private final static IntWritable one = new IntWritable(1);	
	 private final static IntWritable zero = new IntWritable();
	 Text Company;
	 Text State;
	 public void setup(Context context)
	 {
		 State = new Text();
		 Company = new Text();
	 }
	public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException 
{   
	String[] lineArray = value.toString().split("\\|");
	State.set(lineArray[3]);
	Company.set(lineArray[0]);
	if(Company.equals("Onida"))
	{
	    context.write(State, one); 
	}
		else
		{
		//Text t2=new Text(lineArray[3]);
		context.write(State, zero);
		}
	}
}
	