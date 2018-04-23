import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

@SuppressWarnings("unused")
public class TvMapper2
  extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	  private final static IntWritable one = new IntWritable(1);	
	  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException 
	  {   
	String[] lineArray = value.toString().split("\\|");
	
	Text Company= new Text(lineArray[0]);
	if(!(lineArray[0].equals("NA")))
	{
	context.write(Company, one); 
	}
	  }
}
	