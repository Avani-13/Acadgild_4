import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

public class TvMapper
  extends Mapper<LongWritable, Text, NullWritable, Text> {
	
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {   
	String[] lineArray = value.toString().split("\\|");
		    if(lineArray[0].equals("NA") || (lineArray[1].equals("NA"))) {
	    context.write(NullWritable.get(), value); 
	}
  }
} 





