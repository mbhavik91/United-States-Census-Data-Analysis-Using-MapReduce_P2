/**
 * 
 */
package cs455.hadoop.part2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author mbhavik
 *
 */
public class MyMapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String readLine = value.toString();
		if(readLine.contains("Q7")){
			String state = readLine.split("-")[1].split("\\s+")[0];
			double actualDouble = Double.parseDouble(readLine.split("\\s+")[1]);
			context.write(new Text("7"), new DoubleWritable(actualDouble));
		}else if (readLine.contains("Q8")) {
			String stateForQ8 = readLine.split("-")[1].split("\\s+")[0];
			double actualDoubleElderly = Double.parseDouble(readLine.split("\\s+")[1]);
			context.write(new Text("8"+"-"+stateForQ8), new DoubleWritable(actualDoubleElderly));
		}
	}
}
