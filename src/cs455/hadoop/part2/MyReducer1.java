/**
 * 
 */
package cs455.hadoop.part2;
//
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author mbhavik
 *
 */
public class MyReducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	private static ArrayList<Double> sortIt = new ArrayList<Double>();
	private static ArrayList<Double> sortItForElderly = new ArrayList<Double>();
	HashMap<Double, String> actualAnswer = new HashMap<Double, String>();
	private static int averageIndex = 0;
	private static int index = 0;
	private static int count = 0;
	@Override
	protected void reduce(Text arg0, Iterable<DoubleWritable> arg1, Context context)
			throws IOException, InterruptedException {
		if(arg0.toString().equals("7"))
		{
		// TODO Auto-generated method stub
		for (DoubleWritable a : arg1) {
			//double temp = Double.parseDouble(arg1.toString());
		
			//double temp = a.get();
			sortIt.add(a.get());
		}
		index = (int) (Math.round((95.00*52)/100)-1);
		Collections.sort(sortIt);
		context.write(new Text("The 95th percentile of the average number of rooms per house across all states is:"), new DoubleWritable(sortIt.get(index)));
		}
		else if (arg0.toString().split("-")[0].equals("8")) {
			for (DoubleWritable a : arg1) {
			
				sortItForElderly.add(a.get());
				actualAnswer.put(a.get(), arg0.toString().split("-")[1]);
				count++;
			}
			if(count == 51)
			{
			double maxValue = Collections.max(sortItForElderly);
			context.write(new Text("Highest Percentage of elderly people is : " + actualAnswer.get(maxValue)), new DoubleWritable(maxValue));
			}
		}
		
	
	}
}
