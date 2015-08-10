/**
 * 
 */
package cs455.hadoop.part2;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author mbhavik
 *
 */
public class MyPartitioner extends Partitioner<Text, CustomDataTypeSetter>{

	public static String[] statesAbbreviation = {"AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "VI","DC"};
    public static ArrayList<String> statesName = new ArrayList<String>(Arrays.asList(statesAbbreviation));
	@Override
	public int getPartition(Text key, CustomDataTypeSetter val, int arg2) {
		// TODO Auto-generated method stub
		String receivedStatesFromMapper = key.toString();
			
			return statesName.indexOf(receivedStatesFromMapper);
	}
}
