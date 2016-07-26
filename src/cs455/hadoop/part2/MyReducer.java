/**
 * 
 */
package cs455.hadoop.part2;
//
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author mbhavik
 *
 */
public class MyReducer extends Reducer<Text, CustomDataTypeSetter, Text, DoubleWritable>{
	
	private static long tPopulation = 0;
	private static long tFemale = 0;
	private static long tMale = 0;
	private static long tRented = 0;
	private static long tOwner = 0;
	private static long tUrban = 0;
	private static long tRural = 0;
	private static long mDefined = 0;
	private static long tBetween18 = 0;
	private static long tBetween19And29 = 0;
	private static long tBetween30And39 = 0;
	private static long sumOfOwnerAndRented = 0;
	private static long sumOfUrbanAndRural = 0;
	private static long tFemaleBetween18 = 0;
	private static long tFemaleBetween19And29 = 0;
	private static long tFemaleBetween30And39 = 0;
	private static long tMalePop = 0;
	private static long tFemalePop = 0;
	
	private static double maleNeverMarried = 0;
	private static double femaleNeverMarried = 0;
	private static double residenceRented= 0;
	private static double residenceOwned = 0;
	private static double percentOfUrban = 0;
	private static double percentOFRural = 0;
	private static double percentOFMalesBelow18 = 0;
	private static double percentOFMalesBetween19And29 = 0;
	private static double percentOfMalesBetween30And39 = 0;
	private static double percentOFFemalesBelow18 = 0;
	private static double percentOfFemalesBetween19And29 = 0;
	private static double percentOfFemalesBetween30And39 = 0;
	private static long sumOfAll = 0;
	private static long half = 0;
	private static int globalIndex = 0;
	private static long sumOfAll2 = 0;
	private static long half2 = 0;
	private static int globalIndex2 = 0;
	public static final int offset = 9;
	private static long total = 0;
	private static long denominator = 0;
	private static double currentStateAverage = 0;
	private static long totalPop = 0;
	private static long oldPopulation = 0;
	private static double percentageOfElderlyPopulation = 0;
	
	public static HashMap<Integer, String> houseOwnersList = new HashMap<Integer, String>();
	public static HashMap<Integer, String> rentersList = new HashMap<Integer, String>();
	
	
	long[] summation = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
	long[] summation2 = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
	long[] summation3 = {0,0,0,0,0,0,0,0,0};
	
	
	//NullWritable nullWritable = NullWritable.get();
	@Override
	protected void reduce(Text key, Iterable<CustomDataTypeSetter> value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for( CustomDataTypeSetter args : value){
			tMale += args.getTotalMales().get();
			tFemale += args.getTotalFemales().get();
			tPopulation += args.getTotalPopulation().get();
			tRented += args.getTotalRented().get();
			tOwner += args.getTotalOwner().get();
			tUrban += args.getTotalUrban().get();
			tRural += args.getTotalRural().get();
			mDefined += args.getNotDefined().get();
			tBetween18 += args.getPersonBelowEighteen().get();
			tBetween19And29 += args.getPersonBelowNineteenAndTwentyNine().get();
			tBetween30And39 += args.getPersonBelowThirtyAndThirtyNine().get();
			tFemaleBetween18 += args.getFemalePersonBelowEighteen().get();
			tFemaleBetween19And29 += args.getFemalePersonBelowNineteenAndTwentyNine().get();
			tFemaleBetween30And39 += args.getFemalePersonBelowThirtyAndThirtyNine().get();
			tMalePop += args.getTotalMalePopulation().get();
			tFemalePop += args.getTotalFemalePopulation().get();
			int start = 0;
			String temp = args.getHouseOwner().toString();
			if(temp.length() > 9)
			{
			for (int i = 0; i < 20; i++) {
				summation[i] += Long.parseLong(temp.substring(start, start+offset));
				start+=offset;
			}}
			int newStart = 0;
			String temp2 = args.getRentersDetails().toString();
			if(temp2.length() > 9){
				for (int i = 0; i < 17; i++) {
					summation2[i] += Long.parseLong(temp2.substring(newStart, newStart+offset));
					newStart += offset;
				}
			}
			int newStart2 = 0;
			String temp3 = args.getRoomDetails().toString();
			if(temp3.length() > 0){
				for (int i = 0; i < 9; i++) {
					summation3[i] += Long.parseLong(temp3.substring(newStart2, newStart2+offset));
					newStart2 += offset;
				}
			}
			
			String temp4 = args.getLastQuestionSolution().toString();
			int s = 0;
			if(temp4.length()>0){
				for (int i = 0; i < 31; i++) {
				totalPop += Long.parseLong(temp4.substring(s, s+offset));
				
				if (i== 30) {
					oldPopulation += Long.parseLong(temp4.substring(s,s+offset));
				}
				s+=offset;
			}
		}
		}

		houseOwnersList.put(0, "Less than $15,000");
		houseOwnersList.put(1, "$15,000 - $19,999");
		houseOwnersList.put(2, "$20,000 - $24,999");
		houseOwnersList.put(3, "$25,000 - $29,999");
		houseOwnersList.put(4, "$30,000 - $34,999");
		houseOwnersList.put(5, "$35,000 - $39,999");
		houseOwnersList.put(6, "$40,000 - $44,999");
		houseOwnersList.put(7, "$45,000 - $49,999");
		houseOwnersList.put(8, "$50,000 - $59,999");
		houseOwnersList.put(9, "$60,000 - $74,999");
		houseOwnersList.put(10, "$75,000 - $99,999 ");
		houseOwnersList.put(11, "$100,000 - $124,999");
		houseOwnersList.put(12, "$125,000 - $149,999");
		houseOwnersList.put(13, "$150,000 - $174,999");
		houseOwnersList.put(14, "$175,000 - $199,999");
		houseOwnersList.put(15, "$200,000 - $249,999");
		houseOwnersList.put(16, "$250,000 - $299,999");
		houseOwnersList.put(17, "$300,000 - $399,999");
		houseOwnersList.put(18, "$400,000 - $499,999");
		houseOwnersList.put(19, "$500,000 or more");
		
		rentersList.put(0, "Less than $100");
		rentersList.put(1, "$100 to $149");
		rentersList.put(2, "$150 to $199");
		rentersList.put(3, "$200 to $249");
		rentersList.put(4, "$250 to $299");
		rentersList.put(5, "$300 to $349");
		rentersList.put(6, "$350 to $399");
		rentersList.put(7, "$400 to $449");
		rentersList.put(8, "$450 to $499");
		rentersList.put(9, "$500 to $549");
		rentersList.put(10, "$550 to $ 599");
		rentersList.put(11, "$600 to $649");
		rentersList.put(12, "$650 to $699");
		rentersList.put(13, "$700 to $749");
		rentersList.put(14, "$750 to $999");
		rentersList.put(15, "$1000 or more");
		rentersList.put(16, "No cash rent");
		
		//Q1
		sumOfOwnerAndRented = tRented+tOwner;
		LongWritable totalRented = new LongWritable(tRented);
		LongWritable totalOwner = new LongWritable(tOwner);
		LongWritable sumOR = new LongWritable(sumOfOwnerAndRented);
		residenceRented = (totalRented.get()*100.00)/sumOR.get();
		residenceOwned = (totalOwner.get()*100.00)/sumOR.get();
		context.write(new Text("State is ---> "+key), new DoubleWritable());
		context.write(new Text("Q1"), new DoubleWritable());
		context.write(new Text("Percentage of residences rented:"), new DoubleWritable(residenceRented));
		context.write(new Text("Percentage of residences owned:"), new DoubleWritable(residenceOwned));
		
		//Question 2 Answer
		LongWritable receivedMale = new LongWritable(tMale);
		LongWritable receivedFemale = new LongWritable(tFemale);
		LongWritable receivedPopulation = new LongWritable(tPopulation);
		maleNeverMarried = (receivedMale.get()*100.00)/receivedPopulation.get();
		femaleNeverMarried = (receivedFemale.get()*100.00)/receivedPopulation.get();
		context.write(new Text("Q2"), new DoubleWritable());
		context.write(new Text("Percentage of male population never married is:"), new DoubleWritable(maleNeverMarried));
		context.write(new Text("Percentage of female population never married is:"), new DoubleWritable(femaleNeverMarried));
		
		//Question 4 Answer
		sumOfUrbanAndRural = tUrban+tRural+mDefined;
		LongWritable receivedUrban = new LongWritable(tUrban);
		LongWritable receivedRural = new LongWritable(tRural);
		LongWritable receivedSum = new LongWritable(sumOfUrbanAndRural);
		percentOFRural = (receivedRural.get()*100.00)/receivedSum.get();
		percentOfUrban = (receivedUrban.get()*100.00)/receivedSum.get();
		context.write(new Text("Q4"), new DoubleWritable());
		context.write(new Text("Distribution of Urban Household is :"), new DoubleWritable(percentOfUrban));
		context.write(new Text("Distribution of Rural Household is:"), new DoubleWritable(percentOFRural));
		
		//Question 3A, 3b, 3c
		//For Males
		LongWritable peopleBelow18 = new LongWritable(tBetween18);
		LongWritable peopleBetween19and29 = new LongWritable(tBetween19And29);
		LongWritable peopleBetween30and39 = new LongWritable(tBetween30And39);
		LongWritable totalMales = new LongWritable(tMalePop);
		percentOFMalesBelow18 = (peopleBelow18.get()*100.00)/totalMales.get();
		percentOFMalesBetween19And29 = (peopleBetween19and29.get()*100.00)/totalMales.get();
		percentOfMalesBetween30And39 = (peopleBetween30and39.get()*100.00)/totalMales.get();
		
		//For Females
		LongWritable femalePeopleBelow18 = new LongWritable(tFemaleBetween18);
		LongWritable femalePeopleBetween19and29 = new LongWritable(tFemaleBetween19And29);
		LongWritable femalePeopleBetween30and39 = new LongWritable(tFemaleBetween30And39);
		LongWritable totalFemale = new LongWritable(tFemalePop);
		percentOFFemalesBelow18 = (femalePeopleBelow18.get()*100.00)/totalFemale.get();
		percentOfFemalesBetween19And29 = (femalePeopleBetween19and29.get()*100.00)/totalFemale.get();
		percentOfFemalesBetween30And39 = (femalePeopleBetween30and39.get()*100.00)/totalFemale.get();
		context.write(new Text("Q3"), new DoubleWritable());
		context.write(new Text("Percent of males below 18----"), new DoubleWritable(percentOFMalesBelow18));
		context.write(new Text("Percent of males between 19 and 29"), new DoubleWritable(percentOFMalesBetween19And29));
		context.write(new Text("Percent of males between 30 and 39"), new DoubleWritable(percentOfMalesBetween30And39));
		
		context.write(new Text("Percent of females below 18---"), new DoubleWritable(percentOFFemalesBelow18));
		context.write(new Text("Percent of females between 19 and 29"), new DoubleWritable(percentOfFemalesBetween19And29));
		context.write(new Text("Percent of females between 30 and 39"), new DoubleWritable(percentOfFemalesBetween30And39));
		
		//Question 5
		
		for (int i = 0; i < summation.length; i++) {
			sumOfAll += summation[i];
		}
		
		half = sumOfAll/2;
		
		for (int i = 0; i < summation.length; i++) {
			half -= summation[i]; 
			if(half<0){
				globalIndex = i;
				break;
			}
			
		}
		context.write(new Text("Q5 "+houseOwnersList.get(globalIndex)), new DoubleWritable());
		
		//Question 6
		for (int i = 0; i < summation2.length; i++) {
			sumOfAll2 += summation2[i];
		}
		
		half2 = sumOfAll2/2;
		
		for (int i = 0; i < summation2.length; i++) {
			half2 -= summation2[i]; 
			if(half2<0){
				globalIndex2 = i;
				break;
			}
			
		}
		context.write(new Text("Q6 "+rentersList.get(globalIndex2)), new DoubleWritable());
		
		//Question 7
		
		for (int i = 0; i < 9; i++) {
			total += summation3[i];
		}
		int x = 1;
		for (int i = 0; i < 9; i++) {
			denominator += (x*summation3[i]);
			x++;
		}

		//currentStateAverage = (double) total/(double) denominator;
		currentStateAverage = (double) denominator/(double) total;
		context.write(new Text("Q7-"+key.toString()), new DoubleWritable(currentStateAverage));
		
		percentageOfElderlyPopulation = (double)(oldPopulation *100.00)/ totalPop;
		
		context.write(new Text("Q8-"+key.toString()), new DoubleWritable(percentageOfElderlyPopulation));
	
}
}
