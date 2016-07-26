/**
 * 
 */
package cs455.hadoop.part2;

import java.io.IOException;
//
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author mbhavik
 *
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, CustomDataTypeSetter>{

	private static String summaryLevel = "100";
	//private static int fieldSize = 9;
	//private static long maleBelow18 = 0;
	//private static long maleBetween19And29 = 0;
	//private int startForMale = 3864;
	//private static long maleBetween30And39 = 0;
	//public static int startForMalePopulation = 3864;
	//private static long totalMalePopulation = 0;
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String singleRecord = value.toString();
		String extractedSummaryLevel = singleRecord.substring(10, 13);
		String state = singleRecord.substring(8, 10);
		Text states = new Text(state);
		
		if(extractedSummaryLevel.equals(summaryLevel)){
		
			String currentSegment = singleRecord.substring(24,28);
			CustomDataTypeSetter createObject = new CustomDataTypeSetter();
			switch (currentSegment) {
			case "0001":
				//For Q2
				String totalPopulation = singleRecord.substring(300, 309);
				String totalMale = singleRecord.substring(4422, 4431);
				String totalFemale = singleRecord.substring(4467, 4476);
				LongWritable tPopulation = new LongWritable(Long.parseLong(totalPopulation));
				LongWritable tMale = new LongWritable(Long.parseLong(totalMale));
				LongWritable tFemale = new LongWritable(Long.parseLong(totalFemale));
				createObject.setTotalPopulation(tPopulation);
				createObject.setTotalMales(tMale);
				createObject.setTotalFemales(tFemale);
				//For Q3
				//For males
				int startForMale = 3864;
				int fieldSize = 9;
				long maleBelow18 = 0;
				for (int i = 0; i < 13; i++) {
				
					maleBelow18 += Long.parseLong(singleRecord.substring(startForMale, (startForMale+fieldSize)));
					startForMale += fieldSize;
					
				}
				startForMale -= fieldSize;
				//Long pBelowEighteen = Long.parseLong(singleRecord.substring(3864, 3873))+Long.parseLong(singleRecord.substring(3873, 3882))+Long.parseLong(singleRecord.substring(3882, 3891))+Long.parseLong(singleRecord.substring(3891, 3900))+Long.parseLong(singleRecord.substring(3900, 3909))+Long.parseLong(singleRecord.substring(3909, 3918))+Long.parseLong(singleRecord.substring(3918, 3927))+Long.parseLong(singleRecord.substring(3927, 3936))+Long.parseLong(singleRecord.substring(3936, 3945))+Long.parseLong(singleRecord.substring(3945, 3954))+Long.parseLong(singleRecord.substring(3954, 3963))+Long.parseLong(singleRecord.substring(3963, 3972))+Long.parseLong(singleRecord.substring(3972, 3981));
				long maleBetween19And29 = 0;
				int startForMale2 = 3981;
				for (int i = 0; i < 5; i++) {
					maleBetween19And29 += Long.parseLong(singleRecord.substring(startForMale2, (startForMale2+fieldSize)));
					startForMale2 += fieldSize;
					
				}
				startForMale -= fieldSize;
				//Long pBelowNineteenAndTwentyNine = Long.parseLong(singleRecord.substring(3981, 3990))+Long.parseLong(singleRecord.substring(3990, 3999))+ Long.parseLong(singleRecord.substring(3999, 4008))+ Long.parseLong(singleRecord.substring(4008, 4017))+ Long.parseLong(singleRecord.substring(4017, 4026));
				long maleBetween30And39 = 0;
				int startForMale3 = 4026;
				for (int i = 0; i < 2; i++) {
					maleBetween30And39  += Long.parseLong(singleRecord.substring(startForMale3, (startForMale3+fieldSize)));
					startForMale3 += fieldSize;
					
				}
				int startForMalePopulation = 3864;
				long totalMalePopulation = 0;
				for (int i = 0; i < 31; i++) {
					totalMalePopulation  += Long.parseLong(singleRecord.substring(startForMalePopulation, (startForMalePopulation+fieldSize)));
					startForMalePopulation += fieldSize;
					
				}
				//totalMalePopulation = Long.parseLong(singleRecord.substring(3864, 3864+fieldSize))+Long.parseLong(singleRecord.substring(3873, 3873+fieldSize))+Long.parseLong(singleRecord.substring(3882, 3882+fieldSize))+Long.parseLong(singleRecord.substring(3891, 3891+fieldSize))+Long.parseLong(singleRecord.substring(3900, 3900+fieldSize))+Long.parseLong(singleRecord.substring(3909, 3909+fieldSize))+Long.parseLong(singleRecord.substring(3918,3918+fieldSize))+Long.parseLong(singleRecord.substring(3927, 3927+fieldSize))+Long.parseLong(singleRecord.substring(3936, 3936+fieldSize))+Long.parseLong(singleRecord.substring(3945, 3945+fieldSize))+Long.parseLong(singleRecord.substring(3954, 3954+fieldSize))+Long.parseLong(singleRecord.substring(3963, 3963+fieldSize))+Long.parseLong(singleRecord.substring(3972, 3972+fieldSize))+Long.parseLong(singleRecord.substring(3981, 3981+fieldSize))+Long.parseLong(singleRecord.substring(3990, 3990+fieldSize))+Long.parseLong(singleRecord.substring(3999, 3999+fieldSize))+Long.parseLong(singleRecord.substring(4008, 4008+fieldSize))+Long.parseLong(singleRecord.substring(4017, 4017+fieldSize))+Long.parseLong(singleRecord.substring(4026, 4026+fieldSize))+Long.parseLong(singleRecord.substring(4035, 4035+fieldSize))+Long.parseLong(singleRecord.substring(4044, 4044+fieldSize))+Long.parseLong(singleRecord.substring(4053, 4053+fieldSize))+Long.parseLong(singleRecord.substring(4062, 4062+fieldSize))+Long.parseLong(singleRecord.substring(4071, 4071+fieldSize))+Long.parseLong(singleRecord.substring(4080, 4080+fieldSize))+Long.parseLong(singleRecord.substring(4089, 4089+fieldSize))+Long.parseLong(singleRecord.substring(4098, 4098+fieldSize))+Long.parseLong(singleRecord.substring(4107, 4107+fieldSize))+Long.parseLong(singleRecord.substring(4116, 4116+fieldSize))+Long.parseLong(singleRecord.substring(4125, 4125+fieldSize))+Long.parseLong(singleRecord.substring(4134, 4134+fieldSize));
				
				//Long pBelowThirtyAndThirtyNine = Long.parseLong(singleRecord.substring(4026, 4035)) + Long.parseLong(singleRecord.substring(4035, 4044));
				LongWritable personBelowEighteen = new LongWritable(maleBelow18);
				LongWritable personBelowNineteenAndTwentyNine = new LongWritable(maleBetween19And29);
				LongWritable personBelowThirtyAndThirtyNine = new LongWritable(maleBetween30And39);
				LongWritable sumOfMalePopulation = new LongWritable(totalMalePopulation);
				createObject.setTotalMalePopulation(sumOfMalePopulation);
				createObject.setPersonBelowEighteen(personBelowEighteen);
				createObject.setPersonBelowNineteenAndTwentyNine(personBelowNineteenAndTwentyNine);
				createObject.setPersonBelowThirtyAndThirtyNine(personBelowThirtyAndThirtyNine);
				//For Females
				long femaleBelow18 = 0;
				int startForFemale = 4143;
				for (int i = 0; i < 13; i++) {
					femaleBelow18 += Long.parseLong(singleRecord.substring(startForFemale, (startForFemale+fieldSize)));
					startForFemale+=fieldSize;
				}
				long femaleBet19And29 = 0;
				int startForFemale2 = 4260;
				for (int i = 0; i < 5; i++) {
					femaleBet19And29 += Long.parseLong(singleRecord.substring(startForFemale2, startForFemale2+fieldSize));
					startForFemale2+=fieldSize;
				}
				long femaleBet30And39 = 0;
				int startForFemale3 = 4305;
				for (int i = 0; i < 2; i++) {
					femaleBet30And39 += Long.parseLong(singleRecord.substring(startForFemale3, startForFemale3+fieldSize));
					startForFemale3+=fieldSize;
				}
				
				
				
				long totalFemalePop = 0;
				int startForFemalePop = 4143;
				for (int i = 0; i < 31; i++) {
					totalFemalePop += Long.parseLong(singleRecord.substring(startForFemalePop, (startForFemalePop+fieldSize)));
					startForFemalePop+=fieldSize;
				}
				
				
				//Long pFBelowEighteen = Long.parseLong(singleRecord.substring(4143, 4152))+Long.parseLong(singleRecord.substring(4152, 4161))+Long.parseLong(singleRecord.substring(4161, 4170))+Long.parseLong(singleRecord.substring(4170, 4179))+Long.parseLong(singleRecord.substring(4179, 4188))+Long.parseLong(singleRecord.substring(4188, 4197))+Long.parseLong(singleRecord.substring(4197, 4206))+Long.parseLong(singleRecord.substring(4206, 4215))+Long.parseLong(singleRecord.substring(4215, 4224))+Long.parseLong(singleRecord.substring(4224, 4233))+Long.parseLong(singleRecord.substring(4233, 4242))+Long.parseLong(singleRecord.substring(4242, 4251))+Long.parseLong(singleRecord.substring(4251, 4260));
				//Long pFBelowNineteenAndTwentyNine = Long.parseLong(singleRecord.substring(4260, 4269))+Long.parseLong(singleRecord.substring(4269, 4278))+ Long.parseLong(singleRecord.substring(4278, 4287))+ Long.parseLong(singleRecord.substring(4287, 4296))+ Long.parseLong(singleRecord.substring(4296, 4305));
				//Long pFBelowThirtyAndThirtyNine = Long.parseLong(singleRecord.substring(4305, 4314)) + Long.parseLong(singleRecord.substring(4314, 4323));
				LongWritable femalePersonBelowEighteen = new LongWritable(femaleBelow18);
				LongWritable femalePersonBelowNineteenAndTwentyNine = new LongWritable(femaleBet19And29);
				LongWritable femalePersonBelowThirtyAndThirtyNine = new LongWritable(femaleBet30And39);
				LongWritable totalFemales = new LongWritable(totalFemalePop);
				
				createObject.setFemalePersonBelowEighteen(femalePersonBelowEighteen);
				createObject.setFemalePersonBelowNineteenAndTwentyNine(femalePersonBelowNineteenAndTwentyNine);
				createObject.setFemalePersonBelowThirtyAndThirtyNine(femalePersonBelowThirtyAndThirtyNine);
				createObject.setTotalFemalePopulation(totalFemales);
				
				//Question 8
				String lastQuestion = singleRecord.substring(795, 1074);
				Text lQuestion = new Text(lastQuestion);
				createObject.setLastQuestionSolution(lQuestion);
				
				context.write(states, createObject);
				
				
				break;

			case "0002":
				
				//Question1
				String owner = singleRecord.substring(1803, 1812);
				String rented = singleRecord.substring(1812, 1821);
				LongWritable tOwner = new LongWritable(Long.parseLong(owner));
				LongWritable tRented = new LongWritable(Long.parseLong(rented));
				createObject.setTotalOwner(tOwner);
				createObject.setTotalRented(tRented);
				
				//Question 4
				Long urban = Long.parseLong(singleRecord.substring(1857, 1866))+ Long.parseLong(singleRecord.substring(1866, 1875));
				String rural = singleRecord.substring(1875, 1884);
				String notDefined = singleRecord.substring(1884, 1893);
				LongWritable tUrban = new LongWritable(urban);
				LongWritable tRural = new LongWritable(Long.parseLong(rural));
				LongWritable nDefined = new LongWritable(Long.parseLong(notDefined));
				createObject.setTotalRural(tRural);
				createObject.setTotalUrban(tUrban);
				createObject.setNotDefined(nDefined);
				
				//Question 5
				String extractDetails =singleRecord.substring(2928, 3108);
				/*int start = 2928;
				int offset = 9;
				for (int i = 0; i < 20; i++) {
					extractDetails += )+"";
					start+=offset;
				}*/
				
				Text hOwners = new Text(extractDetails);
				createObject.setHouseOwner(hOwners);
				//Question 6
				String rentDetails = singleRecord.substring(3450, 3603);
				Text rentedDetails = new Text(rentDetails);
				createObject.setRentersDetails(rentedDetails);
				
				//Question 7
				String noOfRooms = singleRecord.substring(2388, 2469);
				Text noOfRums = new Text(noOfRooms);
				createObject.setRoomDetails(noOfRums);
				
				
				context.write(states, createObject);
				
				break;
			default:
				break;
			}
			
		}
		
		
	}

}
