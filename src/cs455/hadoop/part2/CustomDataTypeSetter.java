/**
 * 
 */
package cs455.hadoop.part2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author mbhavik
 *
 */
public class CustomDataTypeSetter implements Writable {

	public Text getLastQuestionSolution() {
		return lastQuestionSolution;
	}

	public void setLastQuestionSolution(Text lastQuestionSolution) {
		this.lastQuestionSolution = lastQuestionSolution;
	}

	public Text getRoomDetails() {
		return roomDetails;
	}

	public void setRoomDetails(Text roomDetails) {
		this.roomDetails = roomDetails;
	}

	public Text getRentersDetails() {
		return rentersDetails;
	}

	public void setRentersDetails(Text rentersDetails) {
		this.rentersDetails = rentersDetails;
	}

	public Text getHouseOwner() {
		return houseOwner;
	}

	public void setHouseOwner(Text houseOwner) {
		this.houseOwner = houseOwner;
	}

	public LongWritable getTotalMalePopulation() {
		return totalMalePopulation;
	}

	public void setTotalMalePopulation(LongWritable totalMalePopulation) {
		this.totalMalePopulation = totalMalePopulation;
	}

	public LongWritable getTotalFemalePopulation() {
		return totalFemalePopulation;
	}

	public void setTotalFemalePopulation(LongWritable totalFemalePopulation) {
		this.totalFemalePopulation = totalFemalePopulation;
	}

	public LongWritable getFemalePersonBelowEighteen() {
		return femalePersonBelowEighteen;
	}

	public void setFemalePersonBelowEighteen(LongWritable femalePersonBelowEighteen) {
		this.femalePersonBelowEighteen = femalePersonBelowEighteen;
	}

	public LongWritable getFemalePersonBelowNineteenAndTwentyNine() {
		return femalePersonBelowNineteenAndTwentyNine;
	}

	public void setFemalePersonBelowNineteenAndTwentyNine(
			LongWritable femalePersonBelowNineteenAndTwentyNine) {
		this.femalePersonBelowNineteenAndTwentyNine = femalePersonBelowNineteenAndTwentyNine;
	}

	public LongWritable getFemalePersonBelowThirtyAndThirtyNine() {
		return femalePersonBelowThirtyAndThirtyNine;
	}

	public void setFemalePersonBelowThirtyAndThirtyNine(
			LongWritable femalePpersonBelowThirtyAndThirtyNine) {
		this.femalePersonBelowThirtyAndThirtyNine = femalePpersonBelowThirtyAndThirtyNine;
	}

	public LongWritable getPersonBelowEighteen() {
		return personBelowEighteen;
	}

	public void setPersonBelowEighteen(LongWritable personBelowEighteen) {
		this.personBelowEighteen = personBelowEighteen;
	}

	public LongWritable getPersonBelowNineteenAndTwentyNine() {
		return personBelowNineteenAndTwentyNine;
	}

	public void setPersonBelowNineteenAndTwentyNine(
			LongWritable personBelowNineteenAndTwentyNine) {
		this.personBelowNineteenAndTwentyNine = personBelowNineteenAndTwentyNine;
	}

	public LongWritable getPersonBelowThirtyAndThirtyNine() {
		return personBelowThirtyAndThirtyNine;
	}

	public void setPersonBelowThirtyAndThirtyNine(
			LongWritable personBelowThirtyAndThirtyNine) {
		this.personBelowThirtyAndThirtyNine = personBelowThirtyAndThirtyNine;
	}

	public LongWritable getNotDefined() {
		return notDefined;
	}

	public void setNotDefined(LongWritable notDefined) {
		this.notDefined = notDefined;
	}

	public LongWritable getTotalUrban() {
		return totalUrban;
	}

	public void setTotalUrban(LongWritable totalUrban) {
		this.totalUrban = totalUrban;
	}

	public LongWritable getTotalRural() {
		return totalRural;
	}

	public void setTotalRural(LongWritable totalRural) {
		this.totalRural = totalRural;
	}

	public LongWritable getTotalRented() {
		return totalRented;
	}

	public void setTotalRented(LongWritable totalRented) {
		this.totalRented = totalRented;
	}

	public LongWritable getTotalOwner() {
		return totalOwner;
	}

	public void setTotalOwner(LongWritable totalOwner) {
		this.totalOwner = totalOwner;
	}

	public LongWritable getTotalMales() {
		return totalMales;
	}

	public void setTotalMales(LongWritable totalMales) {
		this.totalMales = totalMales;
	}

	public LongWritable getTotalFemales() {
		return totalFemales;
	}

	public void setTotalFemales(LongWritable totalFemales) {
		this.totalFemales = totalFemales;
	}

	public LongWritable getTotalPopulation() {
		return totalPopulation;
	}

	public void setTotalPopulation(LongWritable totalPopulation) {
		this.totalPopulation = totalPopulation;
	}

	private LongWritable totalMales;
	private LongWritable totalFemales;
	private LongWritable totalPopulation;
	private LongWritable totalRented;
	private LongWritable totalOwner;
	private LongWritable totalUrban;
	private LongWritable totalRural;
	private LongWritable notDefined;
	private LongWritable personBelowEighteen;
	private LongWritable personBelowNineteenAndTwentyNine;
	private LongWritable personBelowThirtyAndThirtyNine;
	private LongWritable femalePersonBelowEighteen;
	private LongWritable femalePersonBelowNineteenAndTwentyNine;
	private LongWritable femalePersonBelowThirtyAndThirtyNine;
	private LongWritable totalMalePopulation;
	private LongWritable totalFemalePopulation;
	private Text houseOwner;
	private Text rentersDetails;
	private Text roomDetails;
	private Text lastQuestionSolution;
	
	
	public CustomDataTypeSetter() {
		// TODO Auto-generated constructor stub
		this.totalMales = new LongWritable();
		this.totalFemales = new LongWritable();
		this.totalPopulation = new LongWritable();
		this.totalRented = new LongWritable();
		this.totalOwner = new LongWritable();
		this.totalUrban = new LongWritable();
		this.totalRural = new LongWritable();
		this.notDefined = new LongWritable();
		this.personBelowEighteen = new LongWritable();
		this.personBelowNineteenAndTwentyNine = new LongWritable();
		this.personBelowThirtyAndThirtyNine = new LongWritable();
		this.femalePersonBelowEighteen = new LongWritable();
		this.femalePersonBelowNineteenAndTwentyNine = new LongWritable();
		this.femalePersonBelowThirtyAndThirtyNine = new LongWritable();
		this.totalMalePopulation = new LongWritable();
		this.totalFemalePopulation = new LongWritable();
		this.houseOwner = new Text();
		this.rentersDetails = new Text();
		this.roomDetails = new Text();
		this.lastQuestionSolution = new Text();
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		// TODO Auto-generated method stub
		totalMales.readFields(dataInput);
		totalFemales.readFields(dataInput);
		totalPopulation.readFields(dataInput);
		totalRented.readFields(dataInput);
		totalOwner.readFields(dataInput);
		totalRural.readFields(dataInput);
		totalUrban.readFields(dataInput);
		notDefined.readFields(dataInput);
		personBelowEighteen.readFields(dataInput);
		personBelowNineteenAndTwentyNine.readFields(dataInput);
		personBelowThirtyAndThirtyNine.readFields(dataInput);
		femalePersonBelowEighteen.readFields(dataInput);
		femalePersonBelowNineteenAndTwentyNine.readFields(dataInput);
		femalePersonBelowThirtyAndThirtyNine.readFields(dataInput);
		totalMalePopulation.readFields(dataInput);
		totalFemalePopulation.readFields(dataInput);
		houseOwner.readFields(dataInput);
		rentersDetails.readFields(dataInput);
		roomDetails.readFields(dataInput);
		lastQuestionSolution.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method stub
		totalMales.write(dataOutput);
		totalFemales.write(dataOutput);
		totalPopulation.write(dataOutput);
		totalRented.write(dataOutput);
		totalOwner.write(dataOutput);
		totalRural.write(dataOutput);
		totalUrban.write(dataOutput);
		notDefined.write(dataOutput);
		personBelowEighteen.write(dataOutput);
		personBelowNineteenAndTwentyNine.write(dataOutput);
		personBelowThirtyAndThirtyNine.write(dataOutput);
		femalePersonBelowEighteen.write(dataOutput);
		femalePersonBelowNineteenAndTwentyNine.write(dataOutput);
		femalePersonBelowThirtyAndThirtyNine.write(dataOutput);
		totalMalePopulation.write(dataOutput);
		totalFemalePopulation.write(dataOutput);
		houseOwner.write(dataOutput);
		rentersDetails.write(dataOutput);
		roomDetails.write(dataOutput);
		lastQuestionSolution.write(dataOutput);
	}

}
