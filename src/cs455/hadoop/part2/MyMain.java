/**
 * 
 */
package cs455.hadoop.part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author mbhavik
 *
 */
public class MyMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub


		// TODO Auto-generated method stub
		try {

            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "Census Analysis");
            // Current class.
            job.setJarByClass(MyMain.class);
            // Mapper
            job.setMapperClass(MyMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
            //job.setCombinerClass(Combiner.class);
            job.setPartitionerClass(MyPartitioner.class);
            // Reducer
            job.setReducerClass(MyReducer.class);
            job.setNumReduceTasks(52);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(CustomDataTypeSetter.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileSystem fs1 = FileSystem.get(conf);
    		
    		String outputPath = args[1] + "";
    	
    		if(fs1.exists(new Path(outputPath)))
    			fs1.delete(new Path(outputPath), true);

    		FileOutputFormat.setOutputPath( job ,new Path(args[1]+""));
            //FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // Block until the job is completed.
            job.waitForCompletion(true);
        
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		try {


            Configuration conf1 = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf1, "Census Analysis-Part2");
            // Current class.
            job.setJarByClass(MyMain.class);
            // Mapper
            job.setMapperClass(MyMapper1.class);
            // Combiner. We use the reducer as the combiner in this case.
            //job.setCombinerClass(Combiner.class);
            //job.setPartitionerClass(MyPartitioner.class);
            // Reducer
            job.setReducerClass(MyReducer1.class);
            //job.setNumReduceTasks(52);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            // path to input in HDFS
            
            FileInputFormat.addInputPath(job, new Path(args[1]));
            // path to output in HDFS
            FileSystem fs1 = FileSystem.get(conf1);
    		
    		String outputPath1 = args[1] + "_lastTwoQuestions";
    	
    		if(fs1.exists(new Path(outputPath1)))
    			fs1.delete(new Path(outputPath1), true);

    		FileOutputFormat.setOutputPath( job ,new Path(outputPath1));
            //FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        
		
		} catch (Exception e) {
			// TODO: handle exception
		}
	
	}

}
