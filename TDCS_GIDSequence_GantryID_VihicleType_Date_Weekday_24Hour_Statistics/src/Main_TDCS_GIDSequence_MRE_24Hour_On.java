/* jdwang@asia.edu.tw
 * 2016.12.3 For Parse TDCS_M06A	各旅次路徑原始資料
 * 交通資料蒐集支援系統 (Traffic Data Collection System,TDCS) 
 * http://tisvcloud.freeway.gov.tw/
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;

public class Main_TDCS_GIDSequence_MRE_24Hour_On {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		// "上"龍井交流道  (南下)03F1860S (北上)03F1779N
		// "下"龍井交流道  (南下)03F1779S (北上)03F1860N
		
		
		//String TargetGantryID = "03F1860S"; // "上"龍井交流道  (南下)03F1860S
		String TargetGantryID = "03F1779N"; // "上"龍井交流道  (北上)03F1779N
		//以{龍井}交流道為例：
		///{上交流道}：
		//｛從龍井南下｝: 如: "03F-186.0S"(國道三號 龍井-和美)=> GantryID="03F1860S"
		//｛從龍井北上｝: 如: "03F-177.9N"(國道三號 龍井-沙鹿)=> GantryID="03F1779N"
		//{下交流道}： (Target)
		//｛南下到龍井｝: 如: "03F-177.9S"(國道三號 沙鹿-龍井)=> GantryID="03F1779S"
		//｛北上到龍井｝: 如: "03F-186.0N"(國道三號 和美-龍井)=> GantryID="03F1860N"
			
		boolean useResourceManager = true;
		if(useResourceManager)
		{
			conf.set("fs.defaultFS", "hdfs://0.0.0.0:9000");
			conf.set("mapreduce.framework.name", "yarn");
			//conf.set("yarn.nodemanager.aux-services","mapreduce_shuffle");
			//conf.set("yarn.resourcemanager.hostname", "0.0.0.0");
			//conf.set("yarn.nodemanager.hostname", "0.0.0.0");	
			conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1.0");		
		}
		else
		{
			conf.set("fs.defaultFS", "file:///");
			conf.set("mapreduce.framework.name", "local");
			//conf.set("yarn.resourcemanager.address", "localhost");
			conf.set("hadoop.tmp.dir", "/windoop/tmp/hadoop-${user.name}");
		}
		
		Job job = Job.getInstance(conf, "TDCS_GIDSequence_MRE");
				
		job.setJarByClass(Main_TDCS_GIDSequence_MRE_24Hour_On.class);
		// TODO: specify a mapper
		//job.setMapperClass(Mapper_TDCS_GIDSequence_MRE_24Hour_SpecificGantryID_Weekday.class);
		job.setMapperClass(Mapper_TDCS_GIDSequence_MRE_24Hour_SpecificGantryID_Weekday_On.class);
		
			 
			
		job.setMapOutputKeyClass(Text.class);    
	    job.setMapOutputValueClass(IntWritable.class); 
		
		// TODO: specify a reducer
		job.setReducerClass(Reducer_TDCS_GIDSequence_MRE_24Hour.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		String InputDir = "2018_9_1-7";
		FileInputFormat.setInputPaths(job, new Path(InputDir));
		//FileInputFormat.setInputPaths(job, new Path("input_TDCS"));
		//FileInputFormat.setInputPaths(job, new Path("input_TDCS_24Hour"));
		//FileInputFormat.setInputPaths(job, new Path("2018_9_1-7"));
		//FileInputFormat.setInputPaths(job, new Path("2018_9_1-7"));		
		//FileInputFormat.setInputPaths(job, new Path("2018_9_8-14"));
		//FileInputFormat.setInputPaths(job, new Path("2018_9_15-21"));
		//FileInputFormat.setInputPaths(job, new Path("2018_9_22-28"));
		
		FileSystem hdfs = FileSystem.get(conf);	    	    
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+ "output_TDCS_24Hour_03F2100S_VehicleType");
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+ "output_TDCS_24Hour_ALL_VehicleType");
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+ "output_TDCS_24Hour_ALL_VehicleType_Hour");
		
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+ "output_VehicleType");
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+ "output_GantryID");
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+ "2018_9_22-28_"+TargetGantryID+"_Date_Weekday_24Hour");
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+ "2018_9_15-28_"+TargetGantryID+"_VehicleType_Date_Weekday_24Hour");
		//Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+ "2018_9_1-28_"+TargetGantryID+"_VehicleType_Date_Weekday_24Hour");
		Path outputPath = new Path(hdfs.getWorkingDirectory().toString() + "/"+InputDir+"_"+TargetGantryID+"_VehicleType_Date_Weekday_24Hour_On");
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setNumReduceTasks(1);
	    
	    if(hdfs.exists(outputPath)) { 
	    	hdfs.delete(outputPath, true);
	    } 
	    hdfs.close();
		
	    job.submit();
	   
		if(job.waitForCompletion(true))
	  	{
	  		System.out.println("Job Done!");
	  		System.exit(0);
	  	}
	  	else
	  	{
	  		System.out.println("Job Failed!");
	  		System.exit(1);
	  	}    
	}
}
