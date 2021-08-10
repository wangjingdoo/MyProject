/* jdwang@asia.edu.tw
 * 2016.12.3 For Parse TDCS_M06A	各旅次路徑原始資料
 * 交通資料蒐集支援系統 (Traffic Data Collection System,TDCS) 
 * http://tisvcloud.freeway.gov.tw/
 */
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Mapper_TDCS_GIDSequence_MRE_24Hour_SpecificGantryID_Weekday_Off extends org.apache.hadoop.mapreduce.Mapper <Object, Text, Text, IntWritable> {
	
	private String TargetGantryID = "03F1779S"; 	// "下"龍井交流道  (南下)03F1779S
	//String TargetGantryID = "03F1779N"; 			// "下"龍井交流道  (北上)03F1860N
	//private String Motion = "On";
	private String Motion = "Off";
	//private String Motion = "Pass";
	//以{龍井}交流道為例：
	///{上交流道}：
	//｛從龍井南下｝: 如: "03F-186.0S"(國道三號 龍井-和美)=> GantryID="03F1860S"
	//｛從龍井北上｝: 如: "03F-177.9N"(國道三號 龍井-沙鹿)=> GantryID="03F1779N"
	//{下交流道}：
	//｛南下到龍井｝: 如: "03F-177.9S"(國道三號 沙鹿-龍井)=> GantryID="03F1779S"
	//｛北上到龍井｝: 如: "03F-186.0N"(國道三號 和美-龍井)=> GantryID="03F1860N"
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	static String[] weekdays = { "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun" };
	
	public void setup(Context context)
 	{
	    	Configuration conf = context.getConfiguration();
	    	//Direction = conf.getInt("Direction", 1);
	    	//TargetGantryID = conf.get("TargetGantryID", "03F1860S");
 	}
	
	public static String date2Day( String dateString ) throws ParseException
	{
	    SimpleDateFormat dateStringFormat = new SimpleDateFormat( "yyyy-MM-dd" );
	    Date date = dateStringFormat.parse( dateString );
	    SimpleDateFormat date2DayFormat = new SimpleDateFormat( "u" );
	    return date2DayFormat.format(date);
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//31,2016-11-27 23:08:38,03F2415N,2016-11-27 23:26:32,03F2078N,36.400,Y,2016-11-27 23:08:38+03F2415N; 2016-11-27 23:12:50+03F2336N; 2016-11-27 23:14:37+03F2306N; 2016-11-27 23:18:42+03F2231N; 2016-11-27 23:20:31+03F2194N; 2016-11-27 23:22:53+03F2153N; 2016-11-27 23:24:22+03F2125N; 2016-11-27 23:25:30+03F2100N; 2016-11-27 23:26:32+03F2078N
		//31,2016-11-27 23:19:30,01H0447N,2016-11-27 23:32:11,01F0233N,29.300,Y,2016-11-27 23:19:30+01H0447N; 2016-11-27 23:26:08+01F0339N; 2016-11-27 23:28:43+01F0293N; 2016-11-27 23:30:46+01F0256N; 2016-11-27 23:32:11+01F0233N
				
		String line = value.toString();   
		String[] OneTranscationInfo = line.split(",");	 			
		String VehicleType = OneTranscationInfo[0]; //31		
		//String TimeStamp_On = OneTranscationInfo[1]; //2016-11-27 23:08:38
		//String GantryID_On = OneTranscationInfo[2]; // 03F2415N
		String TimeStamp_Off = OneTranscationInfo[3]; //2016-11-27 23:08:38
		String GantryID_Off = OneTranscationInfo[4]; // 03F2415N
		
		//String [] TimeStamp_Info = TimeStamp_On.split(" ");
		String [] TimeStamp_Info = TimeStamp_Off.split(" ");
		String Date = TimeStamp_Info[0]; //2016-11-27
		
		String [] Year_Month_Day = Date.split("-");
		
		String YearStr = Year_Month_Day[0];
		String MonthStr = Year_Month_Day[1];
		String DayStr = Year_Month_Day[2];
		
		
		String Hour_Min = TimeStamp_Info[1]; //23:08:38
		
		String [] Hour_Min_Info = Hour_Min.split(":");
		
		String Hour = Hour_Min_Info[0]; //"23":08:38
		
		/*
		 * Determine the weekday
		 */
		//String DateStr = TimeStamp_On.substring(0,10);
		String DateStr = TimeStamp_Off.substring(0,10);
		
		String WeekDayIndexStr = "";
		try {
			WeekDayIndexStr = date2Day(DateStr);
		} catch (ParseException e) {
			// TODO 
			e.printStackTrace();
		}    
		
		String Weekday = "Non";
		if (WeekDayIndexStr != "") {
			int weekday = Integer.valueOf(WeekDayIndexStr);
			if ((weekday>=1)&&( weekday <=7)) {
				Weekday = weekdays[weekday-1];
			}	
		} 
		
		//word.set(VehicleType);
		//String TargetKey = GantryID_On;
		//String TargetKey = VehicleType +"_"+ Hour;
		//String TargetKey = VehicleType ;
		//String TargetKey = GantryID_On +"_"+ Hour;
		//String TargetKey = Weekday+"_"+ Hour;
		//String TargetKey = DayStr+"_"+Weekday+"_"+ Hour;
		//String TargetKey = MonthStr+"_"+DayStr+"_"+Weekday+"_"+ Hour;
	    //String TargetKey = VehicleType+"_"+MonthStr+"_"+DayStr+"_"+Weekday+"_"+ Hour;
		//String TargetKey = TargetGantryID+"_"+VehicleType+"_"+MonthStr+"_"+DayStr+"_"+Weekday+"_"+ Hour;
		String TargetKey = TargetGantryID+"_"+Motion+"_"+VehicleType+"_"+MonthStr+"_"+DayStr+"_"+Weekday+"_"+ Hour;
		
		
		//String Target_GantryON = "03F1860S";
		String Target_GantryON = TargetGantryID;
		//if (GantryID_On.equals(Target_GantryON)){	
		if (GantryID_Off.equals(Target_GantryON)){	
			word.set(TargetKey);
			context.write(word, one);
		}
	}

}
