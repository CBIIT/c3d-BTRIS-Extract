
public class Constants {
	public Constants(){
		
	}
	
	 //C3D Credential properties
	 //public static final String SYSTEM_ID= "C3D";
	 //public static final String DRIVER_CLASS_NAME= "oracle.jdbc.OracleDriver";
	 public static final String C3DURL = "jdbc:oracle:thin:@cbiodb2.nci.nih.gov:1521:OCdev";
	
	// Messages
	 public static final String VALID_USER_CREDENTIALS_MESSAGE = "Valid User Credentials.";
	 public static final String INVALID_USER_CREDENTIALS_MESSAGE = "Invalid User Credentials.";
	 public static final String UNABLE_TO_VERIFY_USER_CREDENTIALS_MESSAGE = "Unable to verify User Credentials.";
	 
	 // PRC: Properties
	 public static final String NameControlChkInterval   = "control.check.interval";
	 public static final String NameControlExtractHour   = "control.extract.hour.time";
	 public static final String NameControlExtractMinute = "control.extract.minute.time";
	 public static final String NameControlCumulativeDay = "control.cumulative.day";
	 public static final String NameControlOutputType    = "control.output.type";
	 public static final String NameControlBTRISURL      = "control.BTRIS.URL";
	 public static final String NameControlBTRISUser     = "control.BTRIS.user";
	 public static final String NameControlBTRISPass     = "control.BTRIS.pass";
	 public static final String NameControlC3DURL        = "control.C3D.URL";
	 public static final String NameControlC3DUser       = "control.C3D.user";
	 public static final String NameControlC3DPass       = "control.C3D.pass";
	 public static final String NameControlQOffsetDays   = "control.query.offset.days";
         public static final String NameControlSubmitPushC3DJob = "control.submit.pushC3D.job";

	 // PRC: Property descriptions
	 public static final String DescControlChkInterval   = "Interval in seconds for re-check of control file for changes";
	 public static final String DescControlCumulativeDay = "Day that Extractor retrieves Cumulative data. Otherwise incremental";
	 public static final String DescControlOutputType    = "Where Extract writes Output";
	 public static final String DescControlQOffsetDays   = "Query Date Offset Days for NEW Results";
	 public static final String DescControlExtractHour   = "Hour of the day at which to start extraction";
	 public static final String DescControlExtractMinute = "Minute of the Hour at which to start the extraction";
         public static final String DescControlSubmitPushC3DJob = "Automatically submit Oracle Job to Push Data to C3D";

	 // PRC: Default Properties Values
	 public static final String ControlFile = "BDE.properties";
	 public static final String ControlChkInterval = "300";   //5 seconds
	 public static final String ControlCumulativeDay = "1"; //Day of Cumulative Extraction: SUN,MON,TUE,WED,THU,FRI,SAT	
	 public static final String ControlOutputType = "FILE";   //Write output to: FILE, TABLE, BOTH
         public static final String ControlQOffsetDays = "-2";
	 public static final String ControlExtractHour   = "0"; // "0" = midnight
	 public static final String ControlExtractMinute = "30"; // "30" = on the half-hour
         public static final String ControlSubmitPushC3DJob = "true";
}
