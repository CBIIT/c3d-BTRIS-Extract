<<<<<<< HEAD

public class Constants {
	public Constants(){
		
	}
	 // PRC: Properties
	 public static final String NameControlCumulativeDay = "control.cumulative.day";
	 public static final String NameControlOutputType    = "control.output.type";
	 public static final String NameControlQOffsetDays   = "control.query.offset.days";
	 public static final String NameControlMRNs          = "control.query.MRNs";
     public static final String NameControlSubmitPushC3DJob = "control.submit.pushC3D.job";
	 public static final String NameControlBTRISURL      = "control.BTRIS.URL";
	 public static final String NameControlBTRISUser     = "control.BTRIS.user";
	 public static final String NameControlBTRISPass     = "control.BTRIS.pass";
	 public static final String NameControlC3DURL        = "control.C3D.URL";
	 public static final String NameControlC3DUser       = "control.C3D.user";
	 public static final String NameControlC3DPass       = "control.C3D.pass";

	 // PRC: Property descriptions
     public static final String DescControlCumulativeDay = "Extractor query type. Either CUMULATIVE or INCREMENTAL";
	 public static final String DescControlOutputType    = "Where Extract writes Output: TABLE, FILE, or BOTH";
	 public static final String DescControlQOffsetDays   = "Query Date Offset Days for NEW Results. -2 means query beginning 2 days back. Ignored if CUMULATIVE.";
	 public static final String DescControlMRNs          = "Query data only for these MRNs. E.g. ('1111111','2222222')  Leave blank if you want all patients.";
     public static final String DescControlSubmitPushC3DJob = "Automatically move data to NCI_LABS and call the Lab Loader. If true, output type must be TABLE or BOTH.";

	 // PRC: Default Properties Values
	 public static final String ControlFile = "BDE.properties";
	 public static final String ControlCumulativeDay = "INCREMENTAL"; //CUMULATIVE or INCREMENTAL
	 public static final String ControlOutputType = "FILE";   //Write output to: FILE, TABLE, BOTH
	 public static final String ControlMRNs = null;
     public static final String ControlQOffsetDays = "-1";
     public static final String ControlSubmitPushC3DJob = "FALSE";
     public static final String ControlC3DURL = "jdbc:oracle:thin:@//cbiodb2.nci.nih.gov:1521/OCDEV.NCI.NIH.GOV";
     public static final String ControlBTRISURL = "jdbc:sqlserver://cc-btris-db-p.cc.nih.gov:1433";
}
=======

public class Constants {
	public Constants(){
		
	}
	 // PRC: Properties
	 public static final String NameControlCumulativeDay = "control.cumulative.day";
	 public static final String NameControlOutputType    = "control.output.type";
	 public static final String NameControlQOffsetDays   = "control.query.offset.days";
	 public static final String NameControlMRNs          = "control.query.MRNs";
     public static final String NameControlSubmitPushC3DJob = "control.submit.pushC3D.job";
	 public static final String NameControlBTRISURL      = "control.BTRIS.URL";
	 public static final String NameControlBTRISUser     = "control.BTRIS.user";
	 public static final String NameControlBTRISPass     = "control.BTRIS.pass";
	 public static final String NameControlC3DURL        = "control.C3D.URL";
	 public static final String NameControlC3DUser       = "control.C3D.user";
	 public static final String NameControlC3DPass       = "control.C3D.pass";

	 // PRC: Property descriptions
     public static final String DescControlCumulativeDay = "Extractor query type. Either CUMULATIVE or INCREMENTAL";
	 public static final String DescControlOutputType    = "Where Extract writes Output: TABLE, FILE, or BOTH";
	 public static final String DescControlQOffsetDays   = "Query Date Offset Days for NEW Results. -2 means query beginning 2 days back. Ignored if CUMULATIVE.";
	 public static final String DescControlMRNs          = "Query data only for these MRNs. E.g. ('1111111','2222222')  Leave blank if you want all patients.";
     public static final String DescControlSubmitPushC3DJob = "Automatically move data to NCI_LABS and call the Lab Loader. If true, output type must be TABLE or BOTH.";

	 // PRC: Default Properties Values
	 public static final String ControlFile = "BDE.properties";
	 public static final String ControlCumulativeDay = "INCREMENTAL"; //CUMULATIVE or INCREMENTAL
	 public static final String ControlOutputType = "FILE";   //Write output to: FILE, TABLE, BOTH
	 public static final String ControlMRNs = null;
     public static final String ControlQOffsetDays = "-1";
     public static final String ControlSubmitPushC3DJob = "FALSE";
     public static final String ControlC3DURL = "jdbc:oracle:thin:@//cbiodb2.nci.nih.gov:1521/OCDEV.NCI.NIH.GOV";
     public static final String ControlBTRISURL = "jdbc:sqlserver://cc-btris-db-p.cc.nih.gov:1433";
}
>>>>>>> origin/master
