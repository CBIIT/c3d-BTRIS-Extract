import java.io.*;
import java.util.Properties;

public class ExtractLog {

	private String startDt;
	private String finishDt;
	private String queryDt;
	private String extractType;
	private String outputType;
	private int studyCnt;
	private int studyPatientCnt;
        private int uniquePatientCnt;
	private int resultCnt;
        private int oracleJobNumber;
        private String oracleJobLogName;

	public String getStartDt() {
		return startDt;
	}
	public String getFinishDt() {
		return finishDt;
	}
	public String getQueryDt() {
		return queryDt;
	}
	public String getExtractType() {
		return extractType;
	}
	public String getOutputType() {
		return outputType;
	}
	public int getStudyCnt() {
		return studyCnt;
	}
	public int getStudyPatientCnt() {
		return studyPatientCnt;
	}
	public int getUniquePatientCnt() {
		return uniquePatientCnt;
	}
	public int getResultCnt() {
		return resultCnt;
	}
	
	public int getOracleJobNumber() {
		return oracleJobNumber;
	}

	public String getOracleJobLogName() {
		return oracleJobLogName;
	}

        public void setStartDt(String inDate) {
		this.startDt = inDate;
	}
	public void setFinishDt(String inDate) {
		this.finishDt = inDate;
	}
	public void setQueryDt(String inDate) {
		this.queryDt = inDate;
	}
	public void setExtractType(String inType) {
		this.extractType = inType;
	}
	public void setOutputType(String inOut) {
		this.outputType = inOut;
	}
	public void setStudyCnt(int inCnt) {
		this.studyCnt = inCnt;
	}
	public void setStudyPatientCnt(int inCnt) {
		this.studyPatientCnt = inCnt;
	}
	public void setUniquePatientCnt(int inCnt) {
		this.uniquePatientCnt = inCnt;
	}
	public void setResultCnt(int inCnt) {
		this.resultCnt = inCnt;
	}
	public void setOracleJobNumber(int inCnt) {
		this.oracleJobNumber = inCnt;
	}
	public void setOracleJobLogName(String inCnt) {
		this.oracleJobLogName = inCnt;
	}
	public void getLastExtraction() {

           File infile = new File("BTRIS_Last_Extract.txt");
           if (!infile.exists()) {
               System.out.println("LastLogFile [BTRIS_Last_Extract.txt] does not exist. ");
               this.setStartDt(null);
           } else {
               InputStream inputStream = null;
               Properties properties = new Properties();

               try {
                   inputStream = new FileInputStream(infile);
               } catch (IOException e) {
                   e.printStackTrace();
               }
               if (inputStream != null) {
                   try {
                       properties.load(inputStream);
                       inputStream.close();
                   } catch (IOException e) {
                       e.printStackTrace();
                   }
               } else {
                   System.out.println("inputStream = null");
               }
               String propString = null;
               int propInt = 0;
               if (!properties.isEmpty()) {

                   if (properties.containsKey("ExtractStart")) {
                       this.setStartDt("");
                       propString = properties.getProperty("ExtractStart");
                       if (propString != null && propString.length() > 0) { //if empty or null
                          this.setStartDt(propString);
                       } 
                   }
                   if (properties.containsKey("ExtractFinish")) {
                       this.setFinishDt("");
                       propString = properties.getProperty("ExtractFinish");
                       if (propString != null && propString.length() > 0) { //if not empty or null
                           this.setFinishDt(propString);
                       }
                   }
                   if (properties.containsKey("ExtractQuery")) {
                       this.setQueryDt("");
                       propString = properties.getProperty("ExtractQuery");
                       if (propString != null && propString.length() > 0) { //if not empty or null
                           this.setQueryDt(propString);
                       }
                   }
                   if (properties.containsKey("ExtractType")) {
                       this.setExtractType("");
                       propString = properties.getProperty("ExtractType");
                       if (propString != null && propString.length() > 0) { //if not empty or null
                           this.setExtractType(propString);
                       }
                   }
                   if (properties.containsKey("OutputType")) {
                       this.setOutputType("");
                       propString = properties.getProperty("OutputType");
                       if (propString != null && propString.length() > 0) { //if not empty or null
                           this.setOutputType(propString);
                       }
                   }
                   if (properties.containsKey("TotalStudy")) {
                       this.setStudyCnt(0);
                       propString = properties.getProperty("TotalStudy");
                       if (propString != null && propString.length() > 0) { //if not empty or null
                           propInt = Integer.parseInt(propString);
                           this.setStudyCnt(Integer.parseInt(propString));
                       }
                   }
                   if (properties.containsKey("TotalStudyPatients")) {
                       this.setStudyPatientCnt(0);
                       propString = properties.getProperty("TotalStudyPatients");
                       if (propString != null && propString.length() > 0) { //if not empty or null
                           propInt = Integer.parseInt(propString);
                           this.setStudyPatientCnt(propInt);
                       }
                   }
                   if (properties.containsKey("TotalUniquePatients")) {
                       this.setUniquePatientCnt(0);
                       propString = properties.getProperty("TotalUniquePatients");
                       if (propString != null && propString.length() > 0) { //if not empty or null
                           propInt = Integer.parseInt(propString);
                           this.setUniquePatientCnt(propInt);
                       }
                   }
                   if (properties.containsKey("TotalResults")) {
                       this.setResultCnt(0);
                       propString = properties.getProperty("TotalResults");
                       if (propString != null && propString.length() > 0) { //if not empty or null
                           propInt = Integer.parseInt(propString);
                           this.setResultCnt(propInt);
                       }
                   }
               } else {

                   this.setStartDt("");
                   this.setFinishDt("");
                   this.setQueryDt("");
                   this.setExtractType("");
                   this.setStudyCnt(0);
                   this.setStudyPatientCnt(0);
                   this.setUniquePatientCnt(0);
                   this.setResultCnt(0);
                   this.setOracleJobNumber(0);
                   this.setOracleJobLogName("");
               }
           }
        }

        public boolean writeNewExtractLog() {

           try { //Write Last Extract File 
               FileWriter outFile = new FileWriter( "BTRIS_Last_Extract.txt", false );
               BufferedWriter outBuf = new BufferedWriter(outFile);
               outBuf.write("ExtractStart = \t\t" + getStartDt());
               outBuf.newLine();
               outBuf.write("ExtractFinish = \t" + getFinishDt());
               outBuf.newLine();
               outBuf.write("ExtractQuery = \t\t" + getQueryDt());
               outBuf.newLine();
               outBuf.write("ExtractType = \t\t" + getExtractType());
               outBuf.newLine();
               outBuf.write("OutputType = \t\t" + getOutputType());
               outBuf.newLine();
               outBuf.write("OracleJobNumber = \t" + getOracleJobNumber());
               outBuf.newLine();
               outBuf.write("OracleJobLogName = \t" + getOracleJobLogName());
               outBuf.newLine();
               outBuf.write("TotalStudy = \t\t" + getStudyCnt());
               outBuf.newLine();
               outBuf.write("TotalStudyPatient = \t" + getStudyPatientCnt());
               outBuf.newLine();
               outBuf.write("TotalUniquePatient = \t" + getUniquePatientCnt());
               outBuf.newLine();
               outBuf.write("TotalResults = \t\t" + getResultCnt());
               outBuf.newLine();
               outBuf.close();
           }
           catch(IOException ioexception)
           {
               ioexception.printStackTrace();
               return false;
           }
           return true;
        }
}
