import java.io.*;
import java.util.Properties;

public class ExtractLog {

	private String startDt;
	private String finishDt;
	private String queryDt;
	private String extractType;
	private String MRNs;
	private String outputType;
	private int resultCnt;
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
	public String getMRNs() {
		return MRNs;
	}

	public String getOutputType() {
		return outputType;
	}

	public int getResultCnt() {
		return resultCnt;
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
	public void setMRNs(String inType) {
		this.MRNs = inType;
	}

	public void setOutputType(String inOut) {
		this.outputType = inOut;
	}

	public void setResultCnt(int inCnt) {
		this.resultCnt = inCnt;
	}

	public void setOracleJobLogName(String inCnt) {
		this.oracleJobLogName = inCnt;
	}

	public boolean writeNewExtractLog() {

		try { // Write Last Extract File
			FileWriter outFile = new FileWriter("BTRIS_Last_Extract.txt", false);
			BufferedWriter outBuf = new BufferedWriter(outFile);
			outBuf.write("ExtractStart = \t\t" + getStartDt());
			outBuf.newLine();
			outBuf.write("ExtractFinish = \t" + getFinishDt());
			outBuf.newLine();
			outBuf.write("ExtractQuery = \t\t" + getQueryDt() + " Ignored if cumulative.");
			outBuf.newLine();
			outBuf.write("ExtractType = \t\t" + getExtractType());
			outBuf.newLine();
			outBuf.write("MRNs = \t\t" + getMRNs());
			outBuf.newLine();
			outBuf.write("OutputType = \t\t" + getOutputType());
			outBuf.newLine();
			outBuf.write("OracleJobLogName = \t" + getOracleJobLogName());
			outBuf.newLine();
			outBuf.write("TotalResults = \t\t" + getResultCnt());
			outBuf.newLine();
			outBuf.close();
		} catch (IOException ioexception) {
			ioexception.printStackTrace();
			return false;
		}
		return true;
	}
}
