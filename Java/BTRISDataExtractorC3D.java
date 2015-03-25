import java.sql.*;
import java.util.logging.*;
import java.io.*;
import java.util.Date;
import java.util.Calendar;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.Iterator;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

public class BTRISDataExtractorC3D {
	private static String urlBTRIS, userBTRIS, passwordBTRIS = null;
	private static String urlC3D, userC3D, passwordC3D = null;
	private static String extractType, offsetDaysStr, outputType, submitPushC3DJob = null;


	private static ExtractLog submitC3DPush(ExtractLog extractLog) {
		String connectionC3DUrl = urlC3D;
		String connectionC3DUser = userC3D;
		String connectionC3DPass = passwordC3D;
		Connection conC3D = null;
		CallableStatement stmtC3D = null;
		ResultSet rsC3D = null;
		SimpleDateFormat dateFormatSQLServer = new SimpleDateFormat(
				"yyMMdd_HH:mm:ss");
		String dateStr = dateFormatSQLServer.format(Calendar.getInstance()
				.getTime());
		int oracleJobNumber = 0;
		String oracleLogName = "BTRIS_PUSHC3D" + dateStr;

		try {
			// Establish the connection to C3D
			Class.forName("oracle.jdbc.driver.OracleDriver");
			// System.out.println("Get Connection to C3D.");
			conC3D = DriverManager.getConnection(connectionC3DUrl,
					connectionC3DUser, connectionC3DPass);
			// System.out.println("Connected to C3D.");

			String SQLC3D = "{call btris_data_transfer.Submit_PULL_AND_PUSH_LABS(?, ?)}";

			stmtC3D = conC3D.prepareCall(SQLC3D);
			stmtC3D.setInt(1, 0);
			stmtC3D.setString(2, oracleLogName);
			stmtC3D.registerOutParameter(1, java.sql.Types.INTEGER);
			stmtC3D.registerOutParameter(2, java.sql.Types.VARCHAR);
			stmtC3D.executeUpdate();
			oracleJobNumber = stmtC3D.getInt(1);
			oracleLogName = stmtC3D.getString(2);

			// System.out.println("Showing the results:");
			// System.out.println("Oracle Job Number=" + oracleJobNumber);
			// System.out.println("Oracle Log Name='" + oracleLogName + "'");

			System.out.println("Submitted BTRIS PushC3D Job, "
					+ oracleJobNumber + ", LogName: '" + oracleLogName + "'.");
		} catch (Exception e) {
			e.printStackTrace();
		}

		finally {
			if (rsC3D != null)
				try {
					rsC3D.close();
				} catch (Exception e) {
				}
			if (stmtC3D != null)
				try {
					stmtC3D.close();
				} catch (Exception e) {
				}
			if (conC3D != null)
				try {
					conC3D.close();
				} catch (Exception e) {
				}
		}
		extractLog.setOracleJobNumber(oracleJobNumber);
		extractLog.setOracleJobLogName(oracleLogName);

		return extractLog;
	}

	private static void writeLogAndStatus(ExtractLog extractLog) {
		String dateStart = extractLog.getStartDt();
		String dateFinish = extractLog.getFinishDt();
		String dateQuery = extractLog.getQueryDt();
		String extractType = extractLog.getExtractType();
		int totalLabTests = extractLog.getResultCnt();
		int oracleJobNumber = extractLog.getOracleJobNumber();
		boolean writeHeader = false;

		extractLog.writeNewExtractLog();

		try { // Write status to Extraction Log
			File infile = new File("BTRIS_Extraction_Log.txt");

			if (!infile.exists()) {
				System.out
						.println("Extraction Log File missing.  Creating new one.");
				writeHeader = true;
			}

			FileWriter outFile = new FileWriter("BTRIS_Extraction_Log.txt",
					true);

			// false means we will be writing to the file
			BufferedWriter outBuf = new BufferedWriter(outFile);
			if (writeHeader) {
				outBuf.write("Start Time\t\tStop Time\t\tQuery Date\t\tType\t\tC3D Job#\tResults");
				outBuf.newLine();
			}

			outBuf.write(dateStart + "\t");
			outBuf.write(dateFinish + "\t");
			outBuf.write(dateQuery + "\t");
			outBuf.write(extractType + "\t");
			outBuf.write(oracleJobNumber + "\t");
			outBuf.write(totalLabTests + "\t\t");
			outBuf.newLine();

			outBuf.close();
		} catch (IOException ioexception) {
			ioexception.printStackTrace();
		}

	}

	private static int getWriteLabResults(String qType, String outType,
			Calendar inQueryDate) {

		Connection conBTRIS = null;
		PreparedStatement stmtBTRIS = null;
		ResultSet rsBTRIS = null;
		String eventGuid = null;
		String observGuid = null;
		String observNameConcept = null;
		String observName = null;
		String observValueText = null;
		String unitOfMeasure = null;
		String range = null;
		String primaryDateTime = null;
		String subjectGuid = null;
		String subjectMRN = null;
		String labTestCode = null;
		String dateCreated = null;
		String dateModified = null;
		String labTestReportedDate = null;
		String queryDate = null;
		String extractDate = null;
		String holdQuery = null;
		String observNote = null;

		int totalResultsRecords = 0;
		SimpleDateFormat dateFormatSQLServer = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss.SSS");
		String queryDateStr = dateFormatSQLServer.format(inQueryDate.getTime());
		Properties extractProps = null;

		String incrementalResults = "select distinct Observation_Measurable.Event_GUID as Event_GUID, "
				+ " Observation_Measurable.Observation_GUID as Observation_GUID, "
				+ " Observation_Name_CONCEPT, Observation_Name, Substring(Observation_Value_Text,1,200), "
				+ " Unit_of_Measure, Range, Primary_Date_Time, Observation_Measurable.Subject_GUID, "
				+ " Subject.MRN, a.Value as LAB_TEST_CODE, Observation_Measurable.DX_Date_Created, "
				+ " Observation_Measurable.DX_Date_Modified, Primary_Date_Time as LabTest_Reported_Date, "
				+ " ? as QueryDate, GETDATE() as ExtractDateTime, "
				+ " Substring(Observation_Note,1,200) as NOTE "
				+ "  from Observation_Measurable, "
				+ "       Subject, "
				+ "       Observation_Measurable_EAV a "
				+ " WHERE (Observation_Measurable.DX_Date_Created >= ? "
				+ "      OR Observation_Measurable.DX_Date_Modified >= ? )"
				+ " and Observation_Measurable.Subject_GUID = Subject.Subject_GUID "
				+ " and Observation_Measurable.Observation_GUID = a.Observation_GUID "
				+ " and a.Attribute_CONCEPT = 'C139475' "
				+ " and Observation_Measurable.Observation_Name_CONCEPT in "
				+ "     (select Descendant_Concept "
				+ "        from RED_Ancestor_Descendant_Identity "
				+ "       where Ancestor_Concept = 'C90151') "
				+ " and Observation_Measurable.Appl_Source_CD = 'C113093' ";

		String cumulativeResults = "select distinct Observation_Measurable.Event_GUID as Event_GUID, "
				+ " Observation_Measurable.Observation_GUID as Observation_GUID, "
				+ " Observation_Name_CONCEPT, Observation_Name, Substring(Observation_Value_Text,1,200), "
				+ " Unit_of_Measure, Range, Primary_Date_Time, Observation_Measurable.Subject_GUID, "
				+ " Subject.MRN, a.Value as LAB_TEST_CODE, Observation_Measurable.DX_Date_Created, "
				+ " Observation_Measurable.DX_Date_Modified, Primary_Date_Time as LabTest_Reported_Date, "
				+ " ? as QueryDate, GETDATE() as ExtractDateTime, "
				+ " Substring(Observation_Note,1,200) as NOTE "
				+ "  from Observation_Measurable, "
				+ "       Subject, "
				+ "       Observation_Measurable_EAV a "
				+ " WHERE Observation_Measurable.Subject_GUID = Subject.Subject_GUID "
				+ " and Observation_Measurable.Observation_GUID = a.Observation_GUID "
				+ " and a.Attribute_CONCEPT = 'C139475' "
				+ " and Observation_Measurable.Observation_Name_CONCEPT in "
				+ "     (select Descendant_Concept "
				+ "        from RED_Ancestor_Descendant_Identity "
				+ "        where Ancestor_Concept = 'C90151') " 
				+ " and Observation_Measurable.Appl_Source_CD = 'C113093'";



		if (qType.equals("CUMULATIVE"))
			holdQuery = cumulativeResults;
		else
			holdQuery = incrementalResults;
//		System.out.print("query="+ holdQuery);
		
		if (outType.equals("FILE") || outType.equals("BOTH")) {
			try { // Write the Header Line for the FILE
				FileWriter outFile = new FileWriter(
						"BTRIS_Lab_Test_Results.txt", false);
				BufferedWriter outBuf = new BufferedWriter(outFile);

				outBuf.write("BTRIS Event ID\tBTRIS Observation ID\tBTRIS Lab Test ID\tLab Test Name\t");
				outBuf.write("LabTest Result\tLabTest_UnitofMeasure\tLabTest_Range\tLabTest_DateTime\t");
				outBuf.write("BTRIS_SubjectID\tPatientID\tLabTest_TestCode\tBTRIS_CreateDate\t");
				outBuf.write("BTRIS_ModifyDate\tLabTest_ReportDate\tQueryDate\tNote\tExtractDate");
				outBuf.newLine();
				outBuf.close();
			} catch (IOException ioexception) {
				ioexception.printStackTrace();
			}
		}

		FileWriter outFile = null;
		BufferedWriter outBuf = null;
		Connection conC3D = null;
		PreparedStatement prepC3D = null;

		String connBTRISUrl = urlBTRIS;
		String connC3DUrl = urlC3D;

		try { // Query Data
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			
			conBTRIS = DriverManager.getConnection(urlBTRIS + ";user="
					+ userBTRIS + ";password=" + passwordBTRIS);

			if (outType.equals("FILE") || outType.equals("BOTH")) {
				outFile = new FileWriter("BTRIS_Lab_Test_Results.txt", true);
				outBuf = new BufferedWriter(outFile);
			}
			if (outType.equals("TABLE") || outType.equals("BOTH")) {
				String SQLC3D_2 = "TRUNCATE TABLE BTRIS_LAB_TEST_RESULTS";
				String SQLC3D_3 = "INSERT INTO BTRIS_LAB_TEST_RESULTS ("
						+ "BTRIS_EVENT_ID, BTRIS_OBSERVATION_ID, BTRIS_LAB_TEST_ID, "
						+ "LAB_TEST_NAME, LAB_RESULT, LAB_UNIT, "
						+ "LAB_RANGE, LAB_DATE_TXT, BTRIS_SUBJECT_ID, "
						+ "PATIENT_ID, LABTEST_CODE, BTRIS_CREATE_DATE, "
						+ "BTRIS_MODIFY_DATE, LAB_REPORT_DATE, EXTRACT_QUERY_DATE, "
						+ "EXTRACT_EXTRACT_DATE, LAB_NOTE) "
						+ "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
				try {
					Class.forName("oracle.jdbc.driver.OracleDriver");
					conC3D = DriverManager.getConnection(urlC3D,
							userC3D, passwordC3D);
					System.out.println("Connected to C3D.");
					Statement stmtC3D = conC3D.createStatement();
					int rsInt = stmtC3D.executeUpdate(SQLC3D_2);
					if (stmtC3D != null)
						try {
							stmtC3D.close();
						} catch (Exception e) {
							e.printStackTrace();
							System.exit(1);
						}
					System.out.println("Table BTRIS_LAB_TEST_RESULTS Truncated.");
					prepC3D = conC3D.prepareStatement(SQLC3D_3);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			stmtBTRIS = conBTRIS.prepareStatement(holdQuery);
			if (qType.equals("CUMULATIVE")) {
				stmtBTRIS.setString(1, queryDateStr);
			} else {
				stmtBTRIS.setString(1, queryDateStr);
				stmtBTRIS.setString(2, queryDateStr);
				stmtBTRIS.setString(3, queryDateStr);
			}
			System.out.println("About to get data from BTRIS");
			rsBTRIS = stmtBTRIS.executeQuery();
			try {
				while (rsBTRIS.next()) {
					eventGuid = rsBTRIS.getString(1); // BTRIS Event ID
					observGuid = rsBTRIS.getString(2); // Observation ID
					observNameConcept = rsBTRIS.getString(3); // Lab Test BTRIS
																// ID
					observName = rsBTRIS.getString(4); // Lab Test Name
					observValueText = rsBTRIS.getString(5); // LabResult
					unitOfMeasure = rsBTRIS.getString(6); // Unit of Measure
					range = rsBTRIS.getString(7); // Lab Range
					primaryDateTime = rsBTRIS.getString(8); // Primary Lab Date
					subjectGuid = rsBTRIS.getString(9); // BTRIS Subject ID
					subjectMRN = rsBTRIS.getString(10); // Subject MRN
					labTestCode = rsBTRIS.getString(11); // Lab Test COde
					dateCreated = rsBTRIS.getString(12); // Create Date
					dateModified = rsBTRIS.getString(13); // Modify Date
					labTestReportedDate = rsBTRIS.getString(14); // Lab Test
																	// Date
					queryDate = rsBTRIS.getString(15); // Query Date
					extractDate = rsBTRIS.getString(16); // ExtractDate
					observNote = rsBTRIS.getString(17);  // Note

					totalResultsRecords = totalResultsRecords + 1;
					if (outType.equals("FILE") || outType.equals("BOTH")) {
						outBuf.write(eventGuid + "\t");
						outBuf.write(observGuid + "\t");
						outBuf.write(observNameConcept + "\t");
						outBuf.write(observName + "\t");
						outBuf.write(observValueText + "\t");
						outBuf.write(unitOfMeasure + "\t");
						outBuf.write(range + "\t");
						outBuf.write(primaryDateTime + "\t");
						outBuf.write(subjectGuid + "\t");
						outBuf.write(subjectMRN + "\t");
						outBuf.write(labTestCode + "\t");
						outBuf.write(dateCreated + "\t");
						outBuf.write(dateModified + "\t");
						outBuf.write(labTestReportedDate + "\t");
						outBuf.write(queryDate + "\t");
						outBuf.write(observNote + "\t");
						outBuf.write(extractDate);
						outBuf.newLine();
					}
					if (outType.equals("TABLE") || outType.equals("BOTH")) {
						prepC3D.setString(1, eventGuid);
						prepC3D.setString(2, observGuid);
						prepC3D.setString(3, observNameConcept);
						if (observName.length() > 1000) {
							prepC3D.setString(4, observName.substring(0, 1000));
						} else {
							prepC3D.setString(4, observName);
						}
						prepC3D.setString(5, observValueText);
						prepC3D.setString(6, unitOfMeasure);
						prepC3D.setString(7, range);
						prepC3D.setString(8, primaryDateTime);
						prepC3D.setString(9, subjectGuid);
						prepC3D.setString(10, subjectMRN);
						prepC3D.setString(11, labTestCode);
						prepC3D.setString(12, dateCreated);
						prepC3D.setString(13, dateModified);
						prepC3D.setString(14, labTestReportedDate);
						prepC3D.setString(15, queryDate);
						prepC3D.setString(16, extractDate);
						prepC3D.setString(17, observNote);
						prepC3D.executeUpdate();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(eventGuid + "'");
				System.out.println(observGuid + "'");
				System.out.println(observNameConcept + "'");
				System.out.println(observName + "'");
				System.out.println(observValueText + "'");
				System.out.println(unitOfMeasure + "'");
				System.out.println(range + "'");
				System.out.println(primaryDateTime + "'");
				System.out.println(subjectGuid + "'");
				System.out.println(subjectMRN + "'");
				System.out.println(labTestCode + "'");
				System.out.println(dateCreated + "'");
				System.out.println(dateModified + "'");
				System.out.println(labTestReportedDate + "'");
				System.out.println(queryDate + "'");
				System.out.println(extractDate +"'");
				System.out.println(observNote + "'");
			}

			if (outType.equals("FILE") || outType.equals("BOTH")) {
				outBuf.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conC3D != null)
				try {
					conC3D.close();
				} catch (Exception e) {
				}
		}
		return totalResultsRecords;
	}

	private static void executeExtraction() {

		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		Calendar todaysDate = Calendar.getInstance();

		String dateStart = dateFormat.format(todaysDate.getTime());

		// query date is today "plus" the Query Date Offset Constant.
		Calendar newQueryDate = Calendar.getInstance();
		int offSetDaysInt = Integer.parseInt(offsetDaysStr);
			System.out.println("");
		newQueryDate.add(Calendar.DATE, offSetDaysInt);
		newQueryDate.set(Calendar.HOUR_OF_DAY, 0);
		newQueryDate.set(Calendar.MINUTE, 0);
		newQueryDate.set(Calendar.SECOND, 0);
		newQueryDate.set(Calendar.MILLISECOND, 0);

		System.out.println("Query BTRIS Data from : " + newQueryDate.getTime());

		System.out.println();
		ExtractLog extractLog = new ExtractLog();
		extractLog.setStartDt(dateStart);
		extractLog.setQueryDt(dateFormat.format(newQueryDate.getTime()));
		extractLog.setExtractType(extractType);
		extractLog.setOutputType(outputType);

		System.out.println("Writing " + extractType + " Test Results to "
				+ outputType);
		extractLog.setResultCnt(getWriteLabResults(extractType, outputType,
				newQueryDate));

		if (submitPushC3DJob.equalsIgnoreCase("TRUE")) {
			extractLog = submitC3DPush(extractLog);
		}

		Date date = new Date();
		extractLog.setFinishDt(dateFormat.format(date));
		writeLogAndStatus(extractLog);
	}

	private static void showCommandLineHelp() {
		System.out.println();
		System.out.println("Usage:");
		System.out.println("-? -help\t\tCommand-line help information");
		System.out.println("-BU:username\t\tBTRIS User Id.");
		System.out.println("-BP:password\t\tBTRIS User Id password.");
		System.out.println("-CU:username\t\tC3D User Id.");
		System.out.println("-CP:password\t\tC3D User Id password.");
		System.out.println();
	}

	private static void showUnknownCommand(String s) {
		System.out
				.println("'"
						+ s
						+ "' is an unknown command.  Use -? for list of valid command options.");
		System.out.println();
	}

	private static String getPromptedString(String prompt, Boolean hidden) {
		String s = null;
		if (hidden) {
			Console cons = System.console();
			char[] hidString = null;
			while ((s == null) || "".equals(s)) {
				hidString = cons.readPassword(prompt);
				s = new String(hidString);
			}
		} else {
			BufferedReader br = null;
			while ((s == null) || "".equals(s)) {
				System.out.print(prompt + " ");

				br = new BufferedReader(new InputStreamReader(System.in));
				try {
					s = br.readLine();
				} catch (IOException e) {
					System.out.println(e);
				}
			}
		}
		return s;
	}

	public static void main(String[] args) {

		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

		Calendar date = null;
		Properties extractProps = null;
		Calendar lastStartDate = null;
		Calendar todayExecuteDate = null;
		Calendar holdTodayExecuteDate = null;
		Calendar holdLastStartDate = null;
		Long nowMillis = null;
		Long extractMillis = null;
		Long diffMillis = null;
		Long sleepMillis = null;

		System.out.println("");
		System.out.println("BTRIS Data Extractor for C3D, v2.0");
		try {
			extractProps = PropertiesUtil.getPropertiesFromFile();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		// get the BTRIS and C3D database connection values from the properties file
		urlBTRIS = extractProps.getProperty(Constants.NameControlBTRISURL);
		userBTRIS = extractProps.getProperty(Constants.NameControlBTRISUser);
		passwordBTRIS = extractProps.getProperty(Constants.NameControlBTRISPass);
		urlC3D = extractProps.getProperty(Constants.NameControlC3DURL);
		userC3D = extractProps.getProperty(Constants.NameControlC3DUser);
		passwordC3D = extractProps.getProperty(Constants.NameControlC3DPass);
		extractType = extractProps.getProperty(Constants.NameControlCumulativeDay);
		offsetDaysStr = extractProps.getProperty(Constants.NameControlQOffsetDays);
		outputType = extractProps.getProperty(Constants.NameControlOutputType);
		submitPushC3DJob = extractProps.getProperty(Constants.NameControlSubmitPushC3DJob);
		// check for command line arguements
		for (String s : args) {
			System.out.println(s);
			if (s.equals("-?") || s.equalsIgnoreCase("-help")) { // Command Line Help
				showCommandLineHelp();
				if (args.length == 1) {
					System.exit(0);
				}
			} else if (s.startsWith("-BU:")) { // BTRIS Userid, OVERRIDES value
												// from property file
				userBTRIS = s.substring(4);
			} else if (s.startsWith("-BP:")) { // BTRIS Password, OVERRIDES
												// value from property file
				passwordBTRIS = s.substring(4);
			} else if (s.startsWith("-CU:")) { // C3D Userid, OVERRIDES value
												// from property file
				userC3D = s.substring(4);
			} else if (s.startsWith("-CP:")) { // C3D Pasword, OVERRIDES value
												// from property file
				passwordC3D = s.substring(4);
			} else { // All other arguements results in Unknown Command Message
				showUnknownCommand(s);
				if (args.length == 1) {
					System.exit(0);
				}
			}
		}

		// If BTRIS Username not found, PROMPT for it
		if ((userBTRIS == null) || "".equals(userBTRIS))
			userBTRIS = getPromptedString("BTRIS username required: ", false);

		// If BTRIS Password not found, PROMPT for it
		if ((passwordBTRIS == null) || "".equals(passwordBTRIS))
			passwordBTRIS = getPromptedString("BTRIS password required: ", true);

		// If C3D Username not found, PROMPT for it
		if ((userC3D == null) || "".equals(userC3D))
			userC3D = getPromptedString("C3D username required: ", false);

		// If C3D Password not found, PROMPT for it
		if ((passwordC3D == null) || "".equals(passwordC3D))
			passwordC3D = getPromptedString("C3D password required: ", true);


		executeExtraction();

	}

}