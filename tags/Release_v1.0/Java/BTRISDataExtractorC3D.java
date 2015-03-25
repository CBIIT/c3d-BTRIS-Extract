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
    private static FileHandler fh;
    private static FileWriter protocols, logRecord;
    private static int numberOfStudies, numberOfPatients, numberOfResults = 0;
    private static String urlBTRIS, userBTRIS, passwordBTRIS = null;
    private static String urlC3D, userC3D, passwordC3D = null;

    private static void printProgBar(int percent){
        // prodedure gives a status bar of sorts
        StringBuilder bar = new StringBuilder("\r[");
        final int width = 50; //progress bar width
        double dblPercent = (double) percent;    //percentage of process
        double sLength = (double) (dblPercent/100)*width;
        int thisWidth = (int) sLength;

        for(int i = 0; i < thisWidth; i++){
                bar.append("=");
        }

        if (thisWidth < width) {
                bar.append(">");
        }

        for(int i = thisWidth+1; i < width; i++){
                bar.append(" ");
        }

        bar.append("]   " + percent + "%     ");
        System.out.print("\r" + bar.toString());
    }

    private static List<StudySubject> findBTRISStudySubjects(Study inStudy) {
        // Finds all BTRIS patients of specified BTRIS study, adds them to a list and
        // returns the list
    // DEPRICATED
        String connBTRISUrl = urlBTRIS;
        String connBTRISUser= userBTRIS;
        String connBTRISPass= passwordBTRIS;

        List<StudySubject> studySubjects = new ArrayList<StudySubject>();
        StudySubject studySubject = null;
        String findProtocol =
        "select Distinct Protocol_number, MRN from Protocol_Subject, Subject " +
                " where Protocol_Subject.Subject_GUID = Subject.Subject_GUID " +
                "   and Protocol_number = ? "; //+
                //" ORDER BY Protocol_number, MRN ";

        Connection conBTRIS = null;
        PreparedStatement stmtBTRIS = null;
        ResultSet rsBTRIS = null;

        String queryStudy = inStudy.getC3DStudyIdentifier().replace("_","-");

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            conBTRIS = DriverManager.getConnection(connBTRISUrl+";user="
                                    +connBTRISUser+";password="+connBTRISPass);
            stmtBTRIS = conBTRIS.prepareStatement(findProtocol);

            stmtBTRIS.setString(1, queryStudy);
            System.out.println("Starting Query 'StudySubject' - " + Calendar.getInstance().getTime());
            rsBTRIS = stmtBTRIS.executeQuery();
            System.out.println("Finished Query 'StudySubject' - " + Calendar.getInstance().getTime());

            // Iterate through the data in the result set and display it.
            System.out.println("Starting While 'StudySubject' - " + Calendar.getInstance().getTime());
            while (rsBTRIS.next()) {
                studySubject = new StudySubject();
                studySubject.setMRN(rsBTRIS.getString(2));
                studySubjects.add(studySubject);
            }
            System.out.println("Finished While 'StudySubject' - " + Calendar.getInstance().getTime());

        }
        // Handle any errors that may have occurred.
        catch (Exception e) {
            e.printStackTrace();
        }

        finally {
            if (rsBTRIS != null) try { rsBTRIS.close(); } catch(Exception e) {}
            if (stmtBTRIS != null) try { stmtBTRIS.close(); } catch(Exception e) {}
            if (conBTRIS != null) try { conBTRIS.close(); } catch(Exception e) {}
        }
        return studySubjects;
    }

    private static Map<String, Object> findUpdateBTRISStudies(Map<String, Object> inStudies) {
        String connBTRISUrl = urlBTRIS;
        String connBTRISUser= userBTRIS;
        String connBTRISPass= passwordBTRIS;
        String findProtocol = "Select distinct Protocol_Number from Protocol_Subject where Protocol_Number = ?";
        String findMRNs =
                "select Distinct Protocol_number, MRN from Protocol_Subject, Subject " +
                " where Protocol_Subject.Subject_GUID = Subject.Subject_GUID " +
                "   and Protocol_number = ? ";

        String queryStudy = null;
        Study study = null;
    StudySubject studySubject = null;
        List<StudySubject> studySubjects = new ArrayList<StudySubject>();

        //System.out.println("Connecting to BTRIS.");
        // Declare the JDBC objects.
        Connection conBTRIS = null;
        PreparedStatement stmtBTRIS1 = null;
        ResultSet rsBTRIS1 = null;
        PreparedStatement stmtBTRIS2 = null;
        ResultSet rsBTRIS2 = null;
        double progMaxStudies = inStudies.size();
        double progCntStudies = 0;

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            conBTRIS = DriverManager.getConnection(connBTRISUrl+";user="
                                  +connBTRISUser+";password="+connBTRISPass);
            stmtBTRIS1 = conBTRIS.prepareStatement(findProtocol);

            for (Iterator iter = inStudies.keySet().iterator(); iter.hasNext();) {
                String key = (String) iter.next();
                study = (Study) inStudies.get(key);

                queryStudy = study.getC3DStudyIdentifier().replace("_","-");

                stmtBTRIS1.setString(1, queryStudy);

                rsBTRIS1 = stmtBTRIS1.executeQuery();
                // Iterate through the data in the result set and display it.

                study.setStudyStatus("Not Found");
                stmtBTRIS2 = conBTRIS.prepareStatement(findMRNs);
                while (rsBTRIS1.next()) {
                    study.setBTRISStudyIdentifier(rsBTRIS1.getString(1));
                    study.setStudyStatus("Found");
                }

                if (study.getBTRISStudyIdentifier() != null){
                    studySubjects = study.getStudySubjects();

                    stmtBTRIS2.setString(1, queryStudy);

                    rsBTRIS2 = stmtBTRIS2.executeQuery();

                    while (rsBTRIS2.next()) {
                        studySubject = new StudySubject();
                        studySubject.setMRN(rsBTRIS2.getString(2));
                        studySubjects.add(studySubject);
                    }
                }
                progCntStudies = progCntStudies + 1;
                printProgBar((int) ((progCntStudies/progMaxStudies)*100));
            }

        }
        // Handle any errors that may have occurred.
        catch (Exception e) {
            e.printStackTrace();
        }

        finally {
            if (rsBTRIS1   != null) try { rsBTRIS1.close(); }   catch(Exception e) {}
            if (stmtBTRIS1 != null) try { stmtBTRIS1.close(); } catch(Exception e) {}
            if (rsBTRIS2   != null) try { rsBTRIS2.close(); }   catch(Exception e) {}
            if (stmtBTRIS2 != null) try { stmtBTRIS2.close(); } catch(Exception e) {}
            if (conBTRIS   != null) try { conBTRIS.close(); }   catch(Exception e) {}
        }
        return inStudies;
    }



    private static Study findBTRISStudy(Study inStudy) {
        String connBTRISUrl = urlBTRIS;
        String connBTRISUser= userBTRIS;
        String connBTRISPass= passwordBTRIS;
        String findProtocol = "Select distinct Protocol_Number from Protocol_Subject where Protocol_Number = ?";

        //System.out.println("Connecting to BTRIS.");
        // Declare the JDBC objects.
        Connection conBTRIS = null;
        PreparedStatement stmtBTRIS = null;
        ResultSet rsBTRIS = null;

        String queryStudy = inStudy.getC3DStudyIdentifier().replace("_","-");

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            conBTRIS = DriverManager.getConnection(connBTRISUrl+";user="
                                  +connBTRISUser+";password="+connBTRISPass);
            stmtBTRIS = conBTRIS.prepareStatement(findProtocol);

            stmtBTRIS.setString(1, queryStudy);

            rsBTRIS = stmtBTRIS.executeQuery();

            // Iterate through the data in the result set and display it.
            inStudy.setStudyStatus("Not Found");
            while (rsBTRIS.next()) {
                inStudy.setBTRISStudyIdentifier(rsBTRIS.getString(1));
                inStudy.setStudyStatus("Found");
            }
        }
        // Handle any errors that may have occurred.
        catch (Exception e) {
            e.printStackTrace();
        }

        finally {
            if (rsBTRIS != null) try { rsBTRIS.close(); } catch(Exception e) {}
            if (stmtBTRIS != null) try { stmtBTRIS.close(); } catch(Exception e) {}
            if (conBTRIS != null) try { conBTRIS.close(); } catch(Exception e) {}
        }
        return inStudy;
    }

    private static ExtractLog submitC3DPush(ExtractLog extractLog) {
        String connectionC3DUrl   = urlC3D;
        String connectionC3DUser  = userC3D;
        String connectionC3DPass  = passwordC3D;
        Connection conC3D = null;
        CallableStatement stmtC3D = null;
        ResultSet rsC3D = null;
        SimpleDateFormat dateFormatSQLServer = new SimpleDateFormat("yyMMdd_HH:mm:ss");
        String dateStr = dateFormatSQLServer.format(Calendar.getInstance().getTime());
    int oracleJobNumber = 0;
        String oracleLogName = "BTRIS_PUSHC3D" + dateStr;

        try {
            // Establish the connection to C3D
            Class.forName("oracle.jdbc.driver.OracleDriver");
            //System.out.println("Get Connection to C3D.");
            conC3D = DriverManager.getConnection(connectionC3DUrl,connectionC3DUser,connectionC3DPass);
            //System.out.println("Connected to C3D.");

            String SQLC3D = "{call btris_data_transfer.Submit_PULL_AND_PUSH_LABS(?, ?)}";

            stmtC3D = conC3D.prepareCall(SQLC3D);
            stmtC3D.setInt(1,0);
            stmtC3D.setString(2,oracleLogName);
            stmtC3D.registerOutParameter(1,java.sql.Types.INTEGER);
            stmtC3D.registerOutParameter(2,java.sql.Types.VARCHAR);
            stmtC3D.executeUpdate();
            oracleJobNumber = stmtC3D.getInt(1);
            oracleLogName = stmtC3D.getString(2);

            //System.out.println("Showing the results:");
            //System.out.println("Oracle Job Number=" + oracleJobNumber);
            //System.out.println("Oracle Log Name='" + oracleLogName + "'");

            System.out.println("Submitted BTRIS PushC3D Job, " + oracleJobNumber + ", LogName: '" + oracleLogName +"'.");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        finally {
            if (rsC3D != null) try { rsC3D.close(); } catch(Exception e) {}
            if (stmtC3D != null) try { stmtC3D.close(); } catch(Exception e) {}
            if (conC3D != null) try { conC3D.close(); } catch(Exception e) {}
        }
        extractLog.setOracleJobNumber(oracleJobNumber);
        extractLog.setOracleJobLogName(oracleLogName);

        return extractLog;
    }

    private static Map<String, Object> getC3DStudies() {
        String connectionC3DUrl   = urlC3D;
        String connectionC3DUser  = userC3D;
        String connectionC3DPass  = passwordC3D;
        Connection conC3D = null;
        Statement stmtC3D = null;
        ResultSet rsC3D = null;
        Study study = null;

        Map<String, Object> studies = new HashMap<String, Object>();

        try {
            // Establish the connection to C3D
            Class.forName("oracle.jdbc.driver.OracleDriver");
            System.out.println("Get Connection to C3D.");
            conC3D = DriverManager.getConnection(connectionC3DUrl,connectionC3DUser,connectionC3DPass);
            System.out.println("Connected to C3D.");

            //Production Query
            //String SQLC3D = "Select Distinct STUDY OC_STUDY from NCI_LAB_VALID_PATIENTS";
            String SQLC3D = "Select Distinct OC_STUDY from NCI_LAB_LOAD_CTL where LABORATORY = 'BTRIS'";
            //Quick Simple Query for testing
            //String SQLC3D = "Select Distinct STUDY OC_STUDY from NCI_LAB_VALID_PATIENTS where OC_STUDY = '11_C_0231'";

            stmtC3D = conC3D.createStatement();
            rsC3D = stmtC3D.executeQuery(SQLC3D);

            System.out.println("Show the results:");
            // Iterate through the data in the result set and display it.
            while (rsC3D.next()) {
                String studyId = rsC3D.getString("OC_STUDY");

                StringBuffer key = new StringBuffer(studyId);

                study = new Study();
                study.setC3DStudyIdentifier(studyId);
                studies.put(key.toString(), study);
            }
            System.out.println("Query Completed. " + studies.size() + " C3D Studies found." );
        }
        // Handle any errors that may have occurred.
        catch (Exception e) {
            e.printStackTrace();
        }

        finally {
            if (rsC3D != null) try { rsC3D.close(); } catch(Exception e) {}
            if (stmtC3D != null) try { stmtC3D.close(); } catch(Exception e) {}
            if (conC3D != null) try { conC3D.close(); } catch(Exception e) {}
        }

        return studies;
    }

    private static int writeProtocols(Map<String, Object> studies, String outType) {
        Study study = null;
        List<StudySubject> studySubjects = null;
        int numberOfStudies = 0;

        Properties extractProps = null;

        try {
            extractProps = PropertiesUtil.getPropertiesFromFile();
        } catch (Exception e) {
            e.printStackTrace();
        }

        String outputType = extractProps.getProperty(Constants.NameControlOutputType);

        // false means we will be writing to the file
        if (outType.equals("FILE") || outType.equals("BOTH")) {
            try {
                FileWriter outFile = new FileWriter( "BTRIS_Protocol_Status.txt", false );
                BufferedWriter outBuf = new BufferedWriter(outFile);
                outBuf.write("C3D Study\tBTRIS Study\tStatus");
                outBuf.newLine();

                for (Iterator iter = studies.keySet().iterator(); iter.hasNext();) {
                    String key = (String) iter.next();
                    study = (Study) studies.get(key);

                    outBuf.write(study.getC3DStudyIdentifier()   + "\t");
                    outBuf.write(study.getBTRISStudyIdentifier() + "\t");
                    outBuf.write(study.getStudyStatus());
                    outBuf.newLine();
                    numberOfStudies = numberOfStudies + 1;
                 }
                 outBuf.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (outType.equals("TABLE") || outType.equals("BOTH")) {
            String connectionC3DUrl   = extractProps.getProperty(Constants.NameControlC3DURL);
            String connectionC3DUser  = userC3D;
            String connectionC3DPass  = passwordC3D;
            String SQLC3D_1 = "TRUNCATE TABLE BTRIS_PROTOCOL_STATUS";
            String SQLC3D_2 = "INSERT INTO BTRIS_PROTOCOL_STATUS (C3D_STUDY, BTRIS_STUDY, STATUS) "
                              + "VALUES ( ?, ?, ?)";
            Connection conC3D = null;
            Statement stmtC3D = null;
            int rsInt = 0;
            try {
                // Establish the connection to C3D
                Class.forName("oracle.jdbc.driver.OracleDriver");
                System.out.println("Get Connection to C3D.");
                conC3D = DriverManager.getConnection(connectionC3DUrl,connectionC3DUser,connectionC3DPass);
                System.out.println("Connected to C3D.");
                stmtC3D = conC3D.createStatement();
                rsInt = stmtC3D.executeUpdate(SQLC3D_1);
                System.out.println("Table Truncated.");

                PreparedStatement prepC3D = conC3D.prepareStatement(SQLC3D_2);

                numberOfStudies = 0;
                for (Iterator iter = studies.keySet().iterator(); iter.hasNext();) {
                    String key = (String) iter.next();
                    study = (Study) studies.get(key);

                    prepC3D.setString(1, study.getC3DStudyIdentifier());
                    prepC3D.setString(2, study.getBTRISStudyIdentifier());
                    prepC3D.setString(3, study.getStudyStatus());
                    prepC3D.executeUpdate();
                    numberOfStudies++;
               }

            } catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                //try { rsC3D.close(); } catch(Exception e) {}
                if (stmtC3D != null) try { stmtC3D.close(); } catch(Exception e) {}
                if (stmtC3D != null) try { stmtC3D.close(); } catch(Exception e) {}
                if (conC3D != null) try { conC3D.close(); } catch(Exception e) {}
            }
        }

        return numberOfStudies;
    }

    private static int writeProtPatients(Map<String, Object> studies, String outType) {
        Study study = null;
        List<StudySubject> studySubjects = null;
        StudySubject studySubject = null;
        int numberOfPatients = 0;

        if (outType.equals("FILE") || outType.equals("BOTH")) {
            try {
                FileWriter outFile = new FileWriter( "BTRIS_Protocol_Patient.txt", false );
                // false means we will be writing to the file
                BufferedWriter outBuf = new BufferedWriter(outFile);
                outBuf.write("BTRIS Study\tMRN");
                outBuf.newLine();
                for (Iterator iter = studies.keySet().iterator(); iter.hasNext();) {
                    String key = (String) iter.next();
                    study = (Study) studies.get(key);
                    if (study.getBTRISStudyIdentifier() != null) {
                       studySubjects = study.getStudySubjects();
                       numberOfPatients = numberOfPatients + studySubjects.size();
                       for (Iterator<StudySubject> iterSS = studySubjects.iterator(); iterSS.hasNext();) {
                           //int key2 = iterSS.next();
                           //studySubject = (StudySubject) studySubjects.get(key2);
                           studySubject = iterSS.next();
                           outBuf.write(study.getBTRISStudyIdentifier() + "\t");
                           outBuf.write(studySubject.getMRN());
                           outBuf.newLine();
                       }
                    }
                }
                outBuf.close();
            } catch(IOException ioexception) {
               ioexception.printStackTrace();
            }

        }

        if (outType.equals("TABLE") || outType.equals("BOTH")) {
            Properties extractProps = null;

            try {
                extractProps = PropertiesUtil.getPropertiesFromFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
            String connectionC3DUrl   = extractProps.getProperty(Constants.NameControlC3DURL);
            String connectionC3DUser  = userC3D;
            String connectionC3DPass  = passwordC3D;
            String SQLC3D_1 = "TRUNCATE TABLE BTRIS_PROTOCOL_PATIENT";
            String SQLC3D_2 = "INSERT INTO BTRIS_PROTOCOL_PATIENT (BTRIS_STUDY, MRN) VALUES ( ?, ?)";
            Connection conC3D = null;
            Statement stmtC3D = null;
            int rsInt = 0;
            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                System.out.println("Get Connection to C3D.");
                conC3D = DriverManager.getConnection(connectionC3DUrl,connectionC3DUser,connectionC3DPass);
                System.out.println("Connected to C3D.");
                stmtC3D = conC3D.createStatement();
                rsInt = stmtC3D.executeUpdate(SQLC3D_1);
                System.out.println("Table Truncated.");

                PreparedStatement prepC3D = conC3D.prepareStatement(SQLC3D_2);

                numberOfPatients = 0;
                for (Iterator iter = studies.keySet().iterator(); iter.hasNext();) {
                    String key = (String) iter.next();
                    study = (Study) studies.get(key);
                    if (study.getBTRISStudyIdentifier() != null) {
                        studySubjects = study.getStudySubjects();
                        numberOfPatients = numberOfPatients + studySubjects.size();
                        for (Iterator<StudySubject> iterSS = studySubjects.iterator(); iterSS.hasNext();) {
                            studySubject = iterSS.next();
                            prepC3D.setString(1, study.getBTRISStudyIdentifier());
                            prepC3D.setString(2, studySubject.getMRN());
                            prepC3D.executeUpdate();
                       }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                //try { rsC3D.close(); } catch(Exception e) {}
                if (stmtC3D != null) try { stmtC3D.close(); } catch(Exception e) {}
                if (conC3D != null) try { conC3D.close(); } catch(Exception e) {}
            }
        }
        return numberOfPatients;
    }

    private static void writeLogAndStatus(ExtractLog extractLog) {
       String dateStart  = extractLog.getStartDt();
       String dateFinish = extractLog.getFinishDt();
       String dateQuery  = extractLog.getQueryDt();
       String extractType= extractLog.getExtractType();
       int totalStudies  = extractLog.getStudyCnt();
       int totalStudyPatients = extractLog.getStudyPatientCnt();
       int totalUniquePatients = extractLog.getUniquePatientCnt();
       int totalLabTests = extractLog.getResultCnt();
       int oracleJobNumber = extractLog.getOracleJobNumber();
       boolean writeHeader = false;

       extractLog.writeNewExtractLog();

       try { //Write status to Extraction Log
          File infile = new File("BTRIS_Extraction_Log.txt");

      if (!infile.exists()) {
              System.out.println("Extraction Log File missing.  Creating new one.");
              writeHeader = true;
          }

          FileWriter outFile = new FileWriter( "BTRIS_Extraction_Log.txt", true );

          // false means we will be writing to the file
          BufferedWriter outBuf = new BufferedWriter(outFile);
          if (writeHeader) {
          outBuf.write("Start Time\t\tStop Time\t\tQuery Date\t\tType\t\tC3D Job#\tStudies\t\tStudyPatients\tUniquePatients\tResults");
              outBuf.newLine();
          }

          outBuf.write(dateStart+"\t");
          outBuf.write(dateFinish+"\t");
          outBuf.write(dateQuery+"\t");
          outBuf.write(extractType+"\t");
          outBuf.write(oracleJobNumber+"\t");
          outBuf.write(totalStudies+"\t\t");
          outBuf.write(totalStudyPatients+"\t\t");
          outBuf.write(totalUniquePatients+"\t\t");
          outBuf.write(totalLabTests+"\t\t");
          outBuf.newLine();

          outBuf.close();
       }
       catch(IOException ioexception)
       {
          ioexception.printStackTrace();
       }

    }

    private static int countUniquePatients(Map<String, Object> studies) {

        Study study = null;
        List<StudySubject> studySubjects = null;
        StudySubject studySubject = null;
        List<String> uniqueMRNs = new ArrayList<String>();
        int numberUniquePatients = 0;

        for (Iterator iter = studies.keySet().iterator(); iter.hasNext();) {
            String key = (String) iter.next();
            study = (Study) studies.get(key);
            if (study.getBTRISStudyIdentifier() != null) {
                studySubjects = study.getStudySubjects();
                for (Iterator<StudySubject> iterSS = studySubjects.iterator(); iterSS.hasNext();) {
                    studySubject = iterSS.next();
                    if (!uniqueMRNs.contains(studySubject.getMRN())) {
                        uniqueMRNs.add(studySubject.getMRN());
                        numberUniquePatients++;
                         
                    }
                }
            }
        }
        System.out.println(numberUniquePatients + " total unique patients");
        return numberUniquePatients;
    }

    private static int getWriteLabResults(Map<String, Object> studies, String qType, String outType, Calendar inQueryDate) {
        String connBTRISUser = userBTRIS;
        String connBTRISPass = passwordBTRIS;
        String connC3DUser  = userC3D;
        String connC3DPass  = passwordC3D;

        Connection conBTRIS = null;
        PreparedStatement stmtBTRIS = null;
        ResultSet rsBTRIS = null;
        Study study = null;
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
        int totalPatientRecords  = 0;
        int maxResultLength = 1000;
        int holdLength = 0;
        double progMaxStudies = studies.size();
        double progCntStudies = 0;
        SimpleDateFormat dateFormatSQLServer = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String queryDateStr = dateFormatSQLServer.format(inQueryDate.getTime());
        Properties extractProps = null;


        List<StudySubject> studySubjects = null;
        StudySubject studySubject = null;
        //String incrementalResults = null;
        String incrementalResults =
            "select Observation_Measurable.Event_GUID as Event_GUID, " +
            " Observation_Measurable.Observation_GUID as Observation_GUID, " +
            " Observation_Name_CONCEPT, Observation_Name, Substring(Observation_Value_Text,1,200), " +
            " Unit_of_Measure, Range, Primary_Date_Time, Observation_Measurable.Subject_GUID, " +
            " Subject.MRN, a.Value as LAB_TEST_CODE, Observation_Measurable.Date_Created, " +
            " Observation_Measurable.Date_Modified, Primary_Date_Time as LabTest_Reported_Date, " +
            " ? as QueryDate, GETDATE() as ExtractDateTime " +
            "  from Observation_Measurable, " +
            "       Subject, " +
            "       Observation_Measurable_EAV a " +
            " where MRN = ? " +
            " and (Observation_Measurable.Date_Created >= ? " +
            "      OR Observation_Measurable.Date_Modified >= ? )" +
            " and Observation_Measurable.Subject_GUID = Subject.Subject_GUID " +
            " and Observation_Measurable.Observation_GUID = a.Observation_GUID " +
            " and a.Attribute_CONCEPT = 'C139475' " +
            " and Observation_Measurable.Observation_Name_CONCEPT in " +
            "     (select Descendant_Concept " +
            "        from RED_Ancestor_Descendant_Identity " +
            "       where Ancestor_Concept = 'C90556') " +
            " and Observation_Measurable.Appl_Source_CD = 'C113093' ";

         String cumulativeResults =
            "select Observation_Measurable.Event_GUID as Event_GUID, " +
            " Observation_Measurable.Observation_GUID as Observation_GUID, " +
            " Observation_Name_CONCEPT, Observation_Name, Substring(Observation_Value_Text,1,200), " +
            " Unit_of_Measure, Range, Primary_Date_Time, Observation_Measurable.Subject_GUID, " +
            " Subject.MRN, a.Value as LAB_TEST_CODE, Observation_Measurable.Date_Created, " +
            " Observation_Measurable.Date_Modified, Primary_Date_Time as LabTest_Reported_Date, " +
            " ? as QueryDate, GETDATE() as ExtractDateTime " +
            "  from Observation_Measurable, " +
            "       Subject, " +
            "       Observation_Measurable_EAV a " +
            " where MRN = ? " +
            " and Observation_Measurable.Subject_GUID = Subject.Subject_GUID " +
            " and Observation_Measurable.Observation_GUID = a.Observation_GUID " +
            " and a.Attribute_CONCEPT = 'C139475' " +
            " and Observation_Measurable.Observation_Name_CONCEPT in " +
            "     (select Descendant_Concept " +
            "        from RED_Ancestor_Descendant_Identity " +
            "       where Ancestor_Concept = 'C90556') " +
            " and Observation_Measurable.Appl_Source_CD = 'C113093'";

         try {
             extractProps = PropertiesUtil.getPropertiesFromFile();
         } catch (Exception e) {
             e.printStackTrace();
         }

         String connBTRISUrl = extractProps.getProperty(Constants.NameControlBTRISURL);;
         String connC3DUrl   = extractProps.getProperty(Constants.NameControlC3DURL);

         if (qType.equals("CUMULATIVE"))
             holdQuery = cumulativeResults;
         else
             holdQuery = incrementalResults;
         System.out.print("Writing Lab Test Results [" + qType + "].");
         if (outType.equals("FILE") || outType.equals("BOTH")) {
             try { //Write the Header Line for the FILE
                 FileWriter outFile = new FileWriter( "BTRIS_Lab_Test_Results.txt", false );
                 BufferedWriter outBuf = new BufferedWriter(outFile);

                 outBuf.write("BTRIS Event ID\tBTRIS Observation ID\tBTRIS Lab Test ID\tLab Test Name\t");
                 outBuf.write("LabTest Result\tLabTest_UnitofMeasure\tLabTest_Range\tLabTest_DateTime\t");
                 outBuf.write("BTRIS_SubjectID\tPatientID\tLabTest_TestCode\tBTRIS_CreateDate\t");
                 outBuf.write("BTRIS_ModifyDate\tLabTest_ReportDate\tQueryDate\tExtractDate");
                 outBuf.newLine();
                 outBuf.close();
             } catch(IOException ioexception) {
               ioexception.printStackTrace();
             }
         }

         FileWriter outFile = null;
         BufferedWriter outBuf = null;
         Connection conC3D = null;
         PreparedStatement prepC3D = null;

         try { //Query Data
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            conBTRIS = DriverManager.getConnection(connBTRISUrl + ";user="
                       + connBTRISUser + ";password=" + connBTRISPass);

            if (outType.equals("FILE") || outType.equals("BOTH")) {
                outFile = new FileWriter( "BTRIS_Lab_Test_Results.txt", true);
                outBuf = new BufferedWriter(outFile);
            }
            if (outType.equals("TABLE") || outType.equals("BOTH")) {
                String SQLC3D_2 = "TRUNCATE TABLE BTRIS_LAB_TEST_RESULTS";
                String SQLC3D_3 =
                    "INSERT INTO BTRIS_LAB_TEST_RESULTS (" +
                    "BTRIS_EVENT_ID, BTRIS_OBSERVATION_ID, BTRIS_LAB_TEST_ID, " +
                    "LAB_TEST_NAME, LAB_RESULT, LAB_UNIT, " +
                    "LAB_RANGE, LAB_DATE_TXT, BTRIS_SUBJECT_ID, " +
                    "PATIENT_ID, LABTEST_CODE, BTRIS_CREATE_DATE, " +
                    "BTRIS_MODIFY_DATE, LAB_REPORT_DATE, EXTRACT_QUERY_DATE, " +
                    "EXTRACT_EXTRACT_DATE) " +
                    "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                try {
                    Class.forName("oracle.jdbc.driver.OracleDriver");
                    conC3D = DriverManager.getConnection(connC3DUrl,connC3DUser,connC3DPass);
                    System.out.println("Connected to C3D.");
                    Statement stmtC3D = conC3D.createStatement();
                    int rsInt = stmtC3D.executeUpdate(SQLC3D_2);
                    if (stmtC3D != null) try { stmtC3D.close(); } catch(Exception e) {}
                    System.out.println("Table Truncated.");
                    prepC3D = conC3D.prepareStatement(SQLC3D_3);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            printProgBar(0);

            List<String> uniqueMRNs = new ArrayList<String>();
            int numberUniquePatients = 0;

            for (Iterator iter = studies.keySet().iterator(); iter.hasNext();) {
                String key = (String) iter.next();
                study = (Study) studies.get(key);
                if (study.getBTRISStudyIdentifier() != null) {
                    studySubjects = study.getStudySubjects();
                    for (Iterator<StudySubject> iterSS = studySubjects.iterator(); iterSS.hasNext();) {
                        studySubject = iterSS.next();
                        int patRecords = 0;
                        if (!uniqueMRNs.contains(studySubject.getMRN())) {
                            uniqueMRNs.add(studySubject.getMRN());
                            numberUniquePatients++;
                           stmtBTRIS = conBTRIS.prepareStatement(holdQuery);
                           if (qType.equals("CUMULATIVE")) {
                               stmtBTRIS.setString(1, queryDateStr);
                               stmtBTRIS.setString(2, studySubject.getMRN());
                           } else {
                               stmtBTRIS.setString(1, queryDateStr);
                               stmtBTRIS.setString(2, studySubject.getMRN());
                               stmtBTRIS.setString(3, queryDateStr);
                               stmtBTRIS.setString(4, queryDateStr);
                           }
                           rsBTRIS = stmtBTRIS.executeQuery();
                           try {
                               while (rsBTRIS.next()) {
                                eventGuid = rsBTRIS.getString(1);            // BTRIS Event ID
                                observGuid = rsBTRIS.getString(2);           // Observation ID
                                observNameConcept = rsBTRIS.getString(3);    // Lab Test BTRIS ID
                                observName = rsBTRIS.getString(4);           // Lab Test Name
                                observValueText = rsBTRIS.getString(5);      // LabResult
                                unitOfMeasure = rsBTRIS.getString(6);        // Unit of Measure
                                range  = rsBTRIS.getString(7);               // Lab Range
                                primaryDateTime = rsBTRIS.getString(8);      // Primary Lab Date
                                subjectGuid = rsBTRIS.getString(9);          // BTRIS Subject ID
                                subjectMRN = rsBTRIS.getString(10);          // Subject MRN
                                labTestCode = rsBTRIS.getString(11);         // Lab Test COde
                                dateCreated = rsBTRIS.getString(12);         // Create Date
                                dateModified = rsBTRIS.getString(13);        // Modify Date
                                labTestReportedDate = rsBTRIS.getString(14); // Lab Test Date
                                queryDate = rsBTRIS.getString(15);        // Query Date
                                extractDate = rsBTRIS.getString(16);      // ExtractDate

                                patRecords = patRecords + 1;
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
                                    outBuf.write(extractDate);
                                    outBuf.newLine();
                                   }
                                   if (outType.equals("TABLE") || outType.equals("BOTH")) {
                                       prepC3D.setString(1,  eventGuid);
                                       prepC3D.setString(2,  observGuid);
                                       prepC3D.setString(3,  observNameConcept);
                                       if (observName.length() > 1000) {prepC3D.setString(4,  observName.substring(0,1000));}
                                                                       else {prepC3D.setString(4,  observName);}
                                       prepC3D.setString(5,  observValueText);
                                       prepC3D.setString(6,  unitOfMeasure);
                                       prepC3D.setString(7,  range);
                                       prepC3D.setString(8,  primaryDateTime);
                                       prepC3D.setString(9,  subjectGuid);
                                       prepC3D.setString(10, subjectMRN);
                                       prepC3D.setString(11, labTestCode);
                                       prepC3D.setString(12, dateCreated);
                                       prepC3D.setString(13, dateModified);
                                       prepC3D.setString(14, labTestReportedDate);
                                       prepC3D.setString(15, queryDate);
                                       prepC3D.setString(16, extractDate);
                                       prepC3D.executeUpdate();
                                   }
                               }
                           } catch(Exception e) {
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
                               System.out.println(extractDate + "'");
                           }

                           totalPatientRecords  = totalPatientRecords + patRecords;
                        }
                        
                    }
                }
                progCntStudies = progCntStudies + 1;
                printProgBar((int) ((progCntStudies/progMaxStudies)*100));
            }
            System.out.println(totalPatientRecords + " total records");
            System.out.println(numberUniquePatients + " total unique patients");

            if (outType.equals("FILE") || outType.equals("BOTH")) {
                outBuf.close();
            }

       } catch(Exception e) {
           e.printStackTrace();
       }
       finally {
           if (conC3D != null) try { conC3D.close(); } catch(Exception e) {}
       }
       return totalPatientRecords;
    }

    private static int getWriteLabResultsCUMU(String outType, Calendar inQueryDate) {
        String connBTRISUser = userBTRIS;
        String connBTRISPass = passwordBTRIS;
        String connC3DUser  = userC3D;
        String connC3DPass  = passwordC3D;

        Connection conBTRIS = null;
        PreparedStatement stmtBTRIS = null;
        ResultSet rsBTRIS = null;
        Study study = null;
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
        int totalPatientRecords  = 0;
        int maxResultLength = 1000;
        int holdLength = 0;
        double progMaxStudies = 0; //studies.size();
        double progCntStudies = 0;
        SimpleDateFormat dateFormatSQLServer = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String queryDateStr = dateFormatSQLServer.format(inQueryDate.getTime());
        Properties extractProps = null;

        String cumulativeResults =
            "select Observation_Measurable.Event_GUID as Event_GUID, " +
            " Observation_Measurable.Observation_GUID as Observation_GUID, " +
            " Observation_Name_CONCEPT, Observation_Name, Observation_Value_Text, " +
            " Unit_of_Measure, Range, Primary_Date_Time, Observation_Measurable.Subject_GUID, " +
            " Subject.MRN, a.Value as LAB_TEST_CODE, Observation_Measurable.Date_Created, " +
            " Observation_Measurable.Date_Modified, Primary_Date_Time as LabTest_Reported_Date, " +
            " ? as QueryDate, GETDATE() as ExtractDateTime " +
            "  from Observation_Measurable, " +
            "       Subject, " +
            "       Observation_Measurable_EAV a, " +
            "       Event_Measurable_EAV b " +
            " where Observation_Measurable.Subject_GUID = Subject.Subject_GUID " +
            "   and Observation_Measurable.Observation_GUID = a.Observation_GUID " +
            "   and a.Attribute_CONCEPT = 'C139475' " +
            "   and Observation_Measurable.Event_GUID = b.Event_GUID " +
            "   and b.Attribute_CONCEPT = 'C128597'  " +
            "   and Observation_Measurable.Observation_Name_CONCEPT in " +
            "       (select Descendant_Concept " +
            "          from RED_Ancestor_Descendant_Identity " +
            "         where Ancestor_Concept = 'C90151') ";

        try {
            extractProps = PropertiesUtil.getPropertiesFromFile();
        } catch (Exception e) {
            e.printStackTrace();
        }

        String connBTRISUrl = extractProps.getProperty(Constants.NameControlBTRISURL);;
        String connC3DUrl   = extractProps.getProperty(Constants.NameControlC3DURL);

        holdQuery = cumulativeResults;

        System.out.print("Writing Lab Test Results [CUMULATIVE].");
        if (outType.equals("FILE") || outType.equals("BOTH")) {
            try { //Write the Header Line for the FILE
                FileWriter outFile = new FileWriter( "BTRIS_Lab_Test_Results.txt", false );
                BufferedWriter outBuf = new BufferedWriter(outFile);

                outBuf.write("BTRIS Event ID\tBTRIS Observation ID\tBTRIS Lab Test ID\tLab Test Name\t");
                outBuf.write("LabTest Result\tLabTest_UnitofMeasure\tLabTest_Range\tLabTest_DateTime\t");
                outBuf.write("BTRIS_SubjectID\tPatientID\tLabTest_TestCode\tBTRIS_CreateDate\t");
                outBuf.write("BTRIS_ModifyDate\tLabTest_ReportDate\tQueryDate\tExtractDate");
                outBuf.newLine();
                outBuf.close();
            } catch(IOException ioexception) {
              ioexception.printStackTrace();
            }
        }

        FileWriter outFile = null;
        BufferedWriter outBuf = null;
        Connection conC3D = null;
        PreparedStatement prepC3D = null;

        try { //Query Data
           Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
           conBTRIS = DriverManager.getConnection(connBTRISUrl + ";user="
                       + connBTRISUser + ";password=" + connBTRISPass);

           if (outType.equals("FILE") || outType.equals("BOTH")) {
               outFile = new FileWriter( "BTRIS_Lab_Test_Results.txt", true);
               outBuf = new BufferedWriter(outFile);
           }
           if (outType.equals("TABLE") || outType.equals("BOTH")) {
               String SQLC3D_2 = "TRUNCATE TABLE BTRIS_LAB_TEST_RESULTS";
               String SQLC3D_3 =
                   "INSERT INTO BTRIS_LAB_TEST_RESULTS (" +
                   "BTRIS_EVENT_ID, BTRIS_OBSERVATION_ID, BTRIS_LAB_TEST_ID, " +
                   "LAB_TEST_NAME, LAB_RESULT, LAB_UNIT, " +
                   "LAB_RANGE, LAB_DATE_TXT, BTRIS_SUBJECT_ID, " +
                   "PATIENT_ID, LABTEST_CODE, BTRIS_CREATE_DATE, " +
                   "BTRIS_MODIFY_DATE, LAB_REPORT_DATE, EXTRACT_QUERY_DATE, " +
                   "EXTRACT_EXTRACT_DATE) " +
                   "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
               try {
                   Class.forName("oracle.jdbc.driver.OracleDriver");
                   conC3D = DriverManager.getConnection(connC3DUrl,connC3DUser,connC3DPass);
                   System.out.println("Connected to C3D.");
                   Statement stmtC3D = conC3D.createStatement();
                   int rsInt = stmtC3D.executeUpdate(SQLC3D_2);
                   if (stmtC3D != null) try { stmtC3D.close(); } catch(Exception e) {}
                   System.out.println("Table Truncated.");
                   prepC3D = conC3D.prepareStatement(SQLC3D_3);
               } catch (Exception e) {
                   e.printStackTrace();
               }
            }

            printProgBar(0);

            int patRecords = 0;

            stmtBTRIS = conBTRIS.prepareStatement(holdQuery);
            stmtBTRIS.setString(1, queryDateStr);

            rsBTRIS = stmtBTRIS.executeQuery();
            try {
                while (rsBTRIS.next()) {
                    eventGuid           = rsBTRIS.getString(1);  // BTRIS Event ID
                    observGuid          = rsBTRIS.getString(2);  // Observation ID
                    observNameConcept   = rsBTRIS.getString(3);  // Lab Test BTRIS ID
                    observName          = rsBTRIS.getString(4);  // Lab Test Name
                    observValueText     = rsBTRIS.getString(5);  // LabResult
                    unitOfMeasure       = rsBTRIS.getString(6);  // Unit of Measure
                    range               = rsBTRIS.getString(7);  // Lab Range
                    primaryDateTime     = rsBTRIS.getString(8);  // Primary Lab Date
                    subjectGuid         = rsBTRIS.getString(9);  // BTRIS Subject ID
                    subjectMRN          = rsBTRIS.getString(10); // Subject MRN
                    labTestCode         = rsBTRIS.getString(11); // Lab Test COde
                    dateCreated         = rsBTRIS.getString(12); // Create Date
                    dateModified        = rsBTRIS.getString(13); // Modify Date
                    labTestReportedDate = rsBTRIS.getString(14); // Lab Test Date
                    queryDate           = rsBTRIS.getString(15); // Query Date
                    extractDate         = rsBTRIS.getString(16); // ExtractDate

                    patRecords = patRecords + 1;
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
                        outBuf.write(extractDate);
                        outBuf.newLine();
                    }
                    if (outType.equals("TABLE") || outType.equals("BOTH")) {
                        prepC3D.setString(1,  eventGuid);
                        prepC3D.setString(2,  observGuid);
                        prepC3D.setString(3,  observNameConcept);
                        if (observName.length() > 1000) {prepC3D.setString(4,  observName.substring(0,1000));}
                                                        else {prepC3D.setString(4,  observName);}
                        prepC3D.setString(5,  observValueText);
                        prepC3D.setString(6,  unitOfMeasure);
                        prepC3D.setString(7,  range);
                        prepC3D.setString(8,  primaryDateTime);
                        prepC3D.setString(9,  subjectGuid);
                        prepC3D.setString(10, subjectMRN);
                        prepC3D.setString(11, labTestCode);
                        prepC3D.setString(12, dateCreated);
                        prepC3D.setString(13, dateModified);
                        prepC3D.setString(14, labTestReportedDate);
                        prepC3D.setString(15, queryDate);
                        prepC3D.setString(16, extractDate);
                        prepC3D.executeUpdate();
                    }
                }
            } catch(Exception e) {
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
                System.out.println(extractDate + "'");
            }

            totalPatientRecords  = totalPatientRecords + patRecords;
            progCntStudies = progCntStudies + 1;
            printProgBar((int) ((progCntStudies/progMaxStudies)*100));

            System.out.println(totalPatientRecords + " total records");
            if (outType.equals("FILE") || outType.equals("BOTH")) {
                outBuf.close();
            }

       } catch(Exception e) {
           e.printStackTrace();
       }
       finally {
           if (conC3D != null) try { conC3D.close(); } catch(Exception e) {}
       }
       return totalPatientRecords;
    }

    private static void executeExtraction() {
        Study study = null;
        Map<String, Object> studies = new HashMap<String, Object>();
        int totalStudies, totalPatients, totalLabTests = 0;
        String extractType = null;

        DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        Calendar todaysDate = Calendar.getInstance();

        String dateStart = dateFormat.format(todaysDate.getTime());
        String dateExtract = dateFormat.format(todaysDate.getTime());

        Properties extractProps = null;

        try {
            extractProps = PropertiesUtil.getPropertiesFromFile();
        } catch (Exception e) {
            e.printStackTrace();
        }

        String cumulativeDay = extractProps.getProperty(Constants.NameControlCumulativeDay);
        String offsetDaysStr = extractProps.getProperty(Constants.NameControlQOffsetDays);
        String outputType = extractProps.getProperty(Constants.NameControlOutputType);
        String submitPushC3DJob = extractProps.getProperty(Constants.NameControlSubmitPushC3DJob);

        if (todaysDate.get(Calendar.DAY_OF_WEEK) == Integer.parseInt(cumulativeDay)) {
            extractType = "CUMULATIVE";
        } else {
            extractType = "INCREMENTAL";
        }

        ExtractLog lastExtractLog = new ExtractLog();
        lastExtractLog.getLastExtraction();

        // Get the old query date to determin NEW query date, if not exist,
        // then NEW query date is today "plus" the Query Date Offset Constant.
        Calendar newQueryDate = Calendar.getInstance();
        int offSetDaysInt = Integer.parseInt(offsetDaysStr);
        String newQueryDateStr = lastExtractLog.getStartDt();
        if (newQueryDateStr == null || "".equals(newQueryDateStr)) {
            System.out.println("");
        } else {
            try {
                newQueryDate.setTime(dateFormat.parse(newQueryDateStr));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        newQueryDate.add(Calendar.DATE, offSetDaysInt);
        newQueryDate.set(Calendar.HOUR_OF_DAY,0);
        newQueryDate.set(Calendar.MINUTE,0);
        newQueryDate.set(Calendar.SECOND,0);
        newQueryDate.set(Calendar.MILLISECOND,0);

        System.out.println("New Query Date is :" + newQueryDate.getTime());

        // Processing starts Here!
        studies = getC3DStudies();

        double progMaxStudies = studies.size();
        double progCntStudies = 0;

        studies = findUpdateBTRISStudies(studies);

        System.out.println();
        ExtractLog extractLog = new ExtractLog();
        extractLog.setStartDt(dateStart);
        extractLog.setQueryDt(dateFormat.format(newQueryDate.getTime()));
        extractLog.setExtractType(extractType);

        System.out.println("Writing Protocol to " + outputType);
        extractLog.setStudyCnt(writeProtocols(studies, outputType));

        System.out.println("Writing Protocol/Patient to " + outputType);
        extractLog.setStudyPatientCnt(writeProtPatients(studies, outputType));

        extractLog.setUniquePatientCnt(countUniquePatients(studies));

        System.out.println("Writing " + extractType + " Test Results to " + outputType);
        extractLog.setResultCnt(getWriteLabResults(studies, extractType, outputType, newQueryDate));

        if (submitPushC3DJob.equalsIgnoreCase("TRUE")) {
           extractLog = submitC3DPush(extractLog);
        }

        Date date = new Date();
        extractLog.setFinishDt(dateFormat.format(date));
        writeLogAndStatus(extractLog);
    }

    private static void showCommandLineHelp() {
        System.out.println("BTRIS Data Extractor for C3D");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("-? -help\t\tCommand-line help information");
        System.out.println("-version\t\tRelease number.");
        System.out.println("-BU:username\t\tBTRIS User Id.");
        System.out.println("-BP:password\t\tBTRIS User Id password.");
        System.out.println("-CU:username\t\tC3D User Id.");
        System.out.println("-CP:password\t\tC3D User Id password.");
        System.out.println();
    }

    private static void showVersion() {
        System.out.println("BTRIS Data Extractor for C3D");
        System.out.println();
        System.out.println("Version: 1.0");
        System.out.println();
    }

    private static void showUnknownCommand(String s) {
        System.out.println("BTRIS Data Extractor for C3D");
        System.out.println();
        System.out.println("'" + s +"' is an unknown command.  Use -? for list of valid command options.");
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
            Boolean repeat = true;
            while ((s == null) || "".equals(s)) {
                System.out.print(prompt + " ");

                br = new BufferedReader(new InputStreamReader(System.in));
                try {
                    s = br.readLine();
                } catch  (IOException e) {
                        System.out.println(e);
                }
            }
        }
        return s;
    }

    public static void main (String[] args) {

        DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

        int sleepCount = 0;
        Calendar date = null;
        Properties extractProps = null;
        String extractHour      = null;
        String extractMinute    = null;
        String extractSleep     = null;
        String extractType      = null;
        Calendar lastStartDate  = null;
        Calendar todayExecuteDate = null;
        Calendar holdTodayExecuteDate = null;
        Calendar holdLastStartDate = null;
        Long nowMillis = null;
        Long extractMillis = null;
        Long diffMillis = null;
        Long sleepMillis = null;

        System.out.println("BTRIS Data Extractor for C3D, v1.0");
        try {
            extractProps = PropertiesUtil.getPropertiesFromFile();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // get the BTRIS and C3D database connection values from the properties file
        urlBTRIS     = extractProps.getProperty(Constants.NameControlBTRISURL);
        userBTRIS     = extractProps.getProperty(Constants.NameControlBTRISUser);
        passwordBTRIS = extractProps.getProperty(Constants.NameControlBTRISPass);
        urlC3D       = extractProps.getProperty(Constants.NameControlC3DURL);
        userC3D       = extractProps.getProperty(Constants.NameControlC3DUser);
        passwordC3D   = extractProps.getProperty(Constants.NameControlC3DPass);

        // check for command line arguements
        for (String s: args) {
            System.out.println(s);
            if (s.equals("-?") || s.equalsIgnoreCase("-help") )  {      //Command Line Help
                showCommandLineHelp();
                if (args.length == 1) {
                    System.exit(0);
                }
            } else if (s.startsWith("-BU:")) {   //BTRIS Userid, OVERRIDES value from property file
                userBTRIS = s.substring(4);
            } else if (s.startsWith("-BP:")) {   //BTRIS Password, OVERRIDES value from property file
                passwordBTRIS = s.substring(4);
            } else if (s.startsWith("-CU:")) {   //C3D Userid, OVERRIDES value from property file
                userC3D = s.substring(4);
            } else if (s.startsWith("-CP:")) {   //C3D Pasword, OVERRIDES value from property file
                passwordC3D = s.substring(4);
            } else {                             //All other arguements results in Unknown Command Message
                showUnknownCommand(s);
                if (args.length == 1) {
                    System.exit(0);
                }
            }
        }

        //If BTRIS Username not found, PROMPT for it
        if ((userBTRIS == null) || "".equals(userBTRIS))
            userBTRIS = getPromptedString("BTRIS username required: ", false);

        //If BTRIS Password not found, PROMPT for it
        if ((passwordBTRIS == null) || "".equals(passwordBTRIS))
            passwordBTRIS = getPromptedString("BTRIS password required: ",true);

        //If C3D Username not found, PROMPT for it
        if ((userC3D == null) || "".equals(userC3D))
            userC3D = getPromptedString("C3D username required: ", false);

        //If C3D Password not found, PROMPT for it
        if ((passwordC3D == null) || "".equals(passwordC3D))
            passwordC3D = getPromptedString("C3D password required: ",true);

        while (true) { // Loop forever!  User will need to kill processing from console (Ctrl-C)

            try { // Re-load the properties from file, incase of changes since last load.
                extractProps = PropertiesUtil.getPropertiesFromFile();
            } catch (Exception e) {
                e.printStackTrace();
            }

            extractHour = extractProps.getProperty(Constants.NameControlExtractHour);
            extractMinute = extractProps.getProperty(Constants.NameControlExtractMinute);
            extractSleep = extractProps.getProperty(Constants.NameControlChkInterval);

            ExtractLog lastExtractLog = new ExtractLog();
            lastExtractLog.getLastExtraction();

            try { // Get the Last Extract Start Date, if missing use 01/01/01
                if (lastExtractLog.getStartDt() == null)  {
                    lastExtractLog.setStartDt("01/01/2001 00:00:00");
                }
                lastStartDate = Calendar.getInstance();
                lastStartDate.setTime(dateFormat.parse(lastExtractLog.getStartDt()));
            } catch (Exception e) {
                e.printStackTrace();
            }

            try { // Get the NEXT extract DATE/TIME
                todayExecuteDate = Calendar.getInstance();
                todayExecuteDate.set(Calendar.HOUR_OF_DAY,Integer.parseInt(extractHour));
                todayExecuteDate.set(Calendar.MINUTE,Integer.parseInt(extractMinute));
                todayExecuteDate.set(Calendar.SECOND,0);
                todayExecuteDate.set(Calendar.MILLISECOND,0);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if ( (todayExecuteDate.after(lastStartDate)) &&
                 (todayExecuteDate.before(Calendar.getInstance())) ) {
                System.out.println("Waking.");
                sleepCount = 0;
                executeExtraction();
            } else {
                sleepCount++;
                if ((holdTodayExecuteDate == null) ||
                    (todayExecuteDate.compareTo(holdTodayExecuteDate) != 0)  ||
                    (lastStartDate.compareTo(holdLastStartDate) != 0) ) {
                    System.out.println("Last: [" + lastStartDate.getTime() + "].");
                    System.out.println(" Now: [" + Calendar.getInstance().getTime() + "]. " );
                    System.out.println("Next: [" + todayExecuteDate.getTime() + "]. " );
                }
        holdTodayExecuteDate = Calendar.getInstance();
                holdTodayExecuteDate.setTime(todayExecuteDate.getTime());
                holdLastStartDate = Calendar.getInstance();
                holdLastStartDate.setTime(lastStartDate.getTime());

                try {
                    nowMillis = Calendar.getInstance().getTimeInMillis();
                    extractMillis = todayExecuteDate.getTimeInMillis();
                    diffMillis = extractMillis - nowMillis;
                    sleepMillis = 1000*Long.parseLong(extractSleep);
                    if ((diffMillis < 0) || (diffMillis >= sleepMillis)) {
                        System.out.print("Sleeping [" + extractSleep + " sec](" + sleepCount + ").\r");
                        Thread.sleep(sleepMillis);
                    } else {
                        System.out.print("Sleeping [" + (diffMillis/1000) + " sec](" + sleepCount + ").\r");
                        Thread.sleep(diffMillis);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

