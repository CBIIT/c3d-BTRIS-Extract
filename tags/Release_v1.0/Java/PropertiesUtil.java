
import java.io.*;
import java.util.Properties;
import java.util.Set;
import java.util.Iterator;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class PropertiesUtil {

    private static Properties props;
    private static String propertiesFileLocation;

    public static Properties getPropertiesFromFile() throws Exception {

        //System.out.println("Reading ControlFile '" + Constants.ControlFile + "'");
        File infile = new File(Constants.ControlFile);

        if (!infile.exists()) {

            System.out.println("ControlFile '" + Constants.ControlFile + "' does not exist. "
                       + "Creating default.");
            try {
                writeDefaultPropertiesToFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        InputStream inputStream = null;
        Properties properties = new Properties();

        try {
            inputStream = new FileInputStream(infile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (inputStream != null) {
            //System.out.println("inputStream != null");
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //else {
        //    System.out.println("inputStream = null");
        //}
        if (!properties.isEmpty()) {
            //System.out.println("!properties.isEmpty()");

            try {
                validateProperties(properties);
            } catch (Exception e) {
                e.printStackTrace();
            }

            props = properties;
            //System.out.println("props = properties;");
        }

        return props;
    }


    public static void writeDefaultPropertiesToFile() throws Exception {

        System.out.println("ControlFile='" + Constants.ControlFile + "'");
        File outfile = new File(Constants.ControlFile);

        if (outfile.exists()) {

            throw new Exception("File already exists, cannot create Default Properties File");
        }

        DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH24:mm:ss");
        Date date = new Date();

        try {
            BufferedWriter outputStream = new BufferedWriter(new FileWriter(outfile));
            outputStream.write("! BTRIS Data Extractor Properies File");
            outputStream.newLine();
            outputStream.write("! Created by: BTRIS Data Extractor [writeDefaultPropertiesToFile]");
            outputStream.newLine();
            outputStream.write("! Created on: " + dateFormat.format(date));
            outputStream.newLine();
            outputStream.write("!");
            outputStream.newLine();
            outputStream.write("! " + Constants.DescControlChkInterval);
            outputStream.newLine();
            outputStream.write(Constants.NameControlChkInterval + " = " + Constants.ControlChkInterval);
            outputStream.newLine();
            outputStream.write("! " + Constants.DescControlExtractHour);
            outputStream.newLine();
            outputStream.write(Constants.NameControlExtractHour + " = " + Constants.ControlExtractHour);
            outputStream.newLine();
            outputStream.write("! " + Constants.DescControlExtractMinute);
            outputStream.newLine();
            outputStream.write(Constants.NameControlExtractMinute + " = " + Constants.ControlExtractMinute);
            outputStream.newLine();
            outputStream.write("! " + Constants.DescControlCumulativeDay);
            outputStream.newLine();
            outputStream.write(Constants.NameControlCumulativeDay + " = " + Constants.ControlCumulativeDay);
            outputStream.newLine();
            outputStream.write("! " + Constants.DescControlOutputType);
            outputStream.newLine();
            outputStream.write(Constants.NameControlOutputType + " = " + Constants.ControlOutputType);
            outputStream.newLine();
            outputStream.write("! " + Constants.DescControlSubmitPushC3DJob );
            outputStream.newLine();
            outputStream.write(Constants.NameControlSubmitPushC3DJob + " = " + Constants.ControlSubmitPushC3DJob);
            outputStream.newLine();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties loadProperties(String path) throws Exception{
        //propertiesFileLocation=path+"/c3dgridservice.properties";
        getPropertiesFromFile();
        props.put("CONF_FILES_DIR", path);
        return props;
    }

    private static String checkProperty(Properties props, String propName, String propValue, String errorMessage) {

        if (props.containsKey(propName)) {
            String propValueStr = props.getProperty(propName);
            //System.out.println("Property '"+ propName +"' = '" + propValueStr + "'.");

            if (!(propValueStr  != null && propValueStr.length() > 0)) {
               errorMessage = errorMessage + ": " + propName
                               + " value is blank, using default value '" + propValue + "'";
               props.put(propName, propValue);
               return errorMessage;
            }
        } else {
           errorMessage = errorMessage + ": " + propName
                           + " property is missing, using default value '"
                           + propValue + "'";
           props.put(propName, propValue);
           return errorMessage;
        }
        return errorMessage;
    }


    private static void validateProperties(Properties properties)
            throws Exception {

        String errorMessage = "";
        boolean isValue = true;

        errorMessage = checkProperty(properties, Constants.NameControlChkInterval, Constants.ControlChkInterval, errorMessage);

        errorMessage = checkProperty(properties, Constants.NameControlExtractHour, Constants.ControlExtractHour, errorMessage);

        errorMessage = checkProperty(properties, Constants.NameControlExtractMinute, Constants.ControlExtractMinute, errorMessage);

        errorMessage = checkProperty(properties, Constants.NameControlCumulativeDay, Constants.ControlCumulativeDay, errorMessage);

        errorMessage = checkProperty(properties, Constants.NameControlOutputType, Constants.ControlOutputType, errorMessage);

        errorMessage = checkProperty(properties, Constants.NameControlQOffsetDays, Constants.ControlQOffsetDays, errorMessage);

        errorMessage = checkProperty(properties, Constants.NameControlSubmitPushC3DJob, Constants.ControlSubmitPushC3DJob, errorMessage);

        if (!("".equals(errorMessage)) || errorMessage.length() > 0 ){
            System.out.println("Property Warning! '"+ errorMessage +"'.");
            //throw new Exception(errorMessage);
        }

    }

    public static void showProperties()
    throws Exception {
        Set properties;
        String str = null;

        if (props == null) {
            try {
                getPropertiesFromFile();
            } catch (Exception e) {

            }
        }

        System.out.println("showProperties();");
        properties = props.keySet();
        Iterator itr = properties.iterator();
        while (itr.hasNext()){
            str = (String) itr.next();
            System.out.println("Property '"+ str +"' = '" + props.getProperty(str)+ "'.");
        }
        System.out.println();

    }
}
