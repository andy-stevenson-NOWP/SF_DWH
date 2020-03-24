/*
 * This is the licence header, don't really know what to put here
 * This is a custom Java app for NOW: Pensions, it's not licenced to any-one else
 * If you find yourself using it and you're not a NP employee chastise yourself serverly and delete this immidiatley
 */

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Connect to Salesforce, connect to SQL Server, bring across a defined list of SF objects
 * Not forgetting we need to generate a WSDL jar file, like this:
 * java -classpath force-wsc-38.0.4.jar;antlr-runtime-3.5.2.jar;js.jar;ST-4.0.8.jar;tools.jar com.sforce.ws.tools.wsdlc partner.wsdl partner.jar
 * After generating the WSDL here:
 * https://cs15.salesforce.com/ui/setup/sforce/WebServicesSetupPage?setupid=WebServices&retURL=%2Fui%2Fsetup%2FSetup%3Fsetupid%3DDevToolsIntegrate
 * 
 * @author andy.stevenson
 * @version 1.1.1
 * 
 * Change History
 * Date       Authour        Notes
 * 2017-09-26 Andy Stevenson Muti thread the object replication
 * 2018-01-18                Limit to 210 threads - SF connection limit
 * 
 * 
 * TODO: Get picklist values for relevant fields
 *       Recover from a failed connection gracefully
 * 
 * FIXME: query locator errors thrown if the querymore() method is invoked 'cos it doesn't know which query we want more from.
 *        https://developer.salesforce.com/forums/?id=906F00000005LQoIAM
 */
public class SFReplication {

    static PartnerConnection sfConnection; //Salesforce connector
    static ConfigProperties properties;
    static ConnectorConfig config;
    
    static Connection ssConnection; //SQL Server connector
    static Statement statement;
    static Logger logger = new Logger(); //set up logging
    static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    static DateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static Calendar now; //a Calendar object for date calcs
    static Integer connectionPause = 10; //if the SF connection fails wait this many minutes before retrying
    static String schema = "[testing]"; //default mode is DEBUG, set testing schema
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args){
        logger.log("Process Starts", null, "SFReplication", null, false);
        
        //parse the arguments
        String mode = "DEBUG"; //default mode is DEBUG
        if (args.length > 0){ //capture no arguments
            switch (args[0]){
                case "prod": mode = "PRODUCTION";
                    schema = "[replicate]";
                    break;
                case "rebuild": mode = "REBUILD";
                    schema = "[replicate]";
                    break;
            }
        }
        logger.log("Mode set to:" + mode, null, "SFReplication", null, false);

        //SF authentication values from the properties file
        properties = new ConfigProperties();
        String USERNAME = properties.getPropertyValue("USERNAME");
        String PASSWORD = properties.getPropertyValue("PASSWORD");
        String SECURITY_TOKEN = properties.getPropertyValue("SECURITY_TOKEN");
        
        //SF connector config
        config = new ConnectorConfig();
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD+SECURITY_TOKEN);

        try{ //connect to SF API & SQL Server
            logger.log("Connecting to SF API", null, "SFReplication", null,false);
            sfConnection = Connector.newConnection(config);
            sfConnection.setQueryOptions(2000); // set batch size to max
            
            logger.log("Connecting to SQL Server", null, "SFReplication", null, false);
            String url ="jdbc:sqlserver://NPSQL01;databaseName=NOW_SF_DWH;integratedSecurity=true";
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            ssConnection = DriverManager.getConnection(url);
            statement = ssConnection.createStatement();
            
            statement.execute("set ansi_warnings off"); //turn off ansi warnings so we get auto truncating of strings if we need it
            if (mode.equals("PRODUCTION")){
                statement.execute("update [replicate].[status] set [Status] = -1;"); //tell the database we're in process
            }
            now = Calendar.getInstance();

        }catch (ConnectionException e){
            logger.log("ConnectionException in main connecting to SF:"+e.toString(), null, "SFReplication", null, true);
        }catch (ClassNotFoundException e){
            logger.log("ClassNotFoundException in main connecting to SQL Server:"+e.toString(), null, "SFReplication", null, true);
        }catch (SQLException e){
            logger.log("SQLException in main connecting to SQL Server:"+e.toString(), null, "SFReplication", null, true);
        }

        //grab a list of objects in SF to validate against
        String[] SFObjects = listObjects();
        
        //DEBUG: List all the SF objects
        //for(String SFObject : SFObjects){
        //    System.out.println(SFObject);
        //}
        
        //get the list of required objects from the input file
        String[] SFObjectList = readLines("C:\\Scripts\\SFReplicate\\SFObjectList.txt");
        //String[] SFObjectList = {"Case"}; //DEBUG

        //Track threads, only 10 allowed at a time (SF Connection limit)
        ExecutorService executor = Executors.newFixedThreadPool(1);
        
        if (SFObjects.length > 0){
            for (String object : SFObjectList){ //loop through each object
                if (Arrays.asList(SFObjects).contains(object)){ //is the object valid?
                    //Multithread a connection to deal with each object
                    Runnable nextThread = new ObjectThread(schema, object, mode, config);
                    executor.execute(nextThread);
 
                }else{
                    logger.log("Not a valid object name", object, "SFReplication", null, true);
                }
            } //next object
            
        }else{
            logger.log("Object list failed to load", null, "SFReplication", null, true);
        }
       
        executor.shutdown(); //prevent any more threads (not really needed...)
        while (!executor.isTerminated()) { //wait for all threads to complete
        }
        logger.log("All threads complete", null, "SFReplication", null, false);

        //send an email if we've logged any errors
        if (!logger.getEmailMessage().isEmpty()){
            Email email = new Email();
            email.sendMail("andy.stevenson@nowpensions.com", "SF Replication Errors", logger.getEmailMessage());
            logger.log("Error email sent", null, "SFReplication", null, false);
            if (mode.equals("PRODUCTION")){
                try {
                    statement.execute("update [replicate].[status] set [Status] = 0;"); //tell SQL Server we're not happy (this blocks the refresh scripts)
                }catch (SQLException e){
                    logger.log("SQLException in main:" + e.toString(), null, "SFReplication", null, true);
                }
            }
        } else {
            if (mode.equals("PRODUCTION")){
                try {
                    statement.execute("update [replicate].[status] set [Status] = 1;"); //SQL Server is good to refresh
                }catch (SQLException e){
                    logger.log("SQLException in main:" + e.toString(), null, "SFReplication", null, true);
                }
            }
        }

        // Clean up
        try{
            //turn ansi warning back on
            statement.execute("set ansi_warnings on");
            statement.close();
            ssConnection.close();
            sfConnection.logout();
            
        }catch (SQLException e){
            logger.log("SQLException in main:" + e.toString(), null, "SFReplication", null, true);
        } catch (ConnectionException e) {
            logger.log("ConnectionException in main:" + e.toString(), null, "SFReplication", null, true);
        }
        if (mode.equals("DEBUG")){ // Test the email
            Email email = new Email();
            email.sendMail("andy.stevenson@nowpensions.com", "SF Replication Complete", "A Test message");
        }
        logger.log("Process Ends", null, "SFReplication", null, false);
    }

    /**
     * Get an array of objects in Salesforce
     * @return String[] objectList
     */
    private static String[] listObjects(){
        String[] objectList = new String[1];
        
        try{
            // Make the describeGlobal() call
            DescribeGlobalResult describeGlobalResult = sfConnection.describeGlobal();

            // Get the sObjects from the describe global result
            DescribeGlobalSObjectResult[] sobjectResults = describeGlobalResult.getSobjects();
            objectList = new String[sobjectResults.length];

            for (int i = 0; i < sobjectResults.length; i++){
                objectList[i] = sobjectResults[i].getName();
            }
            
        } catch (ConnectionException e) {
            logger.log("ConnectionException in listObjects:"+e.toString(), null, "SFReplication", "listObjects", true);
        }

        return objectList;
    }
 
    /**
     * read a file of Salesforce objects into an array
     * Ignore any line that starts with #
     * (SFObjectList.txt)
     * @param filename
     * @return 
     */
    private static String[] readLines(String filename){
        FileReader fileReader = null;
        List<String> lines = new ArrayList<>();
        
        try{
            fileReader = new FileReader(filename);
            try (BufferedReader bufferedReader = new BufferedReader(fileReader)){
                String line; // = null;
                while ((line = bufferedReader.readLine()) != null){
                    if (!line.substring(0,1).equals("#")){
                        lines.add(line);
                    }
                }
            }
            
        }catch (FileNotFoundException e){
            logger.log("FileNotFoundException in readLines:"+e.toString(), null, "SFReplication", "readLines", true);
        }catch (IOException e){
            logger.log("FileNotFoundException in readLines:"+e.toString(), null, "SFReplication", "readLines", true);
        }finally{
            try{
                if (fileReader != null){
                    fileReader.close();
                }
            }catch (IOException e){
                logger.log("FileNotFoundException in readLines:"+e.toString(), null, "SFReplication", "readLines", true);
            }
        }
        
        return lines.toArray(new String[lines.size()]);
    }
}