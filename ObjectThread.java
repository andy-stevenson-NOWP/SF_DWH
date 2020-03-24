/*
 * This is the licence header, don't really know what to put here
 * This is a custom Java app for NOW: Pensions, it's not licenced to any-one else
 * If you find yourself using it and you're not a NP employee chastise yourself severely and delete this immediately
 */
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.DeletedRecord;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.GetDeletedResult;
import com.sforce.soap.partner.GetUpdatedResult;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author andy.stevenson
 * This is the thread for each object
 * Be careful with static and non static variables...
 * 
 */
public class ObjectThread implements Runnable {
    
    private static String schema;
    private String object;
    private static String mode;
    private String tableName;
    private static final Integer CHARLIMIT = 5000; //varchar value
    //private static ConnectorConfig config;
    private boolean hasDelete;
    
    //Exclusions: those fields we don't want to bring through pipe sperated pair.
    private static final String[] EXCLUDES = new String[]{"Attachment|Body","EmailMessage|HtmlBody", "EmailMessage|TextBody"};
    
    private static final Integer CONNECTION_PAUSE = 10; //if the SF connection fails wait this many minutes before retrying
    private static Calendar now; //a Calendar object for date calcs
    private DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private DateFormat DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    private PartnerConnection sfConnection; //Salesforce connector
    private Connection ssConnection; //SQL Server connector
    private Statement statement;
    
    static Logger logger = new Logger(); //set up logging
    
    //Constructor
    public ObjectThread(String schema, String object, String mode, ConnectorConfig config) {
        ObjectThread.schema = schema;
        this.object = object;
        ObjectThread.mode = mode;
        //ObjectThread.config = config;
        
        this.tableName = schema +".["+object+"]";
        
        try{ //connect to SF API and SQL Server
            logger.log("Connecting to SF API", object, "ObjectThread", "Constructor", false);
            sfConnection = Connector.newConnection(config);
            sfConnection.setQueryOptions(2000); // set batch size to max
            /* https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_calls_soql_changing_batch_size.htm */

            logger.log("Connecting to SQL Server", object, "ObjectThread", null, false);
            String url ="jdbc:sqlserver://NPSQL01;databaseName=NOW_SF_DWH;integratedSecurity=true";
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            ssConnection = DriverManager.getConnection(url);
            statement = ssConnection.createStatement();
            statement.setQueryTimeout(0); //set infinite timeout

        }catch (ConnectionException e){
            logger.log("ConnectionException in ObjectThread connecting to SF:"+e.toString(), object, "ObjectThread", "Constructor", true);
        }catch (ClassNotFoundException e){
            logger.log("ClassNotFoundException in ObjectThread connecting to SQL Server:"+e.toString(), object, "ObjectThread", "Constructor", true);
        }catch (SQLException e){
            logger.log("SQLException in ObjectThread connecting to SQL Server:"+e.toString(), object, "ObjectThread", "Constructor", true);
        }
        
        now = Calendar.getInstance();

    }
    
    @Override
    public void run() {
        int objectSize = 0;
        hasDelete = false;
        
        logger.log("Thread started", object, "ObjectThread", "Run", false);
        //Control totals: how many records does SF have for this object
        logger.log("Writing control totals", object, "ObjectThread", "Run", false);
        try{
            QueryResult QR = sfConnection.query("select count() from " + object);
            objectSize = QR.getSize(); //The size of the object

            //stick it in the control table
            statement.execute("merge "+ schema +".[ctrl] as target using (select '"+ object +"', '"+ objectSize + "') as source ([Object Name], [SF Record Count]) "+
                "on (target.[Object Name] = source.[Object Name]) "+
                "when matched then update set target.[SF Record Count] = source.[SF Record Count] "+
                "when not matched then insert ([Object Name], [Last Load Date], [SF Record Count]) values (source.[Object Name], '2001-01-01 00:00:00.000', source.[SF Record Count]);");

        } catch (ConnectionException e){
            logger.log("ConnectionException in ObjectThread connecting to SF:"+e.toString(), object, "ObjectThread", "Run", true);
        } catch (SQLException e) {
            logger.log("SQLException in ObjectThread connecting to SQL Server:"+e.toString(), object, "ObjectThread", "Run", true);
        }

        //get the meta data for this object from SF
        ArrayList<ArrayList<Object>> sfObjectMetadata = getSFObjectFields(object);
        ArrayList<ArrayList<Object>> ssObjectMetadata = new ArrayList<>(); //SQL Server metadata
        try {
            ResultSet metadataRS = statement.executeQuery("select * from "+ schema +".[SF Object Metadata] where [Object Name] = '"+ object +"'");
            while (metadataRS.next()){
                //if (!((object.equals("Attachment")) && (metadataRS.getString("Field Name").equals("Body")))) { //skip the body field of attachments (They're huge!)
                if (!Arrays.asList(EXCLUDES).contains(object+"|"+metadataRS.getString("Field Name"))){ //skip if the field is on the exclusion list
                    ArrayList<Object> metaRow = new ArrayList<>();
                    metaRow.add(0, object);
                    metaRow.add(1, metadataRS.getString("Field Name"));
                    metaRow.add(2, metadataRS.getString("Field Type"));
                    metaRow.add(3, metadataRS.getString("Field Size"));
                    metaRow.add(4, metadataRS.getString("Is Custom"));
                    metaRow.add(5, metadataRS.getString("Label"));
                    metaRow.add(6, metadataRS.getString("reference To"));
                    ssObjectMetadata.add(metaRow);
                    
                    //check for [IsDeleted] field
                    if (metadataRS.getString("Field Name").equals("IsDeleted")) {
                        hasDelete = true;
                    }
                }
            }
        } catch (SQLException e) {
            logger.log("SQLException in ObjectThread connecting to SQL Server:"+e.toString(), object, "ObjectThread", "Run", true);
        }

        //now we have an object name and a list of fields, use them to create a SQL Server table
        //build a SQL string to create the table
        //while we're here build an SOQL string to get the data
        StringBuilder SQLBuilder = new StringBuilder("create table" + tableName + " (");
        StringBuilder SOQLBuilder = new StringBuilder("select ");

        if (!sfObjectMetadata.isEmpty()){ //capture null pointer if the metadata array is empty
            //loop through the field list
            for (ArrayList<Object> fieldMetadata : sfObjectMetadata){
                SQLBuilder.append("[").append(fieldMetadata.get(1)).append("] ").append(getDataType(fieldMetadata));
                SOQLBuilder.append(fieldMetadata.get(1)).append(",");
            }
            SQLBuilder.append(" index Id(Id))");
            SOQLBuilder.setLength(SOQLBuilder.length()-1);
            SOQLBuilder.append(" from ").append(object);
            if (mode.equals("DEBUG")){
                //SOQLBuilder.append(" limit 10"); //DEBUG: Limit to 10 rows for testing
            }
        } else {
            logger.log("Metadata array has no data", object, "ObjectThread", "Run", false);
        }

        /* 
         * go / no go
         * Test to make sure we can replicate the object:
         *   It must be flagged as replicatable in SF
         *   It must be less than 30 days since we last replicated
         *   The object must be the same structure as when we last replicated
         */

        boolean goodToGo = false;

        //over 30 days since replication and we'll need to rebuild
        Calendar lastUpdateDatetime = Calendar.getInstance();

        if (getReplicatable(object) && !mode.equals("REBUILD")){ //no point in going through this nonsence if we can't replicate the object or we want to rebuild them all
            //get last update date from SQL Server
            lastUpdateDatetime.setTime(new Date());
            logger.log("Reading ctrl table", object, "ObjectThread", "Run", false);
            try {
                ResultSet rs  = statement.executeQuery("select [Last Load Date] from " + schema + ".[ctrl] where [Object Name] = '" + object + "'");
                if (rs.next()){
                    lastUpdateDatetime.setTime(rs.getDate(1));

                }

            } catch (SQLException e){
                logger.log("SQLExceptio:" + e.toString(), object, "ObjectThread", "Run", true);
            }

            now.setTime(new Date());
            //use the last updated datetime and the current datetime to find changed objects
            if(daysBetween(lastUpdateDatetime, now) <= 30){
                //validate the metadata. If this isn't a match we need to rebuild from scratch
                if (!sfObjectMetadata.isEmpty()){
                    //There is some metadata, compare it
                    ArrayList<ArrayList<Object>> sfTempCompare = new ArrayList<>(sfObjectMetadata); //copy the arrays so we can remove all the matches from the other
                    ArrayList<ArrayList<Object>> ssTempCompare = new ArrayList<>(ssObjectMetadata);
                    sfTempCompare.removeAll(ssObjectMetadata); // Remove all matches from the temp arrays
                    ssTempCompare.removeAll(sfObjectMetadata);
                    goodToGo = sfTempCompare.isEmpty() && ssTempCompare.isEmpty(); //true only if both arrays are empty

                }
            }
        }
        //goodToGo = false; //DEBUG

        //Get some data from Salesforce
        if (goodToGo){
            Integer attempt = 1;
            logger.log("Replication attempt "+ attempt, object, "ObjectThread", "Run", false);
            boolean success = false;
            while (!success && attempt <=3){
                try{
                    GetUpdatedResult ur = sfConnection.getUpdated(object, lastUpdateDatetime, now); //Ask SF which objects have changed
                    GetDeletedResult dr = sfConnection.getDeleted(object, lastUpdateDatetime, now); //and any that have been deleted
                    String[] updatedIdList = ur.getIds();
                    DeletedRecord[] deletedRecords = dr.getDeletedRecords();
                    logger.log(updatedIdList.length +" records to update", object, "ObjectThread", "Run", false);
                    logger.log(deletedRecords.length +" records to delete", object, "ObjectThread", "Run", false);

                    if (updatedIdList.length > 0) {
                        logger.log("Updating", object, "ObjectThread", "Run", false);
                        SObject[] result = new SObject[updatedIdList.length];
                        //a list of fields from the metadata array
                        StringBuilder fieldList = new StringBuilder("");
                        for (ArrayList<Object> fieldRow : sfObjectMetadata){
                            fieldList.append(fieldRow.get(1)).append(",");
                        }
                        fieldList.deleteCharAt(fieldList.length() - 1); //knock the last comma off

                        //for those object that have changed, get the new data
                        //retrieve method limited to 2000, break into chunks
                        if (updatedIdList.length > 2000){
                            boolean done = false;
                            int loopMin = 0; //the start of the next run through the list (initially zero)
                            int loopMax; //the end of the next run, 2000 unless it's the last run through
                            while (!done){
                                if (updatedIdList.length - loopMin < 2000){ //less than 2000 before we get to the end
                                    loopMax = loopMin + (updatedIdList.length - loopMin);
                                    done = true; //this is the last run through the while loop
                                }else{
                                    loopMax = loopMin + 2000; //2000 or more to go, there's at least one more run through
                                }
                                String[] IDListChunk = new String[loopMax - loopMin]; //hold this chunks list of IDs
                                int loopCounter = 0;
                                for (int i = loopMin ; i < loopMax ; i++){
                                    IDListChunk[loopCounter] = updatedIdList[i];
                                    loopCounter++;
                                }

                                //copy the chunk into the result array
                                System.arraycopy(sfConnection.retrieve(fieldList.toString(), object, IDListChunk), 0, result, loopMin, loopCounter);
                                loopMin = loopMin + loopCounter; //reset for the next chunk
                            }
                        }else{ //2000 or less, no need to muck about
                            result = sfConnection.retrieve(fieldList.toString(), object, updatedIdList);
                        }

                        if (result.length > 0){
                            //build a SQL string to upsert the data for each row
                            StringBuilder SQLUpsertBuilder = new StringBuilder("merge into ");
                            SQLUpsertBuilder.append(schema).append(".[").append(object).append("] as t");
                            SQLUpsertBuilder.append(" using (select ");
                            for (String field : fieldList.toString().split(",")){
                                SQLUpsertBuilder.append("? as [").append(field).append("],");
                            }
                            SQLUpsertBuilder.setCharAt(SQLUpsertBuilder.length() -1, ")".charAt(0));
                            SQLUpsertBuilder.append(" as s on t.Id = s.Id when matched then update set ");
                            for (String field : fieldList.toString().split(",")){
                                SQLUpsertBuilder.append("t.[").append(field).append("] = s.[").append(field).append("],");
                            }
                            SQLUpsertBuilder.deleteCharAt(SQLUpsertBuilder.length()-1);
                            SQLUpsertBuilder.append(" when not matched then insert (");
                            for (String field : fieldList.toString().split(",")){
                                SQLUpsertBuilder.append("[").append(field).append("],");
                            }
                            SQLUpsertBuilder.setCharAt(SQLUpsertBuilder.length() -1, ")".charAt(0));
                            SQLUpsertBuilder.append(" values (");
                            for (String field : fieldList.toString().split(",")){
                                SQLUpsertBuilder.append("s.[").append(field).append("],");
                            }
                            SQLUpsertBuilder.setCharAt(SQLUpsertBuilder.length() -1, ")".charAt(0));
                            SQLUpsertBuilder.append(";");
                            //System.out.println(SQLUpsertBuilder.toString());
                            PreparedStatement prepStatement = ssConnection.prepareStatement(SQLUpsertBuilder.toString());
                            ssConnection.setAutoCommit(false); //turn outocommit off

                            //loop through each returned object setting prepared statement values
                            logger.log("Getting SF Data", object, "ObjectThread", "Run", false);
                            for (SObject SFobject : result){
                                Integer fieldCount = 0;
                                for (String field : fieldList.toString().split(",")){
                                    fieldCount++;
                                    //now we're looping through fields
                                    Object dataValue = SFobject.getField(field);
                                        if (dataValue !=null){
                                            //switch on the data type

                                            switch (sfObjectMetadata.get(fieldCount-1).get(2).toString()){
                                                case "boolean":
                                                    if (dataValue.toString().equals("true")){
                                                        prepStatement.setInt(fieldCount, 1);
                                                    }else{
                                                        prepStatement.setInt(fieldCount, 0);
                                                    }
                                                    break;
                                                case "int":
                                                    prepStatement.setInt(fieldCount, Integer.parseInt(dataValue.toString()));
                                                    break;
                                                case "double":
                                                    prepStatement.setDouble(fieldCount, Double.parseDouble(dataValue.toString()));
                                                    break;
                                                case "date":
                                                    try{
                                                        //parse the string to a java date then convert to a sql date. Not complicated at all!
                                                        prepStatement.setDate(fieldCount, new java.sql.Date(DATE_FORMAT.parse(dataValue.toString()).getTime()));

                                                    }catch (ParseException | SQLException e){
                                                        logger.log("Exception in querySF:"+e.toString(), object, "ObjectThread", "Run", true);
                                                    }
                                                    break;
                                                case "datetime":
                                                    try{
                                                        String dateValue = dataValue.toString().replace("T", " ");
                                                        dateValue = dateValue.substring(0, dateValue.length() - 5);
                                                        prepStatement.setDate(fieldCount, new java.sql.Date(DATETIME_FORMAT.parse(dateValue).getTime()));

                                                    }catch (ParseException | SQLException e){
                                                        logger.log("Exception in querySF:"+e.toString(), object, "ObjectThread", "Run", true);
                                                    }
                                                    break;
                                                case "base64":
                                                    if (dataValue.toString().length() > CHARLIMIT){
                                                        prepStatement.setBytes(fieldCount,Base64.getDecoder().decode(dataValue.toString().substring(0,CHARLIMIT)));
                                                    }else{
                                                        prepStatement.setBytes(fieldCount,Base64.getDecoder().decode(dataValue.toString()));
                                                    }
                                                    break;
                                                case "currency":
                                                    prepStatement.setDouble(fieldCount, Double.parseDouble(dataValue.toString()));
                                                    break;
                                                case "percent":
                                                    prepStatement.setDouble(fieldCount, Double.parseDouble(dataValue.toString()));
                                                    break;

                                                default:
                                                    if (dataValue.toString().length() > CHARLIMIT){
                                                        prepStatement.setString(fieldCount, dataValue.toString().substring(0,CHARLIMIT));
                                                    }else{
                                                        prepStatement.setString(fieldCount, dataValue.toString());
                                                    }
                                                    break;
                                            }

                                        }else{
                                            //set to null
                                            switch (SFobject.getType()){
                                                case "date":
                                                    prepStatement.setNull(fieldCount, java.sql.Types.DATE);
                                                    break;

                                                case "datetime":
                                                    prepStatement.setNull(fieldCount, java.sql.Types.DATE);
                                                    break;

                                                default:
                                                    prepStatement.setNull(fieldCount, java.sql.Types.VARCHAR);
                                                    break;
                                            }
                                        }
                                }
                                prepStatement.addBatch();
                            }

                            logger.log("Executing batch", object, "ObjectThread", "Run", false);

                            int[] batchResults = prepStatement.executeBatch();
                            ssConnection.commit(); //explicitly commit, because we turned auto commit off
                            ssConnection.setAutoCommit(true); //turn it back on

                            if (batchResults.length >0){
                                logger.log("Batch Result: " + batchResults.length, object, "ObjectThread", "Run",  false);
                                //Update control table
                                //statement.execute("update "+ schema +".[ctrl] set [Last Load Date] = '"+ datetimeFormat.format(now.getTime()) +"' where [Object Name] = '"+ object +"'");
                                statement.execute("merge "+ schema +".[ctrl] as target using (select '"+ object +"', '"+ DATETIME_FORMAT.format(now.getTime()) + "') as source ([Object Name], [Last Load Date]) "+
                                    "on (target.[Object Name] = source.[Object Name]) "+
                                    "when matched then update set target.[Last Load Date] = source.[Last Load Date] "+
                                    "when not matched then insert ([Object Name], [Last Load Date]) values (source.[Object Name], source.[Last Load Date]);");
                                //success = true;

                            } else {
                                //TODO: Deal with a failed SQL Server merge...
                            }
                            prepStatement.close();
                        }
                    }else{
                        logger.log("No updates to write", object, "ObjectThread", "Run", false);
                        success = true;
                    }

                    //deal with the deletions
                    if (deletedRecords.length > 0) {
                        logger.log("Deleting records", object, "ObjectThread", "Run",  false);
                        //mark as deleted in SS
                        String initSQL = "";
                        if (hasDelete){ // Deal with tables that don't have [IsDeleted]
                            //the table has an [IsDeleted] field, use it
                            initSQL = "Update "+ tableName + " set [IsDeleted] = 1 where Id in ('";
                        } else {
                            //it doesn't so we'll have to just delete the records
                            initSQL = "delete from "+ tableName + " where Id in ('";
                        }
                        StringBuilder deleteString = new StringBuilder(initSQL);
                        for (DeletedRecord record : deletedRecords){
                            deleteString.append(record.getId()).append("','");
                        }
                        deleteString.deleteCharAt(deleteString.length()-1).deleteCharAt(deleteString.length()-1); //knock off the last comma & quote
                        deleteString.append(");");

                        statement.execute(deleteString.toString());
                    }
                    success = true;

                }catch (SQLException | ConnectionException e){
                    logger.log("ConnectionException in ObjectThread:" + e.toString(), object, "ObjectThread", "Run", true);
                    //failed, retry
                    if (attempt <=3){
                        logger.log("Replication attempt "+ attempt +" failed, retrying", object, "ObjectThread", "Run", false);
                        attempt++;
                        try{
                            TimeUnit.MINUTES.sleep(CONNECTION_PAUSE);
                        }catch (InterruptedException ex){
                            logger.log("InterruptedException in ObjectThread delay " + ex.toString(), object, "ObjectThread", "Run", true);
                        }

                    } else {
                        logger.log("Max Replication attempts reached", object, "ObjectThread", "Run", true);
                        success = true;
                    }
                }
            }
        }else{
            //metadata validation faild, rebuild the table
            logger.log(object + " metadata mismatch, rebuilding table", object, "ObjectThread", "Run", false);
            rebuildTable(SOQLBuilder.toString(), SQLBuilder.toString(),sfObjectMetadata);
        }

        //Validate against the record total, rebuild the table if it's different
        if (objectSize > 0){ //It's possible a new object with no records has been asked for, skip if that's the case
            try{
                ResultSet recordCount = statement.executeQuery("select count(*) as [Row Count] from " + tableName);
                recordCount.next();
                Integer SSRecordTotal = recordCount.getInt("Row Count");
                recordCount.close();
                //log the totals
                logger.log("Control totals: SF - " + objectSize + " SS - " + SSRecordTotal, object, "ObjectThread", "Run", false);
                if (objectSize > SSRecordTotal && mode.equals("PRODUCTION")){
                    logger.log("Data size mismatch, rebuilding table", object, "ObjectThread", "Run", false);
                    rebuildTable(SOQLBuilder.toString(), SQLBuilder.toString(),sfObjectMetadata);
                }

            } catch (SQLException e){
                logger.log("SQLException in ObjectThread " + e.toString(), object, "ObjectThread", "Run", true);

            }
        }

        //clean up
        try {
            statement.close();
            ssConnection.close();

        } catch (SQLException e) {
            logger.log("SQLException in ObjectThread " + e.toString(), object, "ObjectThread", "Run", true);
        }

        logger.log("Thread ended for "+ object, object, "ObjectThread", "Run", false);
        //latch.countDown();

    }
    
        /**
     * Get metadata for an object
     * 
     * Skip Attachment body, far too big to replicate!
     * @param String: A valid SF object
     * @return ArrayList<ArrayList<String>> objectFields
     */
    private ArrayList<ArrayList<Object>> getSFObjectFields(String object){
        ArrayList<ArrayList<Object>> objectFields = new ArrayList<>();
        Integer attempt = 1;
        boolean success = false;
        
        while (!success && attempt <=3){
            try{
                logger.log("Reading metadata attempt " + attempt, object, "ObjectThread", "getSFObjectFields", false);
                DescribeSObjectResult describeSObjectResults = sfConnection.describeSObject(object);
                DescribeSObjectResult desObj = describeSObjectResults;
                if (desObj.isQueryable()){ //bug out if it's not queryable
                    Field[] fields = desObj.getFields(); //get the objects fields
                    for (Field field : fields) {
                        //if (!((object.equals("Attachment")) && (field.getName().equals("Body")))){ //Skip if Attachment Body
                        if (!Arrays.asList(EXCLUDES).contains(object+"|"+field.getName())){ //Skip if it's in the excludes list
                            //loop through each field in the object
                            ArrayList<Object> FieldListRow = new ArrayList<>();

                            FieldListRow.add(0, object);
                            FieldListRow.add(1, field.getName());
                            FieldListRow.add(2, field.getType().toString());
                            if (field.getByteLength() > 0){
                               FieldListRow.add(3, String.valueOf(field.getByteLength()));
                                //FieldListRow.add(3, charLimit);
                            }else if (field.getPrecision() > 0){
                                FieldListRow.add(3, String.valueOf(field.getPrecision()));
                            }else if (field.getDigits()> 0){
                                FieldListRow.add(3, String.valueOf(field.getDigits()));
                            }else if (field.getLength() > 0){
                                FieldListRow.add(3, String.valueOf(field.getLength()));
                            }else{
                                FieldListRow.add(3, "0");
                            }
                            if (field.isCustom()){
                                FieldListRow.add(4,"1");
                            }else{
                                FieldListRow.add(4,"0");
                            }
                            FieldListRow.add(5, field.getLabel());
                            if (field.getReferenceTo().length > 0){ //it's a reference to another object(s), get thier names
                                String refList = "";
                                for (String ref: field.getReferenceTo()){
                                    refList = refList + ref + ",";
                                }
                                refList = refList.substring(0, refList.length()-1); //trim the last comma
                                FieldListRow.add(6, refList); //add the completed string to the array
                            }else{
                                FieldListRow.add(6, null);
                            }

                            objectFields.add(FieldListRow); // add the row to the array
                        }
                    } //next field
                    
                    success = true;

                }else{
                    logger.log("Not a queryable object", object, "ObjectThread", "getSFObjectFields", false);
                    success = true;
                }
            }catch (ConnectionException e){
                logger.log("ConnectionException in getSFObjectFields connecting to SF: "+e.toString(), object, "ObjectThread", "getSFObjectFields", true);
                attempt++;
                if (attempt <= 3){ //have another crack at it if it failed
                    logger.log("Retrying connection", object, "ObjectThread", "getSFObjectFields", false);
                    try{
                        TimeUnit.MINUTES.sleep(CONNECTION_PAUSE);
                    }catch (InterruptedException ex){
                        logger.log("InterruptedException in delay " + ex.toString(), object, "ObjectThread", "getSFObjectFields", true);
                    }
                }else{
                    //Three failed attemptds, we should do something...
                    logger.log("Failed to get metadata after 3 attempts", object, "ObjectThread", "getSFObjectFields", true);
                    return null;
                }
            }
        }

        return objectFields;
    }
    

    /**
     * Return a string with the appropriate data type. We're using this for the create table SQL string
     * @param metaData
     * @return
     * 
     * TODO: Deal with base64 data type
     */
    private String getDataType(ArrayList<Object> metaData){
        String returnString;
        Integer  size = Integer.parseInt(metaData.get(3).toString());
        String type = metaData.get(2).toString();
        //catch field sizes of zero or over the limit
        if (size > CHARLIMIT || size == 0){
            size = CHARLIMIT;
        }
        switch (type){
            case "boolean": returnString = " integer,";
                break;
            case "int": returnString = " integer,";
                break;
            case "double": returnString = " decimal(16,4),";
                break;
            case "date": returnString = " date,";
                break;
            case "datetime": returnString = " datetime,";
                break;
            case "base64": returnString = " varbinary(max),";
                break;
            case "id": returnString = " varchar(18),";
                break;         
            case "reference": returnString = " varchar(18),";
                break;
            case "currency": returnString = " decimal(16,4),";
                break;
            case "percent": returnString = " decimal(16,4),";
                break;
            
            default: returnString = " varchar(" + size.toString() + "),";
                break;
        }
        
        return returnString;
    }
    
    /**
     * 
     * @param object
     * @return 
     */
    private boolean getReplicatable(String object){
        boolean isReplicatble = false;

        try{
            DescribeSObjectResult describeSObjectResults = sfConnection.describeSObject(object);
            logger.log("Testing for replicatable status", object, "ObjectThread", "getReplicatable", false);
            DescribeSObjectResult desObj = describeSObjectResults;
            if (desObj.isReplicateable()){
                isReplicatble = true;
            }

            return isReplicatble;
            
        } catch (ConnectionException e) {
            logger.log("ConnectionException in getReplicatable:"+e.toString(), object, "ObjectThread", "getReplicatable", true);
            return isReplicatble;
        }
  
    }
        
    /**
     * 
     * @param startDate
     * @param endDate
     * @return 
     */
    private long daysBetween(Calendar startDate, Calendar endDate) {
        Calendar date = (Calendar) startDate.clone();
        long daysBetween = 0;
        while (date.before(endDate)){
            date.add(Calendar.DAY_OF_MONTH, 1);
            daysBetween++;
        }
        return daysBetween;
    }
    
    /**
     * Rebuild the SQL Server table with the SF metadata
     * Update the SQL Server metadata table
     * @param SOQLString
     * @param fieldList
     * @return 0 = no data
     *         1 = success
     */
    private void rebuildTable(String SOQLString, String SQL, ArrayList<ArrayList<Object>> sfObjectMetadata){
        Integer attempt = 1;
        boolean success = false;
        
        String table = schema +".[" + sfObjectMetadata.get(0).get(0).toString() + "]";
        while (!success && attempt <=3){
            logger.log("Rebuilding table attempt number " + attempt, object, "ObjectThread", "rebuildTable", false);
            try{
                QueryResult queryResults = sfConnection.query(SOQLString); //get a dataset
                Boolean done = false;
                if (queryResults.getSize() > 0){
                    //now we know we have some data create a SQLServer table to put it in
                    //createTable(table, SQL, sfObjectMetadata.get(0).get(0).toString());
                    createTable(table, SQL);
                    //create a prepared statement - build a string with the appropriate number of placeholders
                    StringBuilder insertSQL = new StringBuilder("insert into " + table + " values (");
                    for (Object element : sfObjectMetadata){
                        // add a ? for each element
                        insertSQL.append("?,");
                    }
                    insertSQL.deleteCharAt(insertSQL.length()-1);
                    insertSQL.append(")"); //remove the last comma and finalise the string
                    
                    //try (PreparedStatement prepStatement = ssConnection.prepareStatement(insertSQL)){
                    now.setTime(new Date());
                    String nowDatestamp = DATETIME_FORMAT.format(now.getTime());
                    PreparedStatement prepStatement = ssConnection.prepareStatement(insertSQL.toString());
                    logger.log("Transferring " + queryResults.getSize() + " " + sfObjectMetadata.get(0).get(0).toString() + " SF records to SQL Server...", object, "ObjectThread", "rebuildTable", false);
                    
                    while (!done){ //loop through the dataset
                        SObject[] records = queryResults.getRecords();
                        for (SObject record : records){
                            //loop through the array of fields adding the data to the output array
                            for (int fieldCount = 1; fieldCount <= sfObjectMetadata.size(); fieldCount++){
                                String fieldName = sfObjectMetadata.get(fieldCount-1).get(1).toString();
                                String fieldType = sfObjectMetadata.get(fieldCount-1).get(2).toString();
                                Object dataValue = record.getField(fieldName);
                                if (dataValue != null){ //If it's null, set to null... Yes yes, I know.
                                    //update the prepared statement based on the field type
                                    switch (fieldType){ //switch on the data type
                                        case "boolean":
                                            if (dataValue.toString().equals("true")){
                                                prepStatement.setInt(fieldCount, 1);
                                            }else{
                                                prepStatement.setInt(fieldCount, 0);
                                            }
                                            break;
                                        case "int":
                                            prepStatement.setInt(fieldCount, Integer.parseInt(dataValue.toString()));
                                            break;
                                        case "double":
                                            prepStatement.setDouble(fieldCount, Double.parseDouble(dataValue.toString()));
                                            break;
                                        case "date":
                                            try{
                                                //parse the string to a java date then convert to a sql date. Not complicated at all!
                                                prepStatement.setDate(fieldCount, new java.sql.Date(DATE_FORMAT.parse(dataValue.toString()).getTime()));
                                                break;
                                            }catch (ParseException | SQLException e){
                                                logger.log("Exception:"+e.toString(), object, "ObjectThread", "rebuildTable", true);
                                            }
                                        case "datetime":
                                            try{
                                                String dateValue = dataValue.toString().replace("T", " ");
                                                dateValue = dateValue.substring(0, dateValue.length() - 5);
                                                prepStatement.setDate(fieldCount, new java.sql.Date(DATETIME_FORMAT.parse(dateValue).getTime()));
                                                break;
                                            }catch (ParseException | SQLException e){
                                                logger.log("Exception:"+e.toString(), object, "ObjectThread", "rebuildTable", true);
                                            }
                                        case "base64":
                                            if (dataValue.toString().length() > CHARLIMIT){
                                                prepStatement.setBytes(fieldCount,Base64.getDecoder().decode(dataValue.toString().substring(0,CHARLIMIT)));
                                            }else{
                                                prepStatement.setBytes(fieldCount,Base64.getDecoder().decode(dataValue.toString()));
                                            }
                                            break;
                                        case "currency":
                                            prepStatement.setDouble(fieldCount, Double.parseDouble(dataValue.toString()));
                                            break;
                                        case "percent":
                                            prepStatement.setDouble(fieldCount, Double.parseDouble(dataValue.toString()));
                                            break;

                                        default: // default to a string;
                                            if (dataValue.toString().length() > CHARLIMIT){
                                                prepStatement.setString(fieldCount, dataValue.toString().substring(0,CHARLIMIT));
                                            } else {
                                                prepStatement.setString(fieldCount, dataValue.toString());
                                            }
                                            break;
                                    }
                                }else{
                                    //no data, add null
                                    switch (fieldType){
                                        case "date":
                                            prepStatement.setNull(fieldCount, java.sql.Types.DATE);
                                            break;

                                        case "datetime":
                                            prepStatement.setNull(fieldCount, java.sql.Types.DATE);
                                            break;

                                        default: prepStatement.setNull(fieldCount, 0);
                                        break;
                                    }
                                }
                            }
                            prepStatement.addBatch();
                        }

                        if (queryResults.isDone()){ //get the next SF batch if there's more records
                            prepStatement.executeBatch(); //send the last SQL batch
                            done = true;
                            logger.log("Dataset done", object, "ObjectThread", "rebuildTable", false);
                        }else{
                            prepStatement.executeBatch(); //send the SQL batch
                            queryResults = sfConnection.queryMore(queryResults.getQueryLocator());
                            logger.log("Getting next " + sfObjectMetadata.get(0).get(0).toString() + " batch", object, "ObjectThread", "rebuildTable", false);
                        }
                    }
                    success = true;
                    //Rebulid SQL metadata table...
                    //Clear it first
                    statement.execute("delete from "+ schema +".[SF Object Metadata] where [Object Name] = '"+ sfObjectMetadata.get(0).get(0).toString() +"'");
                    //reset the prepared statement
                    insertSQL.setLength(0); //clear the string
                    insertSQL.append("insert into ").append(schema).append(".[SF Object Metadata] values (?,?,?,?,?,?,?)");
                    
                    prepStatement = ssConnection.prepareStatement(insertSQL.toString());
                    for (ArrayList<Object> row : sfObjectMetadata){
                        prepStatement.setString(1, row.get(0).toString());
                        prepStatement.setString(2, row.get(1).toString());
                        prepStatement.setString(3, row.get(2).toString());
                        prepStatement.setInt(4, Integer.parseInt(row.get(3).toString()));
                        prepStatement.setInt(5, Integer.parseInt(row.get(4).toString()));
                        prepStatement.setString(6, row.get(5).toString());
                        if (row.get(6) != null){
                            prepStatement.setString(7, row.get(6).toString());
                        }else{
                            prepStatement.setNull(7, java.sql.Types.CHAR);
                        }
                        prepStatement.addBatch();
                    }
                    prepStatement.executeBatch();
                    
                    //...and update control table
                    statement.execute("merge "+ schema +".[ctrl] as target using (select '"+ sfObjectMetadata.get(0).get(0).toString() +"', '"+ nowDatestamp + "') as source ([Object Name], [Last Load Date]) "+
                        "on (target.[Object Name] = source.[Object Name]) "+
                        "when matched then update set target.[Last Load Date] = source.[Last Load Date] "+
                        "when not matched then insert ([Object Name], [Last Load Date]) values (source.[Object Name], source.[Last Load Date]);");
                    //close the prepared statement
                    prepStatement.close();

                }else{
                    logger.log("No records, bugging out", object, "ObjectThread", "rebuildTable", false);
                    success = true;
                }

            }catch (ConnectionException | SQLException | NumberFormatException e){
                logger.log("Exception:"+ table + ":" + e.toString(), object, "ObjectThread", "rebuildTable", true);
                attempt++;
                if (attempt <= 3){ //have another crack at it if it failed
                    logger.log("Retrying connection", object, "ObjectThread", "rebuildTable", false);
                    try{
                        TimeUnit.MINUTES.sleep(CONNECTION_PAUSE);
                    }catch (InterruptedException ex){
                        logger.log("InterruptedException in delay " + ex.toString(), object, "ObjectThread", "rebuildTable", true);
                    }
                }else{
                    //Three failed attemptds, we should do something...
                    logger.log("Table rebuild failed after max attempts:" + table, object, "ObjectThread", "rebuildTable", true);
                }
            }
        }
    }
    
    /**
     * Create a SQL Server table, dropping it first if it exists
     * If it doesn't exist add a placeholder in the control table
     * @param table
     * @param SQL
     * @param object 
     * @throws SQLException 
     */
    //private void createTable(String table, String SQL, String object){
    private void createTable(String table, String SQL){
        try{
                if (tableExist(table)){
                    logger.log(table + " already exsits, dropping it first", object, "ObjectThread", "createTable", false);
                    statement.execute("drop table " + table);
                }
                
                logger.log("Creating table " + table, object, "ObjectThread", "createTable", false);
                statement.execute(SQL); //send the SQL to create the table
                
        }catch (SQLException e){
            logger.log("SQLException:"+e.toString(), object, "ObjectThread", "createTable", true);
        }
    }
    
    /**
     * Check for a table in the SQL Server database
     * @param conn
     * @param tableName
     * @return boolean tExists
     * @throws SQLException
     */
    private boolean tableExist(String tableName){
        String[] tableSplit = tableName.split("\\.");
        String schemaName = tableSplit[0].substring(1, tableSplit[0].length() - 1);
        tableName = tableSplit[1].substring(1, tableSplit[1].length() - 1);

        boolean tExists = false;
        try (ResultSet rs = ssConnection.getMetaData().getTables(null, schemaName, tableName, null)){
            while (rs.next()){ 
                String tName = rs.getString("TABLE_NAME");
                if (tName != null && tName.equals(tableName)){
                    tExists = true;
                    break;
                }
            }
        }catch (Exception e){
            logger.log("Exception in tableExist:"+e.toString(), object, "ObjectThread", "tableExist", true);
        }
    
        return tExists;
    }
}