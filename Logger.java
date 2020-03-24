/*
 * This is the licence header, don't really know what to put here
 * This is a custom Java app for NOW: Pensions, it's not licenced to any-one else
 * If you find yourself using it and you're not a NP employee chastise yourself serverly and delete this immidiatley
 */

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 *
 * @author Andy.Stevenson
 * 
 * TODO: Create an error log table
 *       Method, object, error message
 */
public class Logger {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    
    String emailMessage = "";
    
    /**
     * Add the date to a message and output to the console
     * @param message 
     * @param object 
     * @param Class 
     * @param method 
     * @param isError 
     */
    public void log(String message, String object, String Class, String method, boolean isError){
        Date date = new Date();
        String theDate = dateFormat.format(date);
        
        if (isError){
            System.err.println(theDate + ": " + object + ":" + Class + ":"+ method + ":" + message);
            this.addToEmail(theDate + ": " + object + ":" + Class + ":"+ method + ":" + message);
            
        } else {
            System.out.println(theDate + ": " + object + ":" + Class + ":"+ method + ":" + message);
            
        }
    }
    
    /**
     * Returns the message
     * @return 
     */
    public String getEmailMessage(){
        return emailMessage;
    }
    
    /**
     * Concatenate the message string ready to send
     * @param message 
     */
    private void addToEmail(String message){
        //if the message is blank, add to it
        if (emailMessage.isEmpty()){
            emailMessage = message;
            
        } else {
            //there's already something there, add a line break them the message
            emailMessage = emailMessage + "\n\n" + message;
            
        }
    }
}