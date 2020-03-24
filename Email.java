/*
 * This is the licence header, don't really know what to put here
 * This is a custom Java app for NOW: Pensions, it's not licenced to any-one else
 * If you find yourself using it and you're not a NP employee chastise yourself serverly and delete this immidiatley
 */

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

/**
 *
 * @author andy.stevenson
 * 
 * All the mailboxes must be license enabled Exchange Online mailbox, and cannot be shared mailboxes.
 *      This isn't ideal, as it's set to my individual mailbox...
 * The SMTP set to port 587
 * Transport Layer Security (TLS) encryption enabled in the relay software
 * The mailbox server name must be correct
 */
public class Email{
    //org.apache.commons.mail.Email email = new SimpleEmail();
    
    Logger logger = new Logger();

    /**
     * Send an email
     * @param to
     * @param subject
     * @param message
     */
    public void sendMail(String to, String subject, String message){
        org.apache.commons.mail.Email email = new SimpleEmail();

        ConfigProperties properties = new ConfigProperties();
        //String EMAILUSER = properties.getPropertyValue("EMAILUSERNAME");
        String EMAILUSER = properties.getPropertyValue("USERNAME");
        String EMAILPASSWORD = properties.getPropertyValue("EMAILPASSWORD");

        email.setHostName("smtp.office365.com");
        email.setSmtpPort(587); //587
        email.setAuthenticator(new DefaultAuthenticator(EMAILUSER, EMAILPASSWORD));
        email.setStartTLSEnabled(true);

        try{
            email.setFrom("nowsql@nowpensions.com");
            email.setSubject(subject);
            //email.setDebug(true);
            email.setMsg(message);
            email.addTo(to);
            email.addTo("Subbulakshmi.Avudyappan@nowpensions.com");
            email.send();
            
        }catch (EmailException ex){
            //System.err.println("EmailException in rebuildTable:"+ex.toString());
            logger.log("EmailException in Email:"+ex.toString(), null, "Email", "sendMail", true);
        }
    }
}
