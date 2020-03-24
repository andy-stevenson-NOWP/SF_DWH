/*
 * This is the licence header, don't really know what to put here
 * This is a custom Java app for NOW: Pensions, it's not licenced to any-one else
 * If you find yourself using it and you're not a NP employee chastise yourself serverly and delete this immidiatley
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 * @author Andy.Stevenson
 */
public class ConfigProperties {
    static String result = "";
    static InputStream inputStream;
    
    /**
     * Returns the property asked for
     * @param property
     * @return 
     */
    public String getPropertyValue(String property){

        try {
            Properties prop = new Properties();
            String propFileName = "properties.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            // get the property value
            result = prop.getProperty(property);

        } catch (Exception e) {
                System.err.println("Exception:" + e.toString());
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                System.err.println("IOException:" + e.toString());
            }
        }
        return result;
    }
}