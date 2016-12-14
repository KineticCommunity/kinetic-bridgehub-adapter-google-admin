package com.kineticdata.bridgehub.adapter.google;

import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.admin.directory.DirectoryScopes;
import com.google.api.services.admin.directory.model.*;
import com.google.api.services.admin.directory.Directory;
import java.security.GeneralSecurityException;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class GoogleAdminAdapter implements BridgeAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/
    
    /** Defines the adapter display name. */
    public static final String NAME = "Google Admin Bridge";
    
    /** Defines the logger */
    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(GoogleAdminAdapter.class);

    /** Adapter version constant. */
    public static String VERSION;
    /** Load the properties version from the version.properties file. */
    static {
        try {
            java.util.Properties properties = new java.util.Properties();
            properties.load(GoogleAdminAdapter.class.getResourceAsStream("/"+GoogleAdminAdapter.class.getName()+".version"));
            VERSION = properties.getProperty("version");
        } catch (IOException e) {
            logger.warn("Unable to load "+GoogleAdminAdapter.class.getName()+" version properties.", e);
            VERSION = "Unknown";
        }
    }
    
    /** Defines the collection of property names for the adapter. */
    public static class Properties {
        public static final String EMAIL = "Service Account Email";
        public static final String P12_FILE = "P12 File Location";
        public static final String USER_IMPERSONATION = "Impersonated User Email";
    }

    private String email;
    private String p12File;
    private String userImpersonation;
    private Directory directory;

    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap(
            new ConfigurableProperty(Properties.EMAIL).setIsRequired(true),
            new ConfigurableProperty(Properties.P12_FILE).setIsRequired(true),
            new ConfigurableProperty(Properties.USER_IMPERSONATION).setIsRequired(true)
    );
    
    /**
     * Structures that are valid to use in the bridge
     */
    public static final List<String> VALID_STRUCTURES = Arrays.asList(new String[] {
        "Users"
    });
    
    /*---------------------------------------------------------------------------------------------
     * SETUP METHODS
     *-------------------------------------------------------------------------------------------*/
    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public String getVersion() {
       return VERSION;
    }
    
    @Override
    public ConfigurablePropertyMap getProperties() {
        return properties;
    }
    
    @Override
    public void setProperties(Map<String,String> parameters) {
        properties.setValues(parameters);
    }
    
    @Override
    public void initialize() throws BridgeError {
        this.email = properties.getValue(Properties.EMAIL);
        this.p12File = properties.getValue(Properties.P12_FILE);
        this.userImpersonation = properties.getValue(Properties.USER_IMPERSONATION);
        this.directory = setBridge();
    }
    
      /*---------------------------------------------------------------------------------------------
     * CONSTRUCTOR
     *-------------------------------------------------------------------------------------------*/
    
    /**
     * Initialize a new unique GoogleDriveBridge. The constructor takes a map of
     * configuration name/value pairs.
     *
     * @param configuration
     * @throws BridgeError If it was not possible to register the adapter class.
     * added void to fix error-C
     */
    public Directory setBridge() throws BridgeError {
        
        JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
        HttpTransport httpTransport;
        GoogleCredential credential;
        try {
            httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            List<String> scopes = new ArrayList<String>();
            scopes.add(DirectoryScopes.ADMIN_DIRECTORY_USER);
            credential = new GoogleCredential.Builder()
                .setTransport(httpTransport)
                .setJsonFactory(JSON_FACTORY)
                .setServiceAccountId(email)
                .setServiceAccountPrivateKeyFromP12File(new java.io.File(p12File))
                .setServiceAccountScopes(scopes)
                .setServiceAccountUser(userImpersonation)
                .build();
            
        } catch (GeneralSecurityException gse) {
            throw new BridgeError(gse);
        } catch (IOException ioe) {
            throw new BridgeError(ioe);
        }
        
        Directory directory = new Directory.Builder(httpTransport, JSON_FACTORY, credential).build();
        return directory;
    }
    
    /*---------------------------------------------------------------------------------------------
     * IMPLEMENTATION METHODS
     *-------------------------------------------------------------------------------------------*/
    
    @Override
    public Count count(BridgeRequest request) throws BridgeError {
        if (!VALID_STRUCTURES.contains(request.getStructure())) {
            throw new BridgeError("Invalid Structure: '" + request.getStructure() + "' is not a valid structure");
        }

        GoogleAdminQualificationParser parser = new GoogleAdminQualificationParser();
        String query = parser.parse(request.getQuery(),request.getParameters());
        
        Users result;
        String domain = null;
        String fields = null;
        if(!query.isEmpty()){
            String[] keyValueArray = query.split("&");
            for(String pair : keyValueArray){
                String[] individualValue = pair.split("=");
                if(individualValue[0].trim().toLowerCase().equals("domain")){
                    domain = individualValue[1].trim();
                }
//              TODO: should we allow counts on other feild than just email.                
//              if(individualValue[0].toLowerCase().equals("fields")){
//                  fields = individualValue[1];
//              }
            }
        }
//      This is required if count on other feilds beside email is used        
//      fields = "users("+fields+")"; 
        try{
//      To count on other fields besides email .setFields must have the fields variable
            result = this.directory.users().list().setDomain(domain).setFields(fields).setMaxResults(500).execute();
        }catch(IOException ioe){
            throw new BridgeError("There was a error calling the API.",ioe);
        }
        
        int count = result.getUsers().size();
        //Return the response
        return new Count(count);
    }
    
    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {
        String structure = request.getStructure();
        if (!VALID_STRUCTURES.contains(structure)) {
            throw new BridgeError("Invalid Structure: '" + structure + "' is not a valid structure");
        }
     
        GoogleAdminQualificationParser parser = new GoogleAdminQualificationParser();
        String query = parser.parse(request.getQuery(),request.getParameters());
        List<String> fieldList = request.getFields();

        Users result;
        String domain = null;
        String googleQuery = null;
        if(!query.isEmpty()){
            String[] keyValueArray = query.split("&");
            for(String pair : keyValueArray){
                String[] individualValue = pair.split("=");
                if(individualValue[0].trim().toLowerCase().equals("domain")){
                    domain = individualValue[1].trim();
                }              
                if(individualValue[0].trim().toLowerCase().equals("email") || individualValue[0].trim().toLowerCase().equals("name")){
                    googleQuery = individualValue[1];
                }
            }
        }
        
        //The API will error if fields is empty when the call is made
        String fields;
        if(fieldList == null || fieldList.isEmpty()){
            throw new BridgeError("A field value is required.");
        }else{
            fields = "users("+request.getFieldString()+")";  
        }
        
        try{
            result = this.directory.users().list().setDomain(domain).setQuery(googleQuery).setFields(fields).setMaxResults(500).execute();
        }catch(IOException ioe){
            throw new BridgeError("There was a error calling the API.",ioe);
        }
        
        int count = result.getUsers().size();
        Map<String,Object> record = new LinkedHashMap();

        if (count > 1) {
            throw new BridgeError("Multiple results matched an expected single match query");
        } else if (count == 1 && fieldList != null) {
            for (String field : fieldList) {
                Object value = result.getUsers().get(0).get(field);
                record.put(field,value);
            }
        }

        return new Record(record);
    }

    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {
        // Initialize the result data and response variables
        Map<String,Object> data = new LinkedHashMap();

        String structure = request.getStructure();
        if (!VALID_STRUCTURES.contains(structure)) {
            throw new BridgeError("Invalid Structure: '" + structure + "' is not a valid structure");
        }

        GoogleAdminQualificationParser parser = new GoogleAdminQualificationParser();
        String query = parser.parse(request.getQuery(),request.getParameters());
        List<String> fieldList = request.getFields();
        
        // Parse through the response and create the record lists
        ArrayList<Record> records = new ArrayList<Record>();
        
        Users results;
        String domain = null;
        String googleQuery = null;
        if(!query.isEmpty()){
            String[] keyValueArray = query.split("&");
            for(String pair : keyValueArray){
                String[] individualValue = pair.split("=");
                if(individualValue[0].trim().toLowerCase().equals("domain")){
                    domain = individualValue[1].trim();
                }              
                if(individualValue[0].trim().toLowerCase().equals("email") || individualValue[0].trim().toLowerCase().equals("name")){
                    googleQuery = individualValue[1];
                }
            }
        }
        
        //The API will error if fields is empty when the call is made
        String fields;
        if(fieldList == null || fieldList.isEmpty()){
            throw new BridgeError("A field value is required.");
        }else{
            fields = "users("+request.getFieldString()+")";  
        }
        
        try{
            results = this.directory.users().list().setDomain(domain).setQuery(googleQuery).setFields(fields).setMaxResults(500).execute();
        }catch(IOException ioe){
            throw new BridgeError("There was a error calling the API.",ioe);
        }
        
        int count = results.getUsers().size();
        for(int index=0;index<count;index++){
            Map<String,Object> record = new LinkedHashMap();
            for (String field : fieldList) {
                Object value = results.getUsers().get(index).get(field);
                record.put(field,value);
            }
            records.add(new Record(record));
        }
        
        // Return the response
        return new RecordList(request.getFields(), records);
    }
}
