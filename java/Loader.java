package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import saaf.Inspector;
import saaf.Response;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.sql.Statement;
/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class Loader implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {
    private static final String DB_HOST = "database-1-instance-1.cs9vo722wli4.us-east-2.rds.amazonaws.com";
    private static final String DB_USER = "admin";
    private static final String DB_PASSWORD = "12345678";
    private static final String DB_NAME = "TEST";
    private static final String TABLE_NAME ="`test`";
    /**
     * Lambda Function Handler
     * 
     * @param request Hashmap containing request JSON attributes.
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(HashMap<String, Object> request, Context context) {
        String s3Bucket = "tcss562termproject";
        String s3Key = "output.csv"; // The CSV file key in the S3 bucket
        //Collect initial data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        //****************START FUNCTION IMPLEMENTATION*************************
        
        //Add custom key/value attribute to SAAF's output. (OPTIONAL)
        //inspector.addAttribute("message", "Hello " + request.get("name")
        //        + "! This is a custom attribute added as output from SAAF!");
        
        //Create and populate a separate response object for function output. (OPTIONAL)
        //Response response = new Response();
        //response.setValue("Hello " + request.get("name")
        //        + "! This is from a response object!");
        
        //inspector.consumeResponse(response);
        //Read CSV file from S3
        S3Client s3Client = S3Client.create();
        ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                .bucket(s3Bucket)
                .key(s3Key)
                .build());
        InputStream objectData = s3Object;
        String jdbcUrl = "jdbc:mysql://" + DB_HOST + ":3306/?rewriteBatchedStatements=true"; // Connect to the 'mysql' database for creating the actual database
        // Connect to DB
        try (Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, DB_PASSWORD)) {
            // Create a new database
            try (Statement statement = connection.createStatement()) {
                String createDatabaseQuery = "CREATE DATABASE IF NOT EXISTS " + DB_NAME;
                statement.executeUpdate(createDatabaseQuery);
            }
            // Switch to the new database
            jdbcUrl = "jdbc:mysql://" + DB_HOST + ":3306/" + DB_NAME ;
            connection.setCatalog(DB_NAME);
            // clean the table
            String clearTableSQL = "DROP TABLE IF EXISTS test";
            PreparedStatement preparedStatement = connection.prepareStatement(clearTableSQL);
            preparedStatement.executeUpdate();
            // create the table
            String createTableSQL = "CREATE TABLE test (`Region` text, `Country` text, `Item Type` text, `Sales Channel` text, `Order Priority` text, `Order Date` date, `Order ID` integer PRIMARY KEY, `Ship Date` date, `Units Sold` integer, `Unit Price` float, `Unit Cost` float, `Total Revenue` float, `Total Cost` float, `Total Profit` float, `Order Processing Time` integer, `Gross Margin` float)";
            preparedStatement = connection.prepareStatement(createTableSQL);
            preparedStatement.executeUpdate();
            // Read CSV data from S3
            BufferedReader reader = new BufferedReader(new InputStreamReader(objectData, StandardCharsets.UTF_8));
            String headerLine = reader.readLine(); // Read the header line
            // Prepare INSERT statement
            String insertDataSQL = "INSERT INTO test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            connection.setAutoCommit(false);
            try (PreparedStatement preparedStatement1 = connection.prepareStatement(insertDataSQL)) {
                // Read and insert data row by row
                String line;
                while ((line = reader.readLine()) != null) {
                    // Split the CSV line into values (assuming CSV format)
                    String[] values = line.split(",");
                    SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
                    Date orderDate = dateFormat.parse(values[5]);
                    Date shipDate = dateFormat.parse(values[7]);
                    java.sql.Date sqlorderDate = new java.sql.Date(orderDate.getTime());
                    java.sql.Date sqlshipDate  = new java.sql.Date(shipDate.getTime());
                    // Set values in the PreparedStatement
                    preparedStatement1.setString(1, values[0]);  // `Region` (text)
                    preparedStatement1.setString(2, values[1]);  // `Country` (text)
                    preparedStatement1.setString(3, values[2]);  // `Item Type` (text)
                    preparedStatement1.setString(4, values[3]);  // `Sales Channel` (text)
                    preparedStatement1.setString(5, values[4]);  // `Order Priority` (text)
                    preparedStatement1.setDate(6, sqlorderDate);  // `Order Date` (date)
                    preparedStatement1.setInt(7, Integer.parseInt(values[6]));  // `Order ID` (integer - PRIMARY KEY)
                    preparedStatement1.setDate(8, sqlshipDate);  // `Ship Date` (date)
                    preparedStatement1.setInt(9, Integer.parseInt(values[8]));  // `Units Sold` (integer)
                    preparedStatement1.setFloat(10, Float.parseFloat(values[9]));  // `Unit Price` (float)
                    preparedStatement1.setFloat(11, Float.parseFloat(values[10]));  // `Unit Cost` (float)
                    preparedStatement1.setFloat(12, Float.parseFloat(values[11]));  // `Total Revenue` (float)
                    preparedStatement1.setFloat(13, Float.parseFloat(values[12]));  // `Total Cost` (float)
                    preparedStatement1.setFloat(14, Float.parseFloat(values[13]));  // `Total Profit` (float)
                    preparedStatement1.setInt(15, Integer.parseInt(values[14]));  // `Order Processing Time` (integer)
                    preparedStatement1.setFloat(16, Float.parseFloat(values[15]));  // `Gross Margin` (float)

                    // Execute the SQL statement for each line
                    preparedStatement1.addBatch();
                }
                preparedStatement1.executeBatch();
                connection.commit();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

        } catch (SQLException | IOException e) {
            // Handle SQL exceptions
            e.printStackTrace();
        }

        //****************END FUNCTION IMPLEMENTATION***************************
                
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
