package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import saaf.Inspector;
import saaf.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;



/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class Query implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {
    private static final String DB_HOST = "database-1-instance-1.cs9vo722wli4.us-east-2.rds.amazonaws.com";
    private static final String DB_USER = "admin";
    private static final String DB_PASSWORD = "12345678";
    private static final String DB_NAME = "TEST";
    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(HashMap<String, Object> request, Context context) {

        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        //****************START FUNCTION IMPLEMENTATION*************************
        //connect to my aurora
        String jdbcUrl = "jdbc:mysql://" + DB_HOST + ":3306/" + DB_NAME;
        ResultSet resultSet;
        ResultSetMetaData metaData = null;
        int columnCount;
        try (Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, DB_PASSWORD)) {

            // Prepare  statement
            String sqlQuery = generateSQLQuery(request);

            try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
                resultSet = preparedStatement.executeQuery();
                metaData = resultSet.getMetaData();
                columnCount = metaData.getColumnCount();
                List<String> columnNames = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    try {
                        columnNames.add(metaData.getColumnName(i));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                List<HashMap<String, Object>> resultJsonList = new ArrayList<>();
                while (true) {
                    try {
                        if (!resultSet.next()) break;
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    HashMap<String, Object> rowJson = new HashMap<>();

                    // Dynamically add columns based on metadata
                    for (String columnName : columnNames) {
                        try {
                            rowJson.put(columnName, resultSet.getObject(columnName));
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    // Add the row JSON object to the list
                    resultJsonList.add(rowJson);
                    ObjectMapper objectMapper = new ObjectMapper();
                    String jsonArrayString;
                    try {
                        jsonArrayString = objectMapper.writeValueAsString(resultJsonList);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    Response response = new Response();
                    response.setValue(jsonArrayString);
                    inspector.consumeResponse(response);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Process the result set and build a list of JSON objects
;
        //****************END FUNCTION IMPLEMENTATION***************************

        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
    private static String generateSQLQuery(HashMap<String, Object> jsonRequest ) throws Exception {
        // Parse JSON request
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.valueToTree(jsonRequest);

        // Extract filter and aggregations information
        JsonNode filters = jsonNode.path("filters");
        JsonNode aggregations = jsonNode.path("aggregations");

        // Construct SQL query
        StringBuilder query = new StringBuilder();
        query.append("SELECT ");
        for (JsonNode groupBy : aggregations.path("groupBy")) {
            query.append(groupBy.asText()).append(", ");
        }
        for (JsonNode operation : aggregations.path("operations")) {
            query.append(operation.asText()).append(", ");
        }
        query.delete(query.length() - 2, query.length()); // Remove trailing comma
        query.append(" FROM test ");

        // Add WHERE clause for filters
        if (filters.size() > 0) {
            query.append("WHERE ");
            for (JsonNode filter : filters) {
                Iterator<Map.Entry<String, JsonNode>> filterFields = filter.fields();
                while (filterFields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = filterFields.next();
                    String column = entry.getKey();
                    String value = entry.getValue().asText();
                    query.append(column).append(" = '").append(value).append("' AND ");
                    // Process column and value
                }
            }
            query.delete(query.length() - 5, query.length()); // Remove trailing " AND "
        }

        // Add GROUP BY clause
        query.append(" GROUP BY ");
        for (JsonNode groupBy : aggregations.path("groupBy")) {
            query.append(groupBy.asText()).append(", ");
        }
        query.delete(query.length() - 2, query.length()); // Remove trailing comma

        return query.toString();
    }

}
