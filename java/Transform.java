package lambda;



import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Event;
import saaf.Inspector;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class Transform implements RequestHandler<Request, HashMap<String, Object>> {
    private static final String INPUT_BUCKET_NAME = "tcss562-data-set";
    private static final String OUTPUT_BUCKET_NAME = "tcss562termproject";
    private static final String OUTPUT_FILE_NAME = "output.csv";
    /**
     * Lambda Function Handler
     *
     * @param request
     * @param context
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        //****************START FUNCTION IMPLEMENTATION*************************
        String s3Bucket = INPUT_BUCKET_NAME;
        String s3Key = "data.csv";
        S3Client s3Client = S3Client.create();
        ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(GetObjectRequest.builder()
                .bucket(s3Bucket)
                .key(s3Key)
                .build());


        // Load CSV data from S3

            BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object));
            // Process CSV data
            String processedData = processAndTransformCsv(reader);

            // Write processed data to S3
            writeToS3( s3Client, processedData, OUTPUT_BUCKET_NAME, OUTPUT_FILE_NAME);
            s3Client.close();

        //Add custom key/value attribute to SAAF's output. (OPTIONAL)
        //inspector.addAttribute("message", "Hello " + request.get("name")
         //       + "! This is a custom attribute added as output from SAAF!");
        
        //Create and populate a separate response object for function output. (OPTIONAL)
        //Response response = new Response();
        //response.setValue("Hello " + request.get("name")
        //        + "! This is from a response object!");
        
        //inspector.consumeResponse(response);
        
        //****************END FUNCTION IMPLEMENTATION***************************
                
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
    private void writeToS3(S3Client s3Client, String data, String outputBucket, String outputKey) {
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(outputBucket)
                .key(outputKey)
                .build(), RequestBody.fromString(data));
    }

    private String processAndTransformCsv(BufferedReader reader) {
        // TODO: Implement CSV processing and transformation logic here
        // Read header
        String header = null;
        try {
            header = reader.readLine();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error reading CSV header: " + e.getMessage());
        }

        // Map column indices
        Map<String, Integer> columnIndexMap = new HashMap<>();
        String[] columns = header.split(",");
        for (int i = 0; i < columns.length; i++) {
            columnIndexMap.put(columns[i], i);
        }

        // Process CSV data
        StringBuilder processedData = new StringBuilder();
        processedData.append(header).append(",Order Processing Time,Gross Margin").append("\n");

        Set<String> processedOrderIds = new HashSet<>();

        reader.lines().forEach(line -> {
            String[] values = line.split(",");

            // Operation 1: Add column [Order Processing Time]
            String orderDateStr = values[columnIndexMap.get("Order Date")];
            String shipDateStr = values[columnIndexMap.get("Ship Date")];
            int orderProcessingTime = 0;
            try {
                orderProcessingTime = calculateOrderProcessingTime(orderDateStr, shipDateStr);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            values = Arrays.copyOf(values, values.length + 1);
            values[values.length - 1] = String.valueOf(orderProcessingTime);

            // Operation 2: Transform [Order Priority] column
            String orderPriority = values[columnIndexMap.get("Order Priority")];
            values[columnIndexMap.get("Order Priority")] = transformOrderPriority(orderPriority);

            // Operation 3: Add a [Gross Margin] column
            double totalProfit = Double.parseDouble(values[columnIndexMap.get("Total Profit")]);
            double totalRevenue = Double.parseDouble(values[columnIndexMap.get("Total Revenue")]);
            double grossMargin = totalProfit / totalRevenue;
            values = Arrays.copyOf(values, values.length + 1);
            values[values.length - 1] = String.valueOf(grossMargin);

            // Operation 4: Remove duplicate data identified by [Order ID]
            String orderId = values[columnIndexMap.get("Order ID")];
            if (!processedOrderIds.contains(orderId)) {
                processedOrderIds.add(orderId);
                processedData.append(String.join(",", values)).append("\n");
            }
        });


        return processedData.toString();
    }
    private int calculateOrderProcessingTime(String orderDateStr, String shipDateStr) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
        Date orderDate = dateFormat.parse(orderDateStr);
        Date shipDate = dateFormat.parse(shipDateStr);
        long diffInMillies = Math.abs(shipDate.getTime() - orderDate.getTime());
        return (int) (diffInMillies / (24 * 60 * 60 * 1000));
    }

    private String transformOrderPriority(String orderPriority) {
        switch (orderPriority) {
            case "L":
                return "Low";
            case "M":
                return "Medium";
            case "H":
                return "High";
            case "C":
                return "Critical";
            default:
                return orderPriority;
        }
    }

}
