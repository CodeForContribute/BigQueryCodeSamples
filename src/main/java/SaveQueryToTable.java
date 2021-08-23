import com.google.cloud.bigquery.*;

public class SaveQueryToTable {
    public static void main(String[] args) {
        String query = "SELECT corpus FROM `bigquery-public-data.samples.shakespeare` Group By corpus";
        String destinationTable = "MY_TABLE_NAME2";
        String destinationDataSet = "MY_DATASET_NAME";
        saveQueryToTable(destinationDataSet, destinationTable, query);
    }

    public static void saveQueryToTable(String destinationDataSet, String destinationTableId, String query) {
        try {
            //initialize bigquery client
            BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
            // identify the destination table
            TableId destinationTable = TableId.of(destinationDataSet, destinationTableId);
            //build the query job
            QueryJobConfiguration queryJobConfiguration =
                    QueryJobConfiguration.newBuilder(query).setDestinationTable(destinationTable).build();
            //execute the query
            bigQuery.query(queryJobConfiguration);

            //the results are now save in the destination table
            System.out.println("Saved query ran successfully");

        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Saved Query did not run.\n" + e.toString());
        }
    }
}
