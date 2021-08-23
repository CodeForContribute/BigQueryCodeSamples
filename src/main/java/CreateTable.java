import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

public class CreateTable {

    public static void main(String[] args) {
        // TODO(developer): Replace these variables before running the sample.
        String datasetName = "MY_DATASET_NAME";
        String tableName = "MY_TABLE_NAME2";
        Schema schema =
                Schema.of(
                        Field.of("stringField", StandardSQLTypeName.STRING),
                        Field.of("booleanField", StandardSQLTypeName.BOOL));
//        createTable(datasetName, tableName, schema);
        createTableWithoutSchema(datasetName,tableName);
    }

    public static void createTable(String datasetName, String tableName, Schema schema) {
        try {
            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            TableId tableId = TableId.of(datasetName, tableName);
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

            bigquery.create(tableInfo);
            System.out.println("Table created successfully");
        } catch (BigQueryException e) {
            System.out.println("Table was not created. \n" + e.toString());
        }
    }

    //sample to create a table without schema
    public static void createTableWithoutSchema(String dataSetName,String tableName){
        //initialise client that will be used to sed requests. This client only needs to be
        //create once, and can be reused for multiple requests
        try {
            BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
            TableId tableId = TableId.of(dataSetName,tableName);
            TableDefinition tableDefinition = StandardTableDefinition.of(Schema.of());
            TableInfo tableInfo = TableInfo.newBuilder(tableId,tableDefinition).build();
            bigQuery.create(tableInfo);
            System.out.println("Table created Successfully");

        }catch (BigQueryException e){
            System.out.println("Table was not created.\n" + e.toString());
        }

    }

}