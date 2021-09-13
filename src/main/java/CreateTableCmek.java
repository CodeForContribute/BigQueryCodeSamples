import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

public class CreateTableCmek {
    public static void main(String[] args) {
        String dataSetName = "MY_DATASET_NAME";
        String tableName = "dataTable";
        String kmsKeyName = "MyKeyName";
        Schema schema = Schema.of(
                Field.of("StringField",StandardSQLTypeName.STRING),
                Field.of("booleanField",StandardSQLTypeName.BOOL)
        );

        EncryptionConfiguration encryption = EncryptionConfiguration.newBuilder().setKmsKeyName(kmsKeyName).build();
        createTable(dataSetName,tableName,schema,encryption);
    }

    public static void createTable(String dataSetName,String tableName,Schema schema,EncryptionConfiguration encryption){
        try {
            //initialize client
            BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
            TableId tableId = TableId.of(dataSetName,tableName);
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId,tableDefinition).
                                    setEncryptionConfiguration(encryption).
                                    build();
            bigQuery.create(tableInfo);
            System.out.println("Table CMEK create Successfully !!");

        }catch (BigQueryException e){
            System.out.println("Table CMEK was not created.\n" + e.toString());
        }
    }
}
