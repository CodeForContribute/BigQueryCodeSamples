import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

public class runGetTable {
    public static void main(String[] args) {
        //projectquickstart-323507.MY_DATASET_NAME.MY_TABLE_NAME2
        String projectId = "projectquickstart-323507";
        String dataSetName = "MY_DATASET_NAME";
        String tableName = "MY_TABLE_NAME2";
        getTableInfo(projectId,dataSetName,tableName);
    }

    public static void getTableInfo(String projectId,String dataSetName,String tableName){
        try{
            //initialize big query client
            BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
            TableId tableId = TableId.of(projectId,dataSetName,tableName);
            Table table = bigQuery.getTable(tableId);
            System.out.println("Table info:" + table.getDescription());
        }catch (BigQueryException e){
            System.out.println(" Table info  not  retrieved.\n"+e.toString());
        }
    }
}
