package com.etl.migrator.service;

import com.etl.migrator.constants.Constants;
import com.etl.migrator.dto.TableDTO;
import com.etl.migrator.queueConfig.MessageConsumer;
import com.etl.migrator.queueConfig.MessageProducer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Configuration
@Service
@Slf4j
public class MigratorService {
    private Connection connection;

    //@Autowired
    private ApplicationContext context;
    
    @Value(value = "${message.topic.name}")
    private String topicName;
    
    @Value(value = "${message.topicOneTable.name}")
    private String topicNameOneTable;

    @Autowired
    public MigratorService(ApplicationContext context) throws SQLException {
        this.connection = DriverManager.getConnection(Constants.DATASOURCE_URL, Constants.USERNAME, Constants.PASSWORD);
        this.context = context;
    }

    /**
     *
     * @param db
     * @param tableName
     * @return
     */
    public List<String> getListColumns(String db, String tableName) {
        List<String> Columns = new ArrayList<>();
        try (ResultSet columns = this.connection.getMetaData().getColumns(db, null, tableName, null)) {
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                Columns.add(columnName);
            }
        } catch (SQLException e) {
            log.error(e.getMessage());
        }
        return Columns;
    }

    /**
     * Make the collection, extract from tables in MySQL
     * @param tableParams
     * @return
     * @throws SQLException
     */
    public String makeCollection(TableDTO tableParams) throws SQLException {
        String fromTable, fromIdKey, toTable, foreignKey, db, migrationMode;

        MessageProducer producer = context.getBean(MessageProducer.class);
        MessageConsumer listener = context.getBean(MessageConsumer.class);

        //Sending a Hello World message to topic 'topic1' - defined in app properties. 
        Properties pojoFather;
        List<Properties> listFathers = new ArrayList<>();

        // get params
        fromTable = tableParams.getFromTable();
        fromIdKey = tableParams.getFromIdKey();
        toTable = tableParams.getToTable();
        foreignKey = tableParams.getForeignKey();
        migrationMode = tableParams.getMigrationMode();
        db = tableParams.getDb();
        int fromColumnsCount = getListColumns(db, fromTable).size();

        // get metadata from Table in MySQL
        String querySQL = "SELECT " + /*+ fromColumnsCount +", " + fromTable + ".*, " + toTable + */"* FROM " + db + "." + fromTable
                + " INNER JOIN " + db + "." + toTable + " ON " + fromTable + "." + fromIdKey + "=" + toTable + "." + foreignKey + " ORDER BY " + fromIdKey + ";";
        System.out.println(querySQL);
        ResultSet rs = this.connection.createStatement().executeQuery(querySQL);

        ResultSetMetaData metadata = rs.getMetaData();
        int columnCount = metadata.getColumnCount();

        while (rs.next()) {
            pojoFather = new Properties();

            for (int i = 1; i <= fromColumnsCount; i++) {
                pojoFather.put(metadata.getColumnName(i), rs.getString(i));
            }

            Properties pojoSon = new Properties();
            for (int i = fromColumnsCount + 1; i <= columnCount; i++) {
                pojoSon.put(metadata.getColumnName(i), rs.getString(i));
            }

            String pojoSonJson = "{";
            pojoSonJson += extractValues(pojoSon);
            pojoSonJson = pojoSonJson.substring(0, pojoSonJson.length() - 1) + "}";

            //fixed tags to manage the connection, collection and nested Doc
            pojoFather.put("collection", db);
            pojoFather.put("childrenName", toTable);
            pojoFather.put("masterPk", fromIdKey);
            pojoFather.put("migrationMode", migrationMode);
            pojoFather.put("masterTable", fromTable);
            pojoFather.put("nestedPk", foreignKey);

            String doc = "{";
            doc+= extractValues(pojoFather); //method to create the json structure as string to work with on transformer stage
            doc+= "\"children\":" + pojoSonJson + " }";

            //to send the response to postman
            listFathers.add(pojoFather);

            // send message to the producer
            producer.sendMessage(topicName, doc);
            
        }
        try {
            listener.latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return listFathers.toString();
    }

    /**
     * Extract values from the pojoFather and creates a new String
     * @param pojoFather
     * @return
     */
    public String extractValues(Properties pojoFather) {
        StringBuilder cad = new StringBuilder("");
        pojoFather.forEach((key, val) -> {
            cad.append("\"" + key + "\":\"" + val + "\",");
        });
		return cad.toString();
	}
}
