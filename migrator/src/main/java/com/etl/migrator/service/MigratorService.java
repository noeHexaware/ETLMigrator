package com.etl.migrator.service;

import com.etl.migrator.constants.Constants;
import com.etl.migrator.dto.ManyTableDTO;
import com.etl.migrator.dto.OneTableDTO;
import com.etl.migrator.dto.TableDTO;
import com.etl.migrator.dto.CollectionDTO;
import com.etl.migrator.queueConfig.MessageConsumer;
import com.etl.migrator.queueConfig.MessageProducer;

import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.nonNull;

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

    @Value(value = "${message.topicManyTables.name}")
    private String topicManyTables;

    @Value(value = "${message.topicManyToManyTables.name}")
    private String topicManyToManyTables;

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
            doc+= "\"children\":" + pojoSonJson + "}";

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
     * Make the collection, extract from tables in MySQL
     * @param oneTableParams
     * @return
     * @throws SQLException
     */

    public String makeCollectionOneTable(OneTableDTO oneTableParams) throws SQLException{
        String fromTable, db;

        MessageProducer producer = context.getBean(MessageProducer.class);
        MessageConsumer listener = context.getBean(MessageConsumer.class);

        //Sending a Hello World message to topic 'topic1' - defined in app properties.
        Properties pojoFather;
        List<Properties> listFathers = new ArrayList<>();

        // get params
        fromTable = oneTableParams.getFromTable();
        db = oneTableParams.getDb();
        int fromColumnsCount = getListColumns(db, fromTable).size();

            String querySQL = "SELECT * FROM " + db + "." + fromTable + ";";
            ResultSet rs = this.connection.createStatement().executeQuery(querySQL);
            ResultSetMetaData metadata = rs.getMetaData();

            while (rs.next()) {
                pojoFather = new Properties();

                for (int i = 1; i <= fromColumnsCount; i++) {
                    pojoFather.put(
                            nonNull(metadata.getColumnName(i)) ? metadata.getColumnName(i) : "",
                            nonNull(rs.getString(i)) ? rs.getString(i) : "");
                }


                //fixed tags to manage the connection, collection and nested Doc
                pojoFather.put("collection", db);
                pojoFather.put("masterTable", fromTable);

                String doc = "{";
                doc+= extractValues(pojoFather); //method to create the json structure as string to work with on transformer stage
                doc+= "}";


                //to send the response to postman
                listFathers.add(pojoFather);

                // send message to the producer
                producer.sendMessage(topicNameOneTable, doc);
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
     * Select type of migration
     * @param collectionDTO
     * @return
     */
    public String processMigrationTables(CollectionDTO collectionDTO) {
        StringBuilder result = new StringBuilder();
        if(nonNull(collectionDTO.getTables())){
            result.append(processWithoutRelation(collectionDTO));
        }
        if(nonNull(collectionDTO.getRelational())){
            result.append(",").append(processRelationManyTables(collectionDTO));
        }
        return result.toString();
    }

    /**
     * Process migration - tables without relation
     * @param collectionDTO
     * @return
     */
    private String processWithoutRelation(CollectionDTO collectionDTO) {
        MessageProducer producer = context.getBean(MessageProducer.class);
        String database = collectionDTO.getDatabase();
        StringBuilder result = new StringBuilder();

        collectionDTO.getTables().forEach(
                (table) -> {
                    log.info("Fetching data from TABLE :: " + table);
                    LinkedHashMap<String, Object> mapValues = new LinkedHashMap<>();
                    List<String> documents = new ArrayList<>();
                    String querySQL = "SELECT * FROM " + database + "." + table;
                    int fromColumnsCount = getListColumns(database, table).size();

                    try {
                        ResultSet rs = this.connection.createStatement().executeQuery(querySQL);
                        ResultSetMetaData metadata = rs.getMetaData();
                        Properties pojoFather = new Properties();

                        while(rs.next()){
                            for (int i = 1; i <= fromColumnsCount; i++) {
                                pojoFather.put(
                                        nonNull(metadata.getColumnName(i)) ? metadata.getColumnName(i) : "",
                                        nonNull(rs.getString(i)) ? rs.getString(i) : "");
                            }
                            String doc = "{" + extractValues(pojoFather)  + "}";
                            documents.add(doc);
                        }
                        mapValues.put(table, documents);

                    } catch (Exception e) {
                        log.error("Error fetching data from Database :: " + e.getMessage() + e.getCause());
                    }
                    if(mapValues.size() > 0){
                        JSONObject json = new JSONObject(mapValues);
                        log.info("Sending data to Producer ...");
                        result.append(mapValues);
                        producer.sendMessage(topicManyTables, json.toString());
                    }
                });
        return result.toString().replace("\"", "").replace("\\","");
    }

    /**
     * Process relation between tables
     * @param collectionDTO
     * @return
     */
    private String processRelationManyTables(CollectionDTO collectionDTO) {
        StringBuilder result = new StringBuilder();
        collectionDTO.getRelational().forEach(
                (item) -> {
                    item.setMigrationMode(nonNull(item.getMigrationMode()) ? item.getMigrationMode() : "referenced");
                    item.setDb(collectionDTO.getDatabase());
                    try {
                        result.append(makeCollection(item));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
        return result.toString();
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

    public String processManyToMany(CollectionDTO manyDTO) throws SQLException {
        MessageProducer producer = context.getBean(MessageProducer.class);
        String db = manyDTO.getDatabase();
        String pivotTable = manyDTO.getPivotTable();
        log.info("Fetching data from TABLE :: " + pivotTable);
        LinkedHashMap<String, Object> mapValues = new LinkedHashMap<>();

        String querySQL = "SELECT * FROM " + db + "." + pivotTable;
        for(ManyTableDTO item : manyDTO.getManyTable()){
            String table = item.getPrimaryTable();
            querySQL += " INNER JOIN " + db + "." + table + " ON " + table + "." + item.getPrimaryKey() + "=" + pivotTable + "." + item.getForeignKey() + " ";
        }

        try {
            List<String> documents = new ArrayList<>();
            ResultSet rs = this.connection.createStatement().executeQuery(querySQL);
            ResultSetMetaData metadata = rs.getMetaData();
            Properties pojoFather = new Properties();

            while(rs.next()) {
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    pojoFather.put(
                            nonNull(metadata.getColumnName(i)) ? metadata.getColumnName(i) : "",
                            nonNull(rs.getString(i)) ? rs.getString(i) : "");
                }
                String doc = "{" + extractValues(pojoFather) + "}";
                documents.add(doc);
            }
            mapValues.put(pivotTable, documents);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        JSONObject json = new JSONObject(mapValues);
        log.info("Sending data to Producer ...");
        producer.sendMessage(topicManyTables, json.toString());

        return json.toString().replace("\"", "").replace("\\","");
    }

    /**
     * Process many to many relationship
     * @param manyDTO
     * @return
     * @throws SQLException
     */
    public String processManyToManyDifferentDoc(CollectionDTO manyDTO) throws SQLException {
        MessageProducer producer = context.getBean(MessageProducer.class);
        String db = manyDTO.getDatabase();
        String pivotTable = manyDTO.getPivotTable();
        List<String> result = new ArrayList<>();
        Properties pojoFather;

        String querySQL = "";
        String temp1 = "";
        for(ManyTableDTO item : manyDTO.getManyTable()){
            String table = item.getPrimaryTable();
            temp1 +=  db + "." + table + ".* ,";
            querySQL += " INNER JOIN " + db + "." + table + " ON " + table + "." + item.getPrimaryKey() + "=" + pivotTable + "." + item.getForeignKey() + " ";
        }
        temp1 = temp1.substring(0, temp1.length() - 1);
        querySQL = "SELECT " + temp1 + " FROM " + db + "." + pivotTable + querySQL;

        try {
            pojoFather = new Properties();
            ResultSet rs = this.connection.createStatement().executeQuery(querySQL);
            ResultSetMetaData metadata = rs.getMetaData();
            int columnCount = rs.getMetaData().getColumnCount(); // todo el RS

            while(rs.next()) {
                // first table
                Properties pojoSon1 = new Properties();
                String tableIndex1 = manyDTO.getManyTable().get(0).getPrimaryTable();
                String firstPK = manyDTO.getManyTable().get(0).getPrimaryKey();
                int columnCount1 =  getListColumns(db, tableIndex1).size();

                for (int i = 1; i <= columnCount1; i++) {
                    pojoSon1.put(
                            nonNull(metadata.getColumnName(i)) ? metadata.getColumnName(i) : "",
                            nonNull(rs.getString(i)) ? rs.getString(i) : "");
                }
                String pojoSonJson1 =  extractValues(pojoSon1);
                pojoSonJson1 = "{" + pojoSonJson1.substring(0, pojoSonJson1.length() -1) + "}";

                // second table
                Properties pojoSon2 = new Properties();
                String tableIndex2 = manyDTO.getManyTable().get(1).getPrimaryTable();
                String secondPK = manyDTO.getManyTable().get(1).getPrimaryKey();
                for (int i = columnCount1 + 1; i <= columnCount; i++) {  /// columnCount --> RS size
                    pojoSon2.put(
                            nonNull(metadata.getColumnName(i)) ? metadata.getColumnName(i) : "",
                            nonNull(rs.getString(i)) ? rs.getString(i) : "");
                }
                String pojoSonJson2 =  extractValues(pojoSon2);
                pojoSonJson2 = "{" + pojoSonJson2.substring(0, pojoSonJson2.length() -1) + "}";

                pojoFather.put("database", db);
                pojoFather.put("table", pivotTable);
                pojoFather.put("children1", tableIndex1);
                pojoFather.put("children2", tableIndex2);
                pojoFather.put("firstPK", firstPK);
                pojoFather.put("secondPK", secondPK);

                String doc = "{";
                doc+= extractValues(pojoFather); //method to create the json structure as string to work with on transformer stage
                doc+= "\"" + tableIndex1 + "\":" + pojoSonJson1;
                doc+= ",\"" + tableIndex2+ "\":" + pojoSonJson2 + "}";

                log.info(doc);
                producer.sendMessage(topicManyTables, doc);
                result.add(doc);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result.toString();
    }
}
