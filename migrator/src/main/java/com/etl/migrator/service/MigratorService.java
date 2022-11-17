package com.etl.migrator.service;

import com.etl.migrator.dto.TableDTO;
import com.etl.migrator.queueConfig.MessageConsumer;
import com.etl.migrator.queueConfig.MessageProducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Configuration
@Service
public class MigratorService {
    private Connection connection;
    private DatabaseMetaData databaseMetaData;
    private String database_name = "migrator";
    private Statement databaseStatement;

    @Autowired
    private ApplicationContext context;


    @Autowired
    public MigratorService(@Value("${spring.datasource.url:default}") String datasourceURL,
                           @Value("${spring.datasource.username:default}") String datasourceUserName,
                           @Value("${spring.datasource.password:default}") String datasourcePassword
    ) throws SQLException {
        this.connection = DriverManager.getConnection(datasourceURL,datasourceUserName,datasourcePassword);
        this.databaseMetaData = connection.getMetaData();
        this.databaseStatement = connection.createStatement();
    }

    public String processMigration() {
        List<String> tables = getTables(database_name);//getTables("blog");
        return "something";
    }

    public List<String> getTables(String database) {
        List<String> tables = new ArrayList<>();

        try (ResultSet resultSet =
                     databaseMetaData.getTables(database, null, null, new java.lang.String[]{"TABLE"})) {
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                tables.add(tableName);
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }

        System.out.println("TABLES: ");
        for (java.lang.String tableNam :
                tables) {
            System.out.println("\nCOLUMNS for " + tableNam + ": [");
            getColumns(tableNam);
            System.out.println("]\n");
        }
        return tables;
    }

    public void getColumns(String tableName) {
        List<String> Columns = new ArrayList<>();
        try (ResultSet columns = databaseMetaData.getColumns(database_name, null, tableName, null)) {
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                Columns.add(columnName);
//                String columnSize = columns.getString("COLUMN_SIZE");
//                String datatype = columns.getString("DATA_TYPE");
//                String isNullable = columns.getString("IS_NULLABLE");
//                String isAutoIncrement = columns.getString("IS_AUTOINCREMENT");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        for (String column :
                Columns) {
            System.out.println(column);
        }
    }

    public String getCollection(String fromTable, String fromIdKey, String toTable, String foreignKey) throws SQLException {
        List<String> columns = getListColumns(fromTable);
        ResultSet rs = databaseStatement.executeQuery("SELECT COUNT(*) FROM " + database_name + "." + fromTable + ";");
        // get the number of rows from the result set
        rs.next();
        int rowCount = rs.getInt(1);
        int pivot = 0;
        //Query to retrieve records
        String query = "SELECT * FROM " + database_name + "." + fromTable + ";";
        System.out.println(query);
        //Executing the query
        ResultSet rsc = databaseStatement.executeQuery(query);

        try {
            System.out.println("Results:");
            while (rsc.next()) {
                System.out.println("{");
                for (String column : columns) {
                    System.out.println(column + ":" + rsc.getString(column));
                }
                System.out.println(toTable + ": \n{");
                addDocument(toTable, foreignKey, rsc.getInt(fromIdKey));
                System.out.println("}");
            }
        } finally {
            return "get collection success";
        }
    }

    public void addDocument(String tableName, String foreign_key, int valKey) throws SQLException {
        List<String> columns = getListColumns(tableName);
        String query = "SELECT * FROM " + database_name + "." + tableName;
        query += " WHERE " + foreign_key + "=" + valKey + ";";
        System.out.println(query);
        //Executing the query
        ResultSet rs = databaseStatement.executeQuery(query);
        try {
            System.out.println("Results:");
            while (rs.next()) {
                System.out.println("{");
                for (String column : columns) {
                    if (column != foreign_key) {
                        System.out.println(rs.getString(column));
                    }
                }
                System.out.println("}");
            }
        } finally {
            System.out.println("END");
        }
    }

    public List<String> getListColumns(String db, String tableName) {
        List<String> Columns = new ArrayList<>();
        try (ResultSet columns = databaseMetaData.getColumns(db, null, tableName, null)) {
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                Columns.add(columnName);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return Columns;
    }
    
    public List<String> getListColumns(String tableName) {
        List<String> Columns = new ArrayList<>();
        try (ResultSet columns = databaseMetaData.getColumns(database_name, null, tableName, null)) {
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                Columns.add(columnName);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return Columns;
    }

    public String makeCollection(TableDTO tableParams) throws SQLException {
    	MessageProducer producer = context.getBean(MessageProducer.class);
    	MessageConsumer listener = context.getBean(MessageConsumer.class);
        
        //Sending a Hello World message to topic 'topic1' - defined in app properties. 
    	Properties pojoFather;
        //List<Properties> childrens = new ArrayList<>();
        List<Properties> listFathers = new ArrayList<>();
        String fromTable, fromIdKey, toTable, foreignKey, db;

        // get params
        fromTable = tableParams.getFromTable();
        fromIdKey = tableParams.getFromIdKey();
        toTable = tableParams.getToTable();
        foreignKey = tableParams.getForeignKey();
        db = tableParams.getDb();
        int fromColumnsCount = getListColumns(db,fromTable).size();

        ResultSet rs = databaseStatement.executeQuery("SELECT " + /*+ fromColumnsCount +", " + fromTable + ".*, " + toTable + */"* FROM " + db + "." + fromTable
                + " INNER JOIN " + db + "." + toTable + " ON " + fromTable + "." + fromIdKey + "=" + toTable + "." + foreignKey + " ORDER BY " + fromIdKey + ";");
        //System.out.println(rs);
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
            //childrens.add(pojoSon);
            String pojoSonJson = "{";
            pojoSonJson += extractValues(pojoSon);
            pojoSonJson = pojoSonJson.substring(0,pojoSonJson.length()-1) + "}";
            
            //fixed tags to manage the connection, collection and nested Doc
            pojoFather.put("collection", db);
            pojoFather.put("childrenName", toTable);
            pojoFather.put("masterPk", fromIdKey);
            
            String doc = "{";
            doc+= extractValues(pojoFather); //method to create the json structure as string to work with on transformer stage
            doc+= "\"children\":" + pojoSonJson +" }";
            
            listFathers.add(pojoFather); //to sent the response to postman
            
            producer.sendMessage(doc);
            
        }
        try {
			listener.latch.await(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //System.out.println("List Fathers:" + listFathers);
        return listFathers.toString();
    }

	private String extractValues(Properties pojoFather) {
		StringBuilder cad = new StringBuilder("");
		pojoFather.forEach((key, val) -> {
        	cad.append("\"" + key + "\":\"" + val + "\",");
        });
		return cad.toString();
	}
}
