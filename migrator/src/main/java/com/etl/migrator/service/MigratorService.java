package com.etl.migrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonAppend;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;

@Service
public class MigratorService {
    private Connection connection;
    private DatabaseMetaData databaseMetaData;
    private String database_name = "migration";
    private Statement databaseStatement;

    public MigratorService() throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:mysql://localhost:3306","root", "developer345");
        this.databaseMetaData = connection.getMetaData();
        this.databaseStatement = connection.createStatement();
    }

    public String processMigration(){
        List<String> tables = getTables(database_name);//getTables("blog");
        return "something";
    }

    public List<String> getTables(String database){
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
        try(ResultSet columns = databaseMetaData.getColumns(database_name,null, tableName, null)){
            while(columns.next()) {
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

    public String getCollection(String fromTable, String fromIdKey, String toTable , String foreignKey) throws SQLException{
        List<String> columns = getListColumns(fromTable);
        ResultSet rs = databaseStatement.executeQuery("SELECT COUNT(*) FROM " + database_name + "." + fromTable+";");
        // get the number of rows from the result set
        rs.next();
        int rowCount = rs.getInt(1);
        int pivot = 0;
        //Query to retrieve records
        String query = "SELECT * FROM " + database_name + "." + fromTable+";";
        System.out.println(query);
        //Executing the query
        ResultSet rsc = databaseStatement.executeQuery(query);

        try{
            System.out.println("Results:");
            while(rsc.next()){
                System.out.println("{");
                for (String column : columns) {
                    System.out.println(column + ":" + rsc.getString(column));
                }
                System.out.println(toTable + ": \n{" );
                addDocument(toTable,foreignKey,rsc.getInt(fromIdKey));
                System.out.println("}");
            }
      } finally {
           return "get collection success";
      }
    }

    public void addDocument(String tableName, String foreign_key, int valKey) throws SQLException {
        List<String> columns = getListColumns(tableName);
        String query = "SELECT * FROM " + database_name + "." + tableName;
        query += " WHERE " +foreign_key + "=" +valKey+";";
        System.out.println(query);
        //Executing the query
        ResultSet rs = databaseStatement.executeQuery(query);
        try {
            System.out.println("Results:");
            while (rs.next()) {
                System.out.println("{");
                for (String column : columns) {
                    if(column != foreign_key) {
                        System.out.println(rs.getString(column));
                    }
                }
                System.out.println("}");
            }
        } finally {
            System.out.println("END");
        }
    }

    public List<String> getListColumns(String tableName) {
        List<String> Columns = new ArrayList<>();
        try(ResultSet columns = databaseMetaData.getColumns(database_name,null, tableName, null)){
            while(columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                Columns.add(columnName);
            }
        } catch (SQLException e) {
        	System.out.println(e.getMessage());
        }
        return Columns;
    }

    public String makeCollection(String fromTable, String fromIdKey, String toTable , String foreignKey) throws  SQLException{
        int fromColumnsCount = getListColumns(fromTable).size();
        System.out.println(fromColumnsCount);


        ResultSet rs = databaseStatement.executeQuery("SELECT "+ /*+ fromColumnsCount +", " + fromTable + ".*, " + toTable + */"* FROM " + database_name + "." + fromTable
                +" INNER JOIN "+ database_name + "."+ toTable+" ON "+ fromTable + "." + fromIdKey + "=" + toTable + "." + foreignKey + " ORDER BY "+ fromIdKey+";");
        //System.out.println(rs);
        ResultSetMetaData metadata = rs.getMetaData();
        int columnCount = metadata.getColumnCount();
        System.out.println();
        String pivot = "";
        Properties pojoFather = new Properties();
        List<Properties> childrens = new ArrayList<Properties>();
        while (rs.next()) {
            String firstColumn = rs.getString(1).toString();
            //if(pivot.toString() != firstColumn.toString()) {
                //pivot = firstColumn;
                //System.out.println(pivot);
                pojoFather = new Properties();
                //childrens.clear();
                //pojoFather.put("MasterTableCols", fromColumnsCount);
                for (int i = 1; i <= fromColumnsCount; i++) {
                    pojoFather.put(metadata.getColumnName(i), rs.getString(i));
                }

                Properties pojoSon = new Properties();
                for (int i = fromColumnsCount+1; i <= columnCount; i++) {
                    //row += metadata.getColumnName(i) + "=" + rs.getString(i) + ", ";
                    pojoSon.put(metadata.getColumnName(i), rs.getString(i));
                }
                childrens.add(pojoSon);
                pojoFather.put("children", pojoSon);
           // }
            System.out.println("Row: " + pojoFather.toString());
            //pojoFather.put("childrens",childrens);
            //System.out.println(pojoFather.toString());
            /*for (Properties p: childrens
                 ) {
                System.out.println("child");
                System.out.println(p);
            }*/
        }
        return "inner collection success";
    }
}
