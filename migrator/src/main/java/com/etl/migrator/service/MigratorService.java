package com.etl.migrator.service;

import com.etl.migrator.constants.Constants;
import com.etl.migrator.constants.NestedDocTransformed;
import com.etl.migrator.dto.ManyTableDTO;
import com.etl.migrator.dto.OneTableDTO;
import com.etl.migrator.dto.TableDTO;
import com.etl.migrator.dto.TwoInvertTablesDTO;
import com.etl.migrator.dto.CollectionDTO;
import com.etl.migrator.queueConfig.MessageConsumer;
import com.etl.migrator.queueConfig.MessageProducer;
import com.etl.migrator.transformer.TransformerLogic;

import lombok.extern.slf4j.Slf4j;

import org.bson.types.ObjectId;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
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

    @Value(value = "${message.topicAllTables.name}")
    private String topicAllTables;

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
    public List<String> getTablesDB(String db) {
        List<String> tables = new ArrayList<>();

        try {
            this.connection.setSchema(db);
            ResultSet columns = this.connection.getMetaData().getTables(db, null, "%", null);// .getCatalogs();
            while (columns.next()) {
                System.out.println(columns.getString("TABLE_NAME"));
                tables.add(columns.getString("TABLE_NAME"));
            }
        } catch (SQLException e) {
            log.error(e.getMessage());
        }
        return tables;
    }

    private void processingTables(String database, List<String> tables){
        tables.forEach(
                (table) -> {
                    log.info("Fetching data from TABLE :: " + table);
                    try {
                        makeCollectionOneTable(new OneTableDTO(table, database));
                    } catch (SQLException e) {
                        log.error(e.getMessage());
                    }
                });

    }

    /**
     * Make the collection, extract from tables in MySQL
     * @param tableParams
     * @return
     * @throws SQLException
     * @throws ParseException 
     */
    public String makeCollection(TableDTO tableParams) throws SQLException, ParseException {
        String fromTable, fromIdKey, toTable, foreignKey, db, migrationMode;

        if(nonNull(tableParams.getTables())){
            processingTables(tableParams.getDb(), tableParams.getTables());
        }

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
                + " LEFT JOIN " + db + "." + toTable + " ON " + fromTable + "." + fromIdKey + "=" + toTable + "." + foreignKey 
                + " ORDER BY "+ db + "." + fromTable + "." + fromIdKey + " ;";
        System.out.println(querySQL);
        ResultSet rs = this.connection.createStatement().executeQuery(querySQL);
        if("nested".equals(migrationMode))
        	return nestedDocumentLogic(rs, fromColumnsCount, fromIdKey, fromTable, toTable);
        else 
        	return referencedDocumentLogic(rs, fromColumnsCount, fromIdKey, fromTable, toTable, foreignKey);
    }

	/**
     * Make the collection, extract from tables in MySQL
     * @param oneTableParams
     * @return
     * @throws SQLException
     */

    public String makeCollectionOneTable(OneTableDTO oneTableParams) throws SQLException{
        String fromTable, db;

        Properties pojoFather;
        List<Properties> listFathers = new ArrayList<>();

        // get params
        fromTable = oneTableParams.getFromTable();
        db = oneTableParams.getDb();
        ArrayList<JSONObject> docs = new ArrayList<>();
        JSONParser parser = new JSONParser();
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

                String doc = "{";
                doc+= extractValues(pojoFather); //method to create the json structure as string to work with on transformer stage
                doc+= "}";

                try {
					docs.add((JSONObject)parser.parse(doc));
				} catch (ParseException e) {
                    log.info(e.getMessage());
				}
                //to send the response to postman
                listFathers.add(pojoFather);

                // send message to the producer
               // producer.sendMessage(topicNameOneTable, doc);
            }
            TransformerLogic trans = new TransformerLogic();
            trans.transformDataNested(new NestedDocTransformed(fromTable, docs));
            
        return listFathers.toString();
    }

    /**
     * Select type of migration
     * @param collectionDTO
     * @return
     */
    public String processMigrationTables(CollectionDTO collectionDTO) {
        StringBuilder result = new StringBuilder();
        result.append(processWithoutRelation(collectionDTO));
        return result.toString();
    }

    /**
     * Process migration - tables without relation
     * @param collectionDTO
     * @return
     */
    private String processWithoutRelation(CollectionDTO collectionDTO) {
        //MessageProducer producer = context.getBean(MessageProducer.class);
        String database = collectionDTO.getDatabase();
        StringBuilder result = new StringBuilder();

        if(!nonNull(collectionDTO.getTables())){
            collectionDTO.setTables(getTablesDB(collectionDTO.getDatabase()));
        }

        collectionDTO.getTables().forEach(
                (table) -> {
                    log.info("Fetching data from TABLE :: " + table);
                    
                    try {
						makeCollectionOneTable(new OneTableDTO(table, database));
					} catch (SQLException e) {
                        log.error(e.getMessage());
					}
                });
        return result.toString().replace("\"", "").replace("\\","");
    }

    /**
     * Process relation between tables, call Make Collection Process
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
                    } catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
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
        	if(!key.equals("children"))//because it needs to be process as a nested document.
        		cad.append("\"" + key + "\":\"" + val + "\",");
        });
		return cad.toString();
	}

    public String processManyToMany(CollectionDTO manyDTO) throws SQLException {
        //MessageProducer producer = context.getBean(MessageProducer.class);
        String db = manyDTO.getDatabase();
        String pivotTable = manyDTO.getPivotTable();
        log.info("Fetching data from TABLE :: " + pivotTable);
        //LinkedHashMap<String, Object> mapValues = new LinkedHashMap<>();

        String querySQL = "SELECT * FROM " + db + "." + pivotTable;
        for(ManyTableDTO item : manyDTO.getManyTable()){
            String table = item.getPrimaryTable();
            querySQL += " INNER JOIN " + db + "." + table + " ON " + table + "." + item.getPrimaryKey() + "=" + pivotTable + "." + item.getForeignKey() + " ";
        }
        
        ArrayList<JSONObject> docs = new ArrayList<>();
        JSONParser parser = new JSONParser();
        
        try {
            //List<String> documents = new ArrayList<>();
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
                //documents.add(doc);
                docs.add((JSONObject) parser.parse(doc.toString()));
            }
            //mapValues.put(pivotTable, documents);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        //JSONObject json = new JSONObject(mapValues);
        TransformerLogic trans = new TransformerLogic();
        trans.transformDataNested(new NestedDocTransformed(pivotTable, docs));
        log.info("Sending data to Transformer ...");
        //producer.sendMessage(topicManyTables, json.toString());

        return docs.toString().replace("\"", "").replace("\\","");
    }

    /**
     * Process many to many relationship
     * @param manyDTO
     * @return
     * @throws SQLException
     */
    public String processManyToManyDifferentDoc(CollectionDTO manyDTO) throws SQLException {
        //MessageProducer producer = context.getBean(MessageProducer.class);
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
        log.info(querySQL);
        
        ArrayList<JSONObject> docs = new ArrayList<>();
        JSONParser parser = new JSONParser();

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

                //pojoFather.put("database", db);
                //pojoFather.put("table", pivotTable);
                //pojoFather.put("children1", tableIndex1);
                //pojoFather.put("children2", tableIndex2);
                //pojoFather.put("firstPK", firstPK);
                //pojoFather.put("secondPK", secondPK);

                String doc = "{";
                doc+= extractValues(pojoFather); //method to create the json structure as string to work with on transformer stage
                doc+= "\"" + tableIndex1 + "\":" + pojoSonJson1;
                doc+= ",\"" + tableIndex2+ "\":" + pojoSonJson2 + "}";

                log.info(doc);
                //producer.sendMessage(topicManyTables, doc);
                docs.add((JSONObject) parser.parse(doc.toString()));
                result.add(doc);
            }
            log.info("DOCS :: " + docs);
           TransformerLogic trans = new TransformerLogic();
           trans.transformDataNested(new NestedDocTransformed(pivotTable, docs));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return result.toString();
    }

    /**
     * Create collection Many to Many
     * @param manyDTO
     * @return
     * @throws SQLException
     */
    public String makeCollectionManyToMany(CollectionDTO manyDTO) throws SQLException {
        Properties pojoFather;
        Map<Object, Properties> listFathers = new HashMap<Object, Properties>();

        String pivotTable = manyDTO.getPivotTable();
        String db = manyDTO.getDatabase();
        String fromTable = manyDTO.getManyTable().get(0).getPrimaryTable();
        String fromIdKey = manyDTO.getManyTable().get(0).getPrimaryKey();
        String toTable = manyDTO.getManyTable().get(1).getPrimaryTable();

        String querySQL = "";
        String temp1 = "";
        for(ManyTableDTO item : manyDTO.getManyTable()){
            String table = item.getPrimaryTable();
            temp1 +=  db + "." + table + ".* ,";
            querySQL += " INNER JOIN " + db + "." + table + " ON " + table + "." + item.getPrimaryKey() + "=" + pivotTable + "." + item.getForeignKey() + " ";
        }
        temp1 = temp1.substring(0, temp1.length() - 1);
        querySQL = "SELECT " + temp1 + " FROM " + db + "." + pivotTable + querySQL;
        log.info("QUERY :: " + querySQL);
        int fromColumnsCount = getListColumns(db, fromTable).size();

        ResultSet rs = this.connection.createStatement().executeQuery(querySQL);

        ResultSetMetaData metadata = rs.getMetaData();
        int columnCount = metadata.getColumnCount();

        TransformerLogic trans = new TransformerLogic();

        while (rs.next()) {
            pojoFather = new Properties();

            for (int i = 1; i <= fromColumnsCount; i++) {
                pojoFather.put(metadata.getColumnName(i), rs.getString(i));
            }

            Properties pojoSon = new Properties();
            for (int i = fromColumnsCount + 1; i <= columnCount; i++) {
                pojoSon.put(metadata.getColumnName(i), rs.getString(i));
            }

            String primKeyValue = pojoFather.getProperty(fromIdKey);
            boolean exists = false;
            if(listFathers.containsKey(primKeyValue)) {
                exists = true;
                ArrayList<Properties> children = (ArrayList<Properties>) listFathers.get(primKeyValue).get("children");
                children.add(pojoSon);
                listFathers.get(primKeyValue).replace("children", children);
            }

            if(!exists) {
                ArrayList<Properties> childrens = new ArrayList<Properties>();
                childrens.add(pojoSon);
                pojoFather.put("children", childrens);
                listFathers.put(primKeyValue, pojoFather);
            }
        }

        ArrayList<JSONObject> docs = new ArrayList<>();
        JSONParser parser = new JSONParser();
        System.out.println("Document processing :: start ");
        listFathers.forEach((key, value) -> {
            Properties father = value;
            StringBuilder doc = new StringBuilder("{");
            doc.append(extractValues(father)); //method to create the json structure as string to work with on transformer stage
            String pojoSonJson ="[";

            ArrayList<Properties> children = (ArrayList<Properties>) father.get("children");
            for(Properties child : children) {
                pojoSonJson += "{";
                pojoSonJson += extractValues(child);
                pojoSonJson += "},";
            }
            pojoSonJson = pojoSonJson.substring(0, pojoSonJson.length() - 1);

            doc.append("\"" + toTable +"\":" + pojoSonJson + "]}");

            try {
                docs.add((JSONObject) parser.parse(doc.toString()));
            } catch (ParseException e) {
                log.info("Error parsing JSON ::" + e.getMessage());
            }
        });
        trans.transformDataNested(new NestedDocTransformed(fromTable, docs));
        return listFathers.toString();
    }

	public String processTwoTables(TwoInvertTablesDTO tableParams) throws SQLException, ParseException{
		String mainTable, primKey, forgKey, secondTable, primaryKey, db, migrationMode;

        // get params
        mainTable = tableParams.getMainTable();
        primKey = tableParams.getPrimKey();
        forgKey = tableParams.getForgKey();
        secondTable = tableParams.getSecondTable();
        primaryKey = tableParams.getPrimaryKey();
        migrationMode = tableParams.getMigrationMode();
        db = tableParams.getDb();
        int fromColumnsCount = getListColumns(db, mainTable).size();

        // get metadata from Table in MySQL
        String querySQL = "SELECT " + /*+ fromColumnsCount +", " + fromTable + ".*, " + toTable + */"* FROM " + db + "." + mainTable
                + " LEFT JOIN " + db + "." + secondTable + " ON " + mainTable + "." + forgKey + "=" + secondTable + "." + primaryKey 
                + " ORDER BY "+ db + "." + mainTable + "." + primKey + " ;";
        System.out.println(querySQL);
        ResultSet rs = this.connection.createStatement().executeQuery(querySQL);
        
        return nestedDocumentLogic(rs, fromColumnsCount, primKey, mainTable, secondTable);

	}
	
	private String nestedDocumentLogic(ResultSet rs, int fromColumnsCount, String primKey, String mainTable, String secondTable) throws SQLException, ParseException {
		Properties pojoFather;
        Map<Object, Properties> listFathers = new HashMap<Object, Properties>();
        
        ResultSetMetaData metadata = rs.getMetaData();
        int columnCount = metadata.getColumnCount();
        
        TransformerLogic trans = new TransformerLogic();

        while (rs.next()) {
            pojoFather = new Properties();

            for (int i = 1; i <= fromColumnsCount; i++) {
                pojoFather.put(
                        nonNull(metadata.getColumnName(i)) ? metadata.getColumnName(i) : "",
                        nonNull(rs.getString(i)) ? rs.getString(i) : "");
            }

            Properties pojoSon = new Properties();
            for (int i = fromColumnsCount + 1; i <= columnCount; i++) {
                pojoSon.put(
                        nonNull(metadata.getColumnName(i)) ? metadata.getColumnName(i) : "",
                        nonNull(rs.getString(i)) ? rs.getString(i) : "");
            }
            
            String primKeyValue = pojoFather.getProperty(primKey);
            boolean exists = false;
            if(listFathers.containsKey(primKeyValue)) {
            	exists = true;
            	ArrayList<Properties> children = (ArrayList<Properties>) listFathers.get(primKeyValue).get("children");
            	children.add(pojoSon);
            	listFathers.get(primKeyValue).replace("children", children);
            		//break;
            }
            
            if(!exists) {
	            ArrayList<Properties> childrens = new ArrayList<Properties>();
	            childrens.add(pojoSon);
	            pojoFather.put("children", childrens);
	            
	            //to send the response to postman
	            listFathers.put(primKeyValue, pojoFather);
            }

        }
        
        ArrayList<JSONObject> docs = new ArrayList<>();
        JSONParser parser = new JSONParser();
        System.out.println("Document processing :: start ");
        listFathers.forEach((key, value) -> {
        	Properties father = value;
        	StringBuilder doc = new StringBuilder("{");
            doc.append(extractValues(father)); //method to create the json structure as string to work with on transformer stage
            String pojoSonJson ="[";
            
            ArrayList<Properties> children = (ArrayList<Properties>) father.get("children");
            for(Properties child : children) {
            	pojoSonJson += "{";
            	pojoSonJson += extractValues(child);
            	pojoSonJson += "},";
            }
            pojoSonJson = pojoSonJson.substring(0, pojoSonJson.length() - 1);
            
            doc.append("\"" + secondTable +"\":" + pojoSonJson + "]}");
            //System.out.println("Document :: " + doc);
            try {
				docs.add((JSONObject) parser.parse(doc.toString()));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            // send message to the producer
            //producer.sendMessage(topicName, doc);
        });
        trans.transformDataNested(new NestedDocTransformed(mainTable, docs));
        return listFathers.toString();
	}
	
	private String referencedDocumentLogic(ResultSet rs, int fromColumnsCount, String primKey, String mainTable, String secondTable, String foreignKey) throws SQLException {
		
		Map<Object, Properties> listFathers = new HashMap<Object, Properties>();
		List<Properties> listSons = new ArrayList< Properties>();
        
        ResultSetMetaData metadata = rs.getMetaData();
        int columnCount = metadata.getColumnCount();
        
        while (rs.next()) {
        	Properties pojoFather = new Properties();
        	for (int i = 1; i <= fromColumnsCount; i++) {
                pojoFather.put(metadata.getColumnName(i), rs.getString(i));
            }

            Properties pojoSon = new Properties();
            for (int i = fromColumnsCount + 1; i <= columnCount; i++) {
                pojoSon.put(metadata.getColumnName(i), rs.getString(i));
            }
            
            ObjectId id = new ObjectId();
            
            String primKeyValue = pojoFather.getProperty(primKey);
            if ( listFathers.containsKey(primKeyValue) ) {
            	id = (ObjectId) listFathers.get(primKeyValue).get("_id");
            }
            
            else {
            	pojoFather.put("_id", id);
            	listFathers.put(primKeyValue, pojoFather);
            }
            
            pojoSon.put(foreignKey, id);
            listSons.add(pojoSon);
            
        }
        ArrayList<JSONObject> docs = new ArrayList<>();
        JSONParser parser = new JSONParser();
        TransformerLogic trans = new TransformerLogic();
        listFathers.forEach((key, value) -> {
        	Properties father = value;
        	StringBuilder doc = new StringBuilder("{");
            doc.append(extractValues(father)); //method to create the json structure as string to work with on transformer stage
            doc.append("}");
            try {
				docs.add((JSONObject) parser.parse(doc.toString()));
			} catch (ParseException e) {
				e.printStackTrace();
			}
            
        });
		
        trans.transformDataNested(new NestedDocTransformed(mainTable, docs));
		
        ArrayList<JSONObject> docsSons = new ArrayList<>();
        listSons.forEach((value) -> {
        	Properties father = value;
        	StringBuilder doc = new StringBuilder("{");
            doc.append(extractValues(father)); //method to create the json structure as string to work with on transformer stage
            doc.append("}");
            try {
            	docsSons.add((JSONObject) parser.parse(doc.toString()));
			} catch (ParseException e) {
				e.printStackTrace();
			}
            
        });
		
        trans.transformDataNested(new NestedDocTransformed(secondTable, docsSons));
        
		return null;
	}
	
}
