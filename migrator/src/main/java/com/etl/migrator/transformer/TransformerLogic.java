package com.etl.migrator.transformer;

import java.util.*;

import com.etl.migrator.constants.Constants;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.mongodb.client.result.InsertManyResult;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
import org.springframework.beans.factory.annotation.Value;

@Slf4j
public class TransformerLogic {

	private ArrayList<String> fixedTags; //to remove the tags that contains fixed values that help us with the collection name an so on

	private ConnectionString connectionString;
	private MongoClientSettings settings;

	public TransformerLogic() {
		fixedTags = new ArrayList<String>();
		fixedTags.add("collection");
		fixedTags.add("childrenName");
		fixedTags.add("masterPk");
		fixedTags.add("children");
		fixedTags.add("masterTable");
		fixedTags.add("migrationMode");
		fixedTags.add("nestedPk");

		connectionString = new ConnectionString(Constants.DATASOURCE_MONGODB);
		settings = MongoClientSettings.builder()
				.applyConnectionString(connectionString)
				.serverApi(ServerApi.builder()
						.version(ServerApiVersion.V1)
						.build())
				.build();
	}

	/**
	 * Send each row to MongoDB
	 * @param row
	 */
	public void transformData (String row) {
		JSONParser parser = new JSONParser();

		try (MongoClient mongoClient = MongoClients.create(settings)){
			
			JSONObject json = (JSONObject) parser.parse(row);
			String collectionName = json.get("collection").toString();
			
			MongoDatabase database = mongoClient.getDatabase("Migrator");

	        
	        String keyMas = json.get("masterPk").toString();
	        String valueKeyMas = json.get(keyMas).toString();
	        String migrationMode = json.get("migrationMode").toString();
	        String childrenTable = json.get("childrenName").toString();
	        String masterTable = json.get("masterTable").toString();

	        //created nestedDoc because it will be added to the array if there is a department on the mongodb
	        Document nestedDoc = new Document();
	        
	        ((Map<String, Object>) json.get("children")).forEach((key, value) ->{
	        	if(!fixedTags.contains(key)) {
	        		nestedDoc.append(key.toString(), value);
	        	}
	        });
	        
	        Document doc = new Document();

	        json.forEach((key, value) ->{
	        	if(!fixedTags.contains(key)) {
	        		doc.append(key.toString(), value);
	        	}
	        });

	        Document query = new Document().append(keyMas, valueKeyMas);

	        //validation migration mode
	        switch(migrationMode) {
		        case "referenced":
		        	MongoCollection<Document> collect1 = database.getCollection(masterTable);
		        	MongoCollection<Document> collect2 = database.getCollection(childrenTable);

		        	FindIterable<Document> docsColl1 = collect1.find(query);

		        	ObjectId id = new ObjectId();
		        	String foreignKey = json.get("nestedPk").toString();

		        	MongoCursor<Document> cursor = null;
		        	cursor = docsColl1.cursor();

		        	if(!cursor.hasNext()) {
		        		doc.append("_id", id);
		        		InsertOneResult results = collect1.insertOne(doc);
		        		System.out.println("Result ::: " + results.toString());//just an acknowledge that the row was inserted
		        	} else {
		        		Document doct = cursor.next();
		        		id = doct.getObjectId("_id");
		        	}

		        	nestedDoc.put(foreignKey, id);
		        	InsertOneResult results = collect2.insertOne(nestedDoc);
		        	System.out.println("Result ::: " + results.toString());//just an acknowledge that the row was inserted

		        	break;
		        default:
		        	MongoCollection<Document> collect = database.getCollection(masterTable);
		        	Bson updates = new Document("$push", new Document(json.get("childrenName").toString(),nestedDoc));
		 	        Document resultUpdate = collect.findOneAndUpdate(query,updates);

		 	        if(resultUpdate == null) { //if there is no row into mongodb it will be created

		 		        List<Document> docList = new ArrayList<>();
		 		        docList.add(nestedDoc); //the child is added as an arrayList to make it an array and can insert a new employee there

		 		        doc.append(childrenTable, docList);

		 		        InsertOneResult result = collect.insertOne(doc);

		 		        System.out.println("Result ::: " + result.toString());//just an acknowledge that the row was inserted

		 	        }
		 	        else {
		 	        	System.out.println("Result ::: " + resultUpdate.toString());
		 	        }
	        }
		} catch (MongoException me) {
            log.error("Unable to insert due to an error: " + me);
        } catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			log.error(e.getMessage());
		}
		
	}

	public void transformDataOne(String row) {
		JSONParser parser = new JSONParser();

		try (MongoClient mongoClient = MongoClients.create(settings)){

			JSONObject json = (JSONObject) parser.parse(row);
			String collectionName = json.get("collection").toString();

			MongoDatabase database = mongoClient.getDatabase("Migrator");

			String masterTable = json.get("masterTable").toString();

			Document doc = new Document();

			json.forEach((key, value) ->{
				if(!fixedTags.contains(key)) {
					doc.append(key.toString(), value);
				}
			});

			MongoCollection<Document> collect1 = database.getCollection(masterTable);
			InsertOneResult result = collect1.insertOne(doc);
			System.out.println("Result ::: " + result.toString());
		} catch (MongoException me) {
			log.error("Unable to insert due to an error: " + me);
		} catch (ParseException e) {
			log.error(e.getMessage());
		}
	}


	public void transformDataManyTables (String row) {
		JSONParser parser = new JSONParser();

		try (MongoClient mongoClient = MongoClients.create(settings)){

			JSONObject json = (JSONObject) parser.parse(row);
			MongoDatabase database = mongoClient.getDatabase("Migrator");

			String masterTable = json.get("table").toString();
			String children1 = json.get("children1").toString();
			String children2 = json.get("children2").toString();

			//created nestedDoc because it will be added to the array if there is a department on the mongodb
			Document nestedDoc = new Document();

			((Map<String, Object>) json.get(children1)).forEach((key, value) ->{
				if(!fixedTags.contains(key)) {
					nestedDoc.append(key.toString(), value);
				}
			});

			Document nestedDoc2 = new Document();
			((Map<String, Object>) json.get(children2)).forEach((key, value) ->{
				if(!fixedTags.contains(key)) {
					nestedDoc2.append(key.toString(), value);
				}
			});

			// Validar si existe el registro
			Document doc = new Document();

			MongoCollection<Document> collect = database.getCollection(masterTable);
			List<Document> docList = new ArrayList<>();
			List<Document> docList2 = new ArrayList<>();
			docList.add(nestedDoc);
			docList2.add(nestedDoc2);
			doc.append(children1, docList);
			doc.append(children2, docList2);

			InsertOneResult result = collect.insertOne(doc);
			System.out.println("Result ::: " + result);

		} catch (MongoException me) {
			log.error("Unable to insert due to an error: " + me);
		} catch (ParseException e) {
			log.error(e.getMessage());
		}

	}

	/**Inserta un array de objetos **/
//	public void transformDataManyTables(String data){
//		JSONParser parser = new JSONParser();
//
//		try(MongoClient mongoClient = MongoClients.create(settings)){
//			JSONObject json = (JSONObject) parser.parse(data);
//			Set<String> keys = json.keySet();
//			MongoDatabase database = mongoClient.getDatabase("Migrator"); // create database
//
//			for (String keyItem : keys) {
//				List<Document> listDocuments = new ArrayList<>();
//				JSONArray jsonArray = (JSONArray)json.get(keyItem);
//
//				jsonArray.forEach((value) ->{
//					ObjectId id = new ObjectId();
//					Document document = Document.parse((String) value);
//					document.append("_id", id);
//					listDocuments.add(document);
//				});
//
//				MongoCollection<Document> collection = database.getCollection(keyItem);
//
//				InsertManyResult results = collection.insertMany(listDocuments);
//				log.info("Inserted documents: " + results.getInsertedIds());
//			}
//		} catch (ParseException e) {
//			log.error(e.getMessage() + e.getCause());
//		}catch (MongoException e) {
//			log.error("Unable to insert due to an error: " + e);
//		}
//	}

//    public void transformManyToManyTables(String data)  {
//
//		try (MongoClient mongoClient = MongoClients.create(settings)){
//			JSONParser parser = new JSONParser();
//			JSONObject json = (JSONObject) parser.parse(data);
//			log.info(json.toString());
//			MongoDatabase database = mongoClient.getDatabase("Migrator"); // create database
//
//			json.forEach((key, value) -> {
//				// log.info("KEY :: "+ key.toString() + " VALUE :: "+ value.toString());
//				JSONArray jsonArray = (JSONArray) json.get(key);
//
//				Document document = new Document();
//
//
//				List<Document> documentList = new ArrayList<>();
//				jsonArray.forEach((valueArray) -> {
//					//JSONObject jsonObject = (JSONObject) valueArray;
//
//					//Document document = new Document();
//					document.append("_id", new ObjectId());
//					document.append("example", valueArray);
//
//					documentList.add(document);
//				});
//				//log.info("LIST :: " + documentList.toString());
//				MongoCollection<Document> collection = database.getCollection(key.toString());
//				InsertManyResult results = collection.insertMany(documentList);
//				log.info("Inserted documents: " + results.getInsertedIds());
//			});
//		} catch (ParseException | MongoException e) {
//			log.error("Unable to insert due to an error: " + e);
//		}
//    }

	public void transformManyToManyTables(String row){
		JSONParser parser = new JSONParser();
		try (MongoClient mongoClient = MongoClients.create(settings)){
			JSONObject json = (JSONObject) parser.parse(row);
			log.info(json.toString());
			List<Document> listDocuments = new ArrayList<>();
			MongoDatabase database = mongoClient.getDatabase("Migrator"); // create database


			json.keySet().forEach(item ->{
				JSONArray jsonArray = (JSONArray)json.get(item);
				jsonArray.forEach(value -> {
					Document newDoc = Document.parse(value.toString());
					//JSONArray jsonArray1 = json.get(value);
					newDoc.append("_id", new ObjectId());
					listDocuments.add(newDoc);
				});
				log.info(listDocuments.toString());
				MongoCollection<Document> collection = database.getCollection(item.toString());
				InsertManyResult results = collection.insertMany(listDocuments);
				log.info("Inserted documents: " + results.getInsertedIds());
			});
		}catch (Exception e){
			log.error("Unable to insert: " + e);
		}
	}
}
