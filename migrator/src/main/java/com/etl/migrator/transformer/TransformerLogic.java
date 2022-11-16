package com.etl.migrator.transformer;

import java.util.ArrayList;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;

public class TransformerLogic {
	
	
	private ArrayList<String> fixedTags;
	
	public TransformerLogic() {
		fixedTags = new ArrayList<String>();
		fixedTags.add("collection");
		fixedTags.add("childrenName");
		fixedTags.add("masterPk");
		fixedTags.add("children");
	}
	
	public void transformData (String row) {
		
		ConnectionString connectionString = new ConnectionString("mongodb+srv://testMongo:8CyWCCkRaK7QyB9B@learningreact.6j0uwgk.mongodb.net/?retryWrites=true&w=majority");
		MongoClientSettings settings = MongoClientSettings.builder()
		        .applyConnectionString(connectionString)
		        .serverApi(ServerApi.builder()
		            .version(ServerApiVersion.V1)
		            .build())
		        .build();
		//String uri = "mongodb+srv://testMongo:8CyWCCkRaK7QyB9B@learningreact.6j0uwgk.mongodb.net/?retryWrites=true&w=majority";
		
		JSONParser parser = new JSONParser();
		try (MongoClient mongoClient = MongoClients.create(settings)){
			//Object obj = parser.parse(row);
			MongoDatabase database = mongoClient.getDatabase("Migrator");
			JSONObject json = (JSONObject) parser.parse(row);
			System.out.println(json.get("masterPk"));
			
			
	        MongoCollection<Document> collection = database.getCollection(json.get("collection").toString());
	        Document doc = new Document();
	        
	        json.forEach((key, value) ->{
	        	if(!fixedTags.contains(key)) {
	        		doc.append(key.toString(), value);
	        	}
	        });
	        
	        doc.append(json.get("childrenName").toString(), json.get("children"));
	        
	        InsertOneResult result = collection.insertOne(doc);
			
			
			
			//buscar en mongodb si masterpk existe, si si agregar a childrenName el object de ChildrenName del json presente si no existe,
			//se agrega el registro completo sin masterpk, childrenName, collection
		} catch (MongoException me) {
            System.err.println("Unable to insert due to an error: " + me);
        } catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		try {
			
			parser.parse(row);
			//System.out.println(parse);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		System.out.println("Please transform this ::: " + row);
	}
}
