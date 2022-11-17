package com.etl.migrator.transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
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
	
	
	private ArrayList<String> fixedTags; //to remove the tags that contains fixed values that help us with the collection name an so on
	
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
		
		JSONParser parser = new JSONParser();
		try (MongoClient mongoClient = MongoClients.create(settings)){
			
			MongoDatabase database = mongoClient.getDatabase("Migrator");
			JSONObject json = (JSONObject) parser.parse(row);			
			
	        MongoCollection<Document> collect = database.getCollection(json.get("collection").toString());
	        
	        String keyMas = json.get("masterPk").toString();
	        String valueKeyMas = json.get(keyMas).toString();
	        
	        //created nestedDoc because it will be added to the array if there is a department on the mongodb
	        Document nestedDoc = new Document();
	        
	        ((Map<String, Object>) json.get("children")).forEach((key, value) ->{
	        	if(!fixedTags.contains(key)) {
	        		nestedDoc.append(key.toString(), value);
	        	}
	        });
	        
	        Document query = new Document().append(keyMas, valueKeyMas);
	        
	        Bson updates = new Document("$push", new Document(json.get("childrenName").toString(),nestedDoc));
	        Document resultUpdate = collect.findOneAndUpdate(query,updates);
	        
	        if(resultUpdate == null) { //if there is no row into mongodb it will be created
	        	Document doc = new Document();
		        
		        json.forEach((key, value) ->{
		        	if(!fixedTags.contains(key)) {
		        		doc.append(key.toString(), value);
		        	}
		        });
		        
		        List<Document> docList = new ArrayList<>();
		        docList.add(nestedDoc); //the child is added as an arrayList to make it an array and can insert a new employee there

		        doc.append(json.get("childrenName").toString(), docList);
		        
		        InsertOneResult result = collect.insertOne(doc);
		        
		        System.out.println("Result ::: " + result.toString());//just an acknowledge that the row was inserted
				
	        }
	        
	        
		} catch (MongoException me) {
            System.err.println("Unable to insert due to an error: " + me);
        } catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
