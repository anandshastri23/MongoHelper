package com.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;

public class AssignmentCRUD {

    public static void assignment() {
        MongoClient client = new MongoClient();
        MongoDatabase db = client.getDatabase("students");
        MongoCollection<Document> coll = db.getCollection("grades");

        MongoCursor<Document> iterable = coll.find(new Document("type", "homework")).sort(new Document("student_id", 1).append("score", 1)).iterator();
        Document prevDoc = new Document();
        while (iterable.hasNext()) {
            Document currDoc = iterable.next();
            if(currDoc.get("student_id").equals(prevDoc.get("student_id"))) {

            } else {
                coll.deleteOne(new Document("_id",currDoc.get("_id")));
            }
            prevDoc = currDoc;
        }

        ArrayList<Document> list1 = coll.find(new Document("type", "homework")).sort(new Document("student_id", 1).append("score", 1)).into(new ArrayList<>());
        list1.forEach((item1) -> Helper.printJson(item1));


    }
}
