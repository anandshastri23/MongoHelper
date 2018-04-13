package com.mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

public class Assignment3 {

    public static void assigment3() {
        MongoClient client = new MongoClient();
        MongoDatabase db = client.getDatabase("school");
        MongoCollection<Document> coll = db.getCollection("students");

        List<Document> result = coll.find(new Document()).into(new ArrayList<>());

        result.forEach((student) -> {
            ArrayList<Document> scores = (ArrayList<Document>) student.get("scores");
            double score1 = 0;
            double score2 = 0;
            Document scoreDocument1 = null;
            Document scoreDocument2 = null;
            for (Document score : scores) {
                if (score.get("type").equals("homework")) {
                    double homeworkScore = (double) score.get("score");
                    if (score1 == 0) {
                        score1 = homeworkScore;
                        scoreDocument1 = score;
                    } else {
                        score2 = homeworkScore;
                        scoreDocument2 = score;
                    }
                }
            }
//            BasicDBObject updateObject;
            if (score1 > score2) {
                scores.remove(scoreDocument2);
//                updateObject = new BasicDBObject().append("$pull",
//                        new BasicDBObject("scores", new BasicDBObject("score", score2)));
            } else {
                scores.remove(scoreDocument1);
//                updateObject = new BasicDBObject().append("$pull",
//                        new BasicDBObject("scores", new BasicDBObject("score", score1)));
            }

//            BasicDBObject updateQuery = new BasicDBObject("_id", student.get("_id"));
//            coll.updateOne(updateQuery, updateObject);

            coll.updateOne(eq("_id", student.get("_id")), new Document("$set", new Document("scores", scores)));
        });

    }


}
