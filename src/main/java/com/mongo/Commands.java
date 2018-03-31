package com.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Indexes.descending;
import static com.mongodb.client.model.Sorts.orderBy;

public class Commands {

    public static void main(String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase db = client.getDatabase("test");
        MongoCollection<Document> coll = db.getCollection("movies");
        coll.drop();

        //Insert docs

        //Insert one
        Document document1 = new Document("name", "The Dark Knight")
                .append("rating","*****")
                .append("year", "2010");
        coll.insertOne(document1);
        Helper.printJson(coll.find().first());

        //Insert many
        Document document2 = new Document("name", "Arrival")
                .append("rating","*****")
                .append("year", "2016");
        Document document3 = new Document("name", "Inception")
                .append("rating","*****")
                .append("year", "2011");;
        coll.insertMany(Arrays.asList(document2, document3));
        List<Document> results = coll.find().into(new ArrayList<>());
        results.forEach((result) -> Helper.printJson(result));
//        MongoCursor<Document> cursor = coll.find().iterator();
//        while(cursor.hasNext()) {
//            Helper.printJson(cursor.next());
//        }

        //count
        System.out.println(coll.count());

        //filter
        Bson filter = new Document("name","Arrival").append("rating","*****");
        Helper.printJson(coll.find(filter).first());
        Bson advanceFilter = new Document("year",new Document("$gt", "2010").append("$lt","2015"));
        Helper.printJson(coll.find(advanceFilter).first());
        Bson filter1 = Filters.and(gt("year","2010"), lt("year","2015"));
        Helper.printJson(coll.find(filter1).first());

        //projection
        Bson filter2 = Filters.eq("name","The Dark Knight");
        Bson projection = new Document("name",0).append("_id", 0);
        Helper.printJson(coll.find(filter2).projection(projection).first());
        Bson projection2 = Projections.include("rating");
        Helper.printJson(coll.find(filter2).projection(projection2).first());
        Bson projection3 = Projections.fields(Projections.include("rating"), Projections.excludeId());
        Helper.printJson(coll.find(filter2).projection(projection3).first());
        Helper.printJson(coll.find(filter2).projection(new Document("name",1).append("_id",0)).first());

        //sort and limit
        Bson sort = new Document("year",-1);
        List<Document> list1 = coll.find().sort(sort).into(new ArrayList<>());
        list1.forEach((item) -> Helper.printJson(item));
        Bson sort1 = descending("year");
        List<Document> list2 = coll.find().sort(sort1).limit(3).into(new ArrayList<>());
        list2.forEach((item) -> Helper.printJson(item));
        Bson sort2 = orderBy(descending("year"), ascending("name"));
        List<Document> list3 = coll.find().sort(sort2).skip(1).into(new ArrayList<>());
        list3.forEach((item) -> Helper.printJson(item));

    }

}