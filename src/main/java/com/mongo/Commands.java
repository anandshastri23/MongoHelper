package com.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Indexes.descending;
import static com.mongodb.client.model.Sorts.orderBy;
import static com.mongodb.client.model.Updates.set;

public class Commands {

    public static void main(String[] args) {
//      MongoClientOptions options = MongoClientOptions.builder().connectionsPerHost(100).build();
//      MongoClient client = new MongoClient(new ServerAddress(), options);
        MongoClient client = new MongoClient();
        MongoDatabase db = client.getDatabase("test");
        MongoCollection<Document> coll = db.getCollection("movies");
        coll.drop();

        //Insert one
        Document document1 = new Document("name", "The Dark Knight")
                .append("rating","*****")
                .append("year", "2010");
        coll.insertOne(document1);

        //Insert many
        Document document2 = new Document("name", "Arrival").append("rating","*****").append("year", "2016");
        Document document3 = new Document("name", "Inception").append("rating","*****").append("year", "2011");
        Document document4 = new Document("name", "Kill Bill").append("rating", "****").append("year","2006");
        coll.insertMany(Arrays.asList(document2, document3, document4));

        MongoCursor<Document> cursor = coll.find().iterator();
        while(cursor.hasNext()) {
            Helper.printJson(cursor.next());
        }

        //count
        System.out.println(coll.count());

        //filter
        Bson filter = new Document("name","Arrival").append("rating","*****");
        printJsonFromArray(coll.find(filter).into(new ArrayList<>()), "Filter");

        Bson advanceFilter = new Document("year",new Document("$gt", "2010").append("$lt","2015"));
        printJsonFromArray(coll.find(advanceFilter).into(new ArrayList<>()), "Advanced Filter");

        Bson filter1 = Filters.and(gt("year","2010"), lt("year","2015"));
        printJsonFromArray(coll.find(filter1).into(new ArrayList<>()), "Filter builder");

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
        printJsonFromArray(coll.find().sort(sort).into(new ArrayList<>()), "sort ascending");

        Bson sort1 = descending("year");
        printJsonFromArray(coll.find().sort(sort1).limit(3).into(new ArrayList<>()), "sort descending");

        Bson sort2 = orderBy(descending("year"), ascending("name"));
        printJsonFromArray(coll.find().sort(sort2).skip(1).into(new ArrayList<>()),"sort order by");

        //replace
        coll.replaceOne(eq("name", "Kill Bill"),new Document("name","Kill Bill 2").append("rating", "*****").append("year", "2008"));
        printJsonFromArray(coll.find().into(new ArrayList<>()), "replace one");

        //update
        coll.updateOne(eq("name","Kill Bill 2"), new Document("$set", new Document("rating","****")));
        printJsonFromArray(coll.find().into(new ArrayList<>()),"update one");

        coll.updateOne(eq("name", "Kill Bill 2"), set("rating","*****"));
        printJsonFromArray(coll.find().into(new ArrayList<>()), "update builder");

        coll.updateOne(eq("name", "Inception"), Updates.combine(set("year","2012"), set("rating","****")));
        printJsonFromArray(coll.find().into(new ArrayList<>()), "update combine");

        coll.updateMany(eq("rating","****"), set("rating","*****"));
        printJsonFromArray(coll.find().into(new ArrayList<>()), "update many");

        //upserts
        coll.updateOne(new Document("name", "Ready Player One"), new Document("$set", new Document("rating","**")), new UpdateOptions().upsert(true));
        printJsonFromArray(coll.find().into(new ArrayList<>()), "upserts");

        //delete
        coll.deleteOne(eq("rating", "**"));
        printJsonFromArray(coll.find().into(new ArrayList<>()), "delete one");

        coll.deleteMany(gt("year", "2010"));
        printJsonFromArray(coll.find().into(new ArrayList<>()), "delete many");

    }

    static void printJsonFromArray(ArrayList<Document> list, String banner){
        System.out.println("*********************** " +banner +" ***********************");
        list.forEach((item) -> Helper.printJson(item));
        System.out.println("*********************** END ***********************\n\n");
    }

}