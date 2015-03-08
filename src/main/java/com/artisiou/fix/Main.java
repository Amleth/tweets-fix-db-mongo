package com.artisiou.fix;

import com.mongodb.*;

import java.net.UnknownHostException;

public class Main {

    public static void main(String[] argv) throws UnknownHostException {
        System.out.println();

        MongoClient mongoClient = new MongoClient("localhost", 27017);
        DB db = mongoClient.getDB("hdr");
        DBCollection coll = db.getCollection("tweets");

        //
        // Info...
        //

        System.out.println("Total:   " + coll.getCount());
        System.out.println();

        //
        // rawJson
        //

        System.out.println("Restructuring documents: the tweet must be in ``rawJson`` field");
        DBCursor withRawJson = coll.find(new BasicDBObject("rawJson", new BasicDBObject("$exists", true)));
        DBCursor withoutRawJson = coll.find(new BasicDBObject("rawJson", new BasicDBObject("$exists", false)));
        System.out.println("    ok    : " + withRawJson.size());
        System.out.println("    not ok: " + withoutRawJson.size());

        try {
            while (withoutRawJson.hasNext()) {
                DBObject doc = withoutRawJson.next();
                coll.update(
                        new BasicDBObject("_id", doc.get("_id")),
                        new BasicDBObject().append("rawJson", doc)
                );
            }
        } finally {
            withRawJson.close();
            withoutRawJson.close();
        }
        System.out.println();

        //TODO: clean rawJson._id

        //
        // timestamp_ms
        //

        System.out.println("Pulling ``timestamp_ms`` up & converting to Long...");
        DBCursor tweetsToRestructure = coll.find();
        try {
            while (tweetsToRestructure.hasNext()) {
                DBObject doc = tweetsToRestructure.next();
                Long timestamp = Long.parseLong(((DBObject) doc.get("rawJson")).get("timestamp_ms").toString());
                BasicDBObject newDoc = new BasicDBObject();
                newDoc.append("$set", new BasicDBObject().append("timestamp_ms", timestamp));
                coll.update(new BasicDBObject("_id", doc.get("_id")), newDoc);
            }
        } finally {
            tweetsToRestructure.close();
        }
        System.out.println();

        //
        // cleanup
        //

        System.out.println("Cleaning ``id`` up...");
        coll.update(new BasicDBObject(), new BasicDBObject("$unset", new BasicDBObject("id", 1)), false, true);
        System.out.println();

        System.out.println("Cleaning ``__v`` up...");
        coll.update(new BasicDBObject(), new BasicDBObject("$unset", new BasicDBObject("__v", 1)), false, true);
        System.out.println();

        System.out.println("Cleaning ``rawJson._id`` up...");
        //TODO
        System.out.println();

        System.out.println("Cleaning ``rawJson.__v`` up...");
        //TODO
        System.out.println();

    }
}
