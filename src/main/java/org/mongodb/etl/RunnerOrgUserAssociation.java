package org.mongodb.etl;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.UuidCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class RunnerOrgUserAssociation {

    private MongoClient client;
    private int batchSize = 20000;
    private MongoCollection<Document> src;
    private MongoCollection<Document> tgt;
    private InsertManyOptions options;

    public static void main(String[] args) {

       /*
            Args :
                MongoURI,
                Source Namespace
                Target Namespace

            Example:
                "mongodb://siteRootAdmin:UIfqk3LRuFtcVmn5@casstomongoreg-shard-00-00-ubxcu.mongodb.net:27017,casstomongoreg-shard-00-01-ubxcu.mongodb.net:27017,casstomongoreg-shard-00-02-ubxcu.mongodb.net:27017/groupmanager?ssl=true&replicaSet=casstomongoreg-shard-0&authSource=admin&retryWrites=true&w=majority"
                "groupmanager.memberplay"
                "groupmanager.memberplayout"
         */

        if (args.length != 3) {
            throw new IllegalArgumentException("expected 3 arguments: mongoUri <src namespace> <tgt namespace>");
        }

        // Starting the thread run()
        new RunnerOrgUserAssociation(args[0], args[1], args[2]).run();
    }

    public RunnerOrgUserAssociation(String mongoUri, String srcNs, String tgtNs) {

        String[] srcSplit = srcNs.split("\\.");
        String[] tgtSplit = tgtNs.split("\\.");

        if (srcSplit.length != 2) {
            throw new IllegalArgumentException("src namespace is not valid (" + srcNs + ")");
        }
        if (tgtSplit.length != 2) {
            throw new IllegalArgumentException("tgt namespace is not valid (" + tgtNs + ")");
        }

        MongoClientSettings mcs = MongoClientSettings.builder()
                .codecRegistry(fromRegistries(
                        fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)),
                        MongoClientSettings.getDefaultCodecRegistry()))
                .applyConnectionString(new ConnectionString(mongoUri))
                .build();


        client = MongoClients.create(mcs);

        src = client.getDatabase(srcSplit[0]).getCollection(srcSplit[1], Document.class);
        tgt = client.getDatabase(tgtSplit[0]).getCollection(tgtSplit[1], Document.class);

        options = new InsertManyOptions();
        options.ordered(false);

    }

    public void populateTestData(int docCount) {

        System.out.println("dropping src collection");
        Mono.from(src.drop()).block();

        long start = System.currentTimeMillis();

        Flux<Document> docFlux = Flux.generate(
                () -> 1,
                (state, sink) -> {
                    Document doc = new Document();
                    doc.put("text", new BsonString("{ f1: \"some string\", f2: " + state + ", f3: \"another string\" }"));
                    sink.next(doc);
                    if (state == docCount)
                        sink.complete();
                    return state+1;
                }
        );

        docFlux
                .buffer(batchSize)
                .flatMap(batch -> src.insertMany(batch, options), 3)
                .blockLast();

        double time = (System.currentTimeMillis() - start) /1000;
        double speed = docCount / time;

        System.out.println("populated " + docCount + " documents in " + Math.round(time) + "s (" + Math.round(speed) + " doc/s)");

    }


    public void run() {
        System.out.println("dropping tgt collection");
        Mono.from(tgt.drop()).block();

        long start = System.currentTimeMillis();
        long docCount = Mono.from(src.estimatedDocumentCount()).block();

        Flux.from(src.find().batchSize(batchSize))                                 // Extract
                .flatMap(transform, 8)                                 // Transform
                .buffer(batchSize)                                                 // Batch docs
                .flatMap(batch -> tgt.insertMany(batch, options), 8)   // Load docs
//                .doOnNext(System.out::println)
                .doOnComplete(() -> System.out.println("complete!"))
                .blockLast();

        double time = (System.currentTimeMillis() - start) /1000;
        double speed = docCount / time;

        System.out.println(
                "transformed " + docCount +
                        " documents in " + Math.round(time) +
                        "s (" + Math.round(speed) + " doc/s)");

    }

    /**
     * Takes an incoming document and parses the JSON in the "text" field into a Document which is then returned.
     */
    Function<Document, Mono<Document>> transform = doc -> {

        Date createdateISO = null;
        Date updatedateISO = null;

        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            String createdate = doc.getString("createdate");
            String updatedate = doc.getString("updatedate");

            // Convert the dates although some dates are really badly not dates
            try {
                if (createdate != null && createdate.length() > 0)
                    createdateISO = dateFormat.parse(createdate);
            } catch( java.text.ParseException pe ) {
                new Exception("Not a valid date : {\"createdate\": \"createdate\"} for createdate");
            }

            try {
                if ( updatedate != null && updatedate.length() > 0 )
                    updatedateISO = dateFormat.parse(updatedate);
            } catch( java.text.ParseException pe ) {
                new Exception("Not a valid date : {\"updatedate\": \"updatedate\"} for updatedate");
            }


            UUID uuidID = UUID.randomUUID();

            // UUID to be stored as organizationid from id
            String organizationid = doc.getString("id");

            String assocJsonBlob = doc.getString("assocblob");


            // Hydrate
            Document newDoc = new Document("_id", uuidID);
            newDoc.append("organizationid", organizationid);
            newDoc.append("associd", doc.getString("associd"));
            newDoc.append("authgroupid", doc.getString("authgroupid"));
            newDoc.append("authgrouptype", doc.getString("authgrouptype"));
            newDoc.append("status", doc.getString("status"));

            try {
                newDoc.append("assocblob", new Document(Document.parse(assocJsonBlob)));
            } catch( Exception jpe ) {
                newDoc.append("assocblob", doc.getString("assocblob"));
            }

            if( createdateISO != null )
                newDoc.append("createdat", createdateISO);
            else
                newDoc.append("createdat", createdate);

            if( updatedateISO != null )
                newDoc.append("updatedat", updatedateISO);
            else
                newDoc.append("updatedat", updatedate);

            return Mono.just(newDoc);

        }
        catch(Exception e)
        {
            System.out.println( doc.toJson() );
            e.printStackTrace();
        }
        return null;

    };


}

