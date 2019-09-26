package org.mongodb.etl;

import com.mongodb.ConnectionString;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.MongoClientSettings;
import org.bson.Document;
import org.bson.BsonString;
import org.bson.UuidRepresentation;
import org.bson.codecs.UuidCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class RunnerGroup {

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
        new RunnerGroup(args[0], args[1], args[2]).run();
    }

    public RunnerGroup(String mongoUri, String srcNs, String tgtNs) {

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

        Date createdatISO = null;
        Date updatedatISO = null;

        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            String createdat = doc.getString("createdat");
            String updatedat = doc.getString("updatedat");

            // Convert the dates
            if ( createdat != null && createdat.length() > 0 )
                createdatISO = dateFormat.parse(createdat);

            if ( updatedat != null && updatedat.length() > 0 )
                updatedatISO = dateFormat.parse(updatedat);

            // UUID to be stored as _id
            String uuid = doc.getString("id");

            // Permissions unwrap to proper String Array
            String permissions = doc.getString("permissions");
            List<String> permissionsList = null;

            if ( permissions != null & permissions.length() > 0 ) {
                permissions = permissions.replace("'","");
                permissions = permissions.replace("[","");
                permissions = permissions.replace("]","");
                permissionsList = Arrays.stream( permissions.split(",") )
                        .map(String::trim)
                        .collect(Collectors.toList());
            }

            // contextid get correct type
            Object contextIdObject = doc.get("contextid");
            String contextid = null;

            if ( contextIdObject instanceof String )
                contextid = doc.getString("contextid" );
            else if ( contextIdObject instanceof Integer )
                contextid = String.valueOf(contextIdObject);

            // contextType get correct type
            Object contextTypeObject = doc.get("contexttype");
            String contextType = null;

            if ( contextTypeObject instanceof String )
                contextType = doc.getString("contexttype" );
            else if ( contextTypeObject instanceof Integer )
                contextType = String.valueOf(contextTypeObject);

            // name get correct type
            Object nameObject = doc.get("name");
            String name = null;

            if ( nameObject instanceof String )
                name = doc.getString("name" );
            else if ( contextIdObject instanceof Integer )
                name = String.valueOf(nameObject);


            // system get correct type
            Object systemObject = doc.get("system");
            String system = null;

            if ( systemObject instanceof String )
                system = doc.getString("system" );
            else if ( contextIdObject instanceof Integer )
                system = String.valueOf(systemObject);



            // Hydrate
            Document newDoc = new Document("_id", UUID.fromString(uuid));

            if ( contextid != null )
                newDoc.append("contextid", contextid);

            if ( contextType != null )
                newDoc.append("contexttype", contextType);

            if ( createdatISO != null )
                newDoc.append("createdat", createdatISO);

            if ( name != null ) {
                newDoc.append("name", name);
            }

            newDoc.append("parentid", doc.getString("parentid"));

            if ( permissionsList != null ) {
                newDoc.append("permissions", permissionsList);

            }

            if ( system != null ) {
                newDoc.append("system", system);
            }

            newDoc.append("type", doc.getString("type"));

            if( updatedatISO != null )
                newDoc.append("updatedat", updatedatISO);

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
