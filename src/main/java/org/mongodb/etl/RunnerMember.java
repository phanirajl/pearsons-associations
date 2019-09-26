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
import org.bson.types.Binary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Function;

import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class RunnerMember {

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
        new RunnerMember(args[0], args[1], args[2]).run();
    }


    public RunnerMember(String mongoUri, String srcNs, String tgtNs) {

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
     * Convert a UUID object to a Binary with a subtype 0x04
     */
    public static Binary toStandardBinaryUUID(java.util.UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();

        byte[] uuidBytes = new byte[16];

        for (int i = 15; i >= 8; i--) {
            uuidBytes[i] = (byte) (lsb & 0xFFL);
            lsb >>= 8;
        }

        for (int i = 7; i >= 0; i--) {
            uuidBytes[i] = (byte) (msb & 0xFFL);
            msb >>= 8;
        }

        return new Binary((byte) 0x04, uuidBytes);
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

            createdatISO = dateFormat.parse(createdat);
            updatedatISO = dateFormat.parse(updatedat);

        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        String groupid = doc.getString("groupid");

        UUID uuidID = UUID.randomUUID();

        // memberId
        Object memberIdObject = doc.get("memberid");
        String memberId = null;
        if ( memberIdObject instanceof String )
            memberId = doc.getString("memberid" );
        else if ( memberIdObject instanceof Integer )
            memberId = String.valueOf(memberIdObject);

        // memberType
        Object membertypeObject = doc.get("membertype");
        String membertype = null;
        if ( membertypeObject instanceof String )
            membertype = doc.getString("membertype" );
        else if ( membertypeObject instanceof Integer )
            membertype = String.valueOf(membertypeObject);


        Document newDoc = new Document("_id", uuidID);

        if ( memberId != null )
            newDoc.append("memberid", memberId);

        newDoc.append("system", doc.getString("system"));

        if ( membertype != null )
            newDoc.append("membertype", membertype);

        newDoc.append("groupid",   UUID.fromString(groupid));

        if ( createdatISO != null )
            newDoc.append("createdat", createdatISO);

        if( updatedatISO != null )
            newDoc.append("updatedat", updatedatISO);

        return Mono.just(newDoc);
    };

}
