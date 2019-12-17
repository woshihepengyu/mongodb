package com.mongodb;

        import com.mongodb.client.model.changestream.ChangeStreamDocument;
        import com.mongodb.client.model.changestream.FullDocument;
        import org.bson.codecs.configuration.CodecRegistry;
        import org.bson.codecs.pojo.PojoCodecProvider;

        import java.text.SimpleDateFormat;
        import java.util.Date;
        import java.util.function.Consumer;
        import java.util.logging.Level;

        import static java.util.logging.Logger.getLogger;
        import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
        import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class ChangeStreamDemo {

    private static MongoClient client;

    public static void main(String[] args) {
        initMongoDB(args[0]);
        watchChangeStream(client);
    }

    private static void watchChangeStream(MongoClient client) {
        System.out.println("Watching " + client.getAddress());
        client.watch()
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .forEach((Consumer<ChangeStreamDocument<?>>) doc -> System.out.println(
                        new SimpleDateFormat("yyy-MM-dd hh:mm:ss")
                        .format(new Date(doc.getClusterTime().getTime() * 1000L))
                        + " => " + doc.getOperationType().toString()
                        + ": " + doc.getFullDocument()
                ));
    }

    private static void initMongoDB(String mongodbURI) {
        getLogger("org.mongodb.driver").setLevel(Level.SEVERE);

        CodecRegistry providers = fromProviders(
                PojoCodecProvider.builder().register("com.mongodb.models").build());
        CodecRegistry codecRegistry = fromRegistries(
                MongoClient.getDefaultCodecRegistry(), providers);
        MongoClientOptions.Builder options = new MongoClientOptions.Builder()
                .codecRegistry(codecRegistry);

        MongoClientURI uri = new MongoClientURI(mongodbURI, options);
        client = new MongoClient(uri);

        return;
    }
}