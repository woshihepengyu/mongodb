package com.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.models.Account;
import com.mongodb.models.Transfer;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.logging.Level;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.inc;
import static java.util.logging.Logger.getLogger;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class Demo {

    private static MongoClient client;
    private static MongoCollection<Account> accountCollection;
    private static MongoCollection<Transfer> transferCollection;

    private final String USER_ALICE = "alice";
    private final String USER_BOB = "bob";

    private static final String jsonSchema = "{ $jsonSchema: "
        + "{ bsonType: \"object\", required: [ \"_id\", \"balance\" ], "
        + "properties: { _id: { bsonType: \"string\", "
        + "description: \"must be a string and is required\" }, "
        + "balance: { bsonType: \"int\", minimum: 0, "
        + "description: \"must be a positive integer and is required\" }} } } ";

    public static void main(String[] args) {
        initMongoDB(args[0]);
        new Demo().demo();
    }

    private static void initMongoDB(String mongodbURI) {
        getLogger("org.mongodb.driver").setLevel(Level.SEVERE);
        CodecRegistry codecRegistry = fromRegistries(
                MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder()
                        .register("com.mongodb.models").build())
        );
        MongoClientOptions.Builder options = new MongoClientOptions.Builder()
                .codecRegistry(codecRegistry);
        MongoClientURI uri = new MongoClientURI(mongodbURI, options);
        client = new MongoClient(uri);

        MongoDatabase db = client.getDatabase("test");
        if (!collectionExists(db, "account")) {
            db.createCollection("account", productJsonSchemaValidator());
        }
        accountCollection = db.getCollection("account", Account.class);
        if (!collectionExists(db, "transfer")) {
            db.createCollection("transfer");
        }
        transferCollection = db.getCollection("transfer", Transfer.class);

    }

    private static CreateCollectionOptions productJsonSchemaValidator() {
        return new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validationAction(ValidationAction.ERROR)
                        .validator(BsonDocument.parse(jsonSchema)));
    }

    private void demo() {

        clearCollections();
        insertAccounts();
        printDatabaseState();

        System.out.println("#########  不使用事务 #########");
        System.out.println("Bob向Alice转账");
        System.out.println("两人账户的分别操作可能导致数据错误");
        System.out.println("--------------------------------------------------------");

        depositTo(USER_ALICE, 100);
        sleep();
        withdrawFrom(USER_BOB, 100);

        System.out.println("####################################");
        printDatabaseState();

        sleep();
        System.out.println("######### 使用事务 #########");
        System.out.println("Bob向Alice转账");
        System.out.println("两人账户的操作将在同一个事务内执行");
        System.out.println("--------------------------------------------------------");

        transferFunds(USER_BOB, USER_ALICE, 10);

        sleep();
        System.out.println("######### 使用事务 #########");
        System.out.println("Bob向Alice转账");
        System.out.println("操作错误将导致事务回滚，数据库返回事务进行前的有效状态");
        System.out.println("--------------------------------------------------------");

        transferFunds(USER_BOB, USER_ALICE, 520);

        client.close();

    }

    private void withdrawFrom(String id, Integer amount) {
        System.out.println("Trying to withdraw " + amount + " from " + id + " ... ");
        try {
            accountCollection.updateOne(
                    eq("_id", id),
                    inc("balance", -amount)
            );
        } catch (Exception e) {
            System.out.println("#### BALANCE CANNOT BE NEGATIVE ####");
        }
    }

    private void depositTo(String id, Integer amount) {
        System.out.println("Trying to deposit " + amount + " to " + id + " ... ");
        accountCollection.updateOne(
                eq("_id", id),
                inc("balance", amount)
        );
    }

    private void transferFrom(ClientSession session, String id, Integer amount) {
        System.out.println("Trying to transfer " + amount + " from " + id + " ... ");
        try {
            accountCollection.updateOne(
                    session,
                    eq("_id", id),
                    inc("balance", -amount)
            );
        } catch (Exception e) {
            System.out.println("#### BALANCE CANNOT BE NEGATIVE ####");
            throw e;
        }
    }

    private void transferTo(ClientSession session, String id, Integer amount) {
        System.out.println("Trying to transfer " + amount + " to " + id + " ... ");
        accountCollection.updateOne(
                session,
                eq("_id", id),
                inc("balance", amount)
        );
    }

    private void recordTransfer(ClientSession session,
                                String from,
                                String to,
                                Integer amount) {
        System.out.println("Recording transfer from "
                            + from + " to " + to
                            + " with amount " + amount);
        transferCollection.insertOne(
                session,
                new Transfer(new Date(), from, to, amount)
        );
    }

    private void transferFunds(String from, String to, Integer amount) {
        ClientSession session = client.startSession();
        try {
            session.startTransaction();
            transferTo(session, to, amount);
            sleep();
            transferFrom(session, from, amount);
            recordTransfer(session, from, to, amount);
            session.commitTransaction();
        } catch (Exception e) {
            System.out.println("#### ROLLBACK TRANSACTION ####");
            session.abortTransaction();
        } finally {
            session.close();
            printDatabaseState();
        }
    }

    private void insertAccounts() {
        accountCollection.insertOne(new Account(USER_ALICE, 20));
        accountCollection.insertOne(new Account(USER_BOB, 20));
    }

    private void clearCollections() {
        accountCollection.deleteMany(new BsonDocument());
        transferCollection.deleteMany(new BsonDocument());
    }

    private void printDatabaseState() {
        System.out.println("Database state:");
        accountCollection.find().into(new ArrayList<>())
                .forEach(System.out::println);
        transferCollection.find().into(new ArrayList<>())
                .forEach(System.out::println);
        System.out.println();
    }

    private static boolean collectionExists(MongoDatabase db,
                                            final String collectionName) {
        return db.listCollectionNames().into(new HashSet<>())
                .contains(collectionName);
    }

    private void sleep() {
        System.out.println("Sleeping 3 seconds...");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            System.err.println("Oups...");
            e.printStackTrace();
        }
    }
}