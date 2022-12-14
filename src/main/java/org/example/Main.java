package org.example;

import org.example.record.Person;
import org.example.repository.Dynamodb;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class Main {
    public static void main(String[] args) {
        Person mykola = new Person("Mykola", 29, true);
        Person violet = new Person("Violet", 29, true);
        Person max = new Person("Max", 4, false);

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.US_EAST_1;
        DynamoDbClient ddb = DynamoDbClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(region)
                .build();

//        Dynamodb.getDynamoDBItem(ddb, "java_test", "PK", "Mykola", "SK", "Mykola");

        Dynamodb.deleteDymamoDBItem(ddb, "java_test", "PK", "Mykola", "SK", "Mykola");



//        Dynamodb.putItemInTable(ddb, "java_test", mykola);
//        Dynamodb.putItemInTable(ddb, "java_test", violet);
//        Dynamodb.putItemInTable(ddb, "java_test", max);

//        DynamoDbClient ddb = DynamoDbClient.create();
//        Dynamodb.createTable(ddb, "java_test", "PK", "SK");
//        Dynamodb.deleteDynamoDBTable(ddb, "java_test");


    }
}