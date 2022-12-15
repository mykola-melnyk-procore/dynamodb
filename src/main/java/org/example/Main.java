package org.example;

import org.example.record.Person;
import org.example.repository.Dynamodb;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws URISyntaxException {
        Scanner sc = new Scanner(System.in);
        URI localstackURI = new URI("http://localhost:4566");

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.US_EAST_1;
        DynamoDbClient ddb = DynamoDbClient.builder()
                .endpointOverride(localstackURI)
                .credentialsProvider(credentialsProvider)
                .region(region)
                .build();
        do {
            System.out.println("""
                    Possible options are:
                    1. Create a DynamoDB table;
                    2. Create a person and add to DynamoDB table;
                    3. Delete a person from DynamoDB table;
                    4. Show all persons in a DynamoDB table;
                    5. Delete a DynamoDB table;
                    6. List all tables.
                    7. Quit.
                    Please select the option by typing its number (1-7):""");
            char option = sc.next().charAt(0);
            switch (option) {
                case '1' -> {
                    System.out.println("Please specify the DynamoDB table name to be created:");
                    sc.nextLine();
                    String tableName = sc.nextLine();
                    System.out.printf("Creating the DynamoDB table %s.\n", tableName);
                    Dynamodb.createTable(ddb, tableName, "PK", "SK");
                }
                case '2' -> {
                    System.out.println("""
                            Creating a person.
                            Please enter a name:""");
                    sc.nextLine();
                    String name = sc.nextLine();
                    System.out.println("Please enter age:");
                    int age = sc.nextInt();
                    System.out.println("Is the person married (y/n)?");
                    boolean isMarried;
                    sc.nextLine();
                    char answer = sc.next().charAt(0);
                    switch (answer) {
                        case 'y', 'Y' -> isMarried = true;
                        default -> isMarried = false;
                    }
                    Person person = new Person(name, age, isMarried);
                    System.out.printf("""
                            Person %s is created.
                            Adding them to DynamoDB.
                            Please specify the DynamoDB table name to add to.\n""", name);
                    sc.nextLine();
                    String tableName = sc.nextLine();
                    Dynamodb.putItemInTable(ddb, tableName, person);
                }
                case '3' -> {
                    System.out.println("""
                            Deleting a person from a DynamoDB table.
                            Please specify the TABLE name to operate upon:""");
                    sc.nextLine();
                    String tableName = sc.nextLine();
                    System.out.println("Please specify the name of the person to be deleted:");
                    sc.nextLine();
                    String name = sc.nextLine();
                    System.out.printf("Deleting %s from %s.", name, tableName);
                    Dynamodb.deleteDymamoDBItem(ddb, tableName, "PK", name, "SK", name);
                }
                case '4' -> {
                    System.out.println("""
                            Showing all persons in a table.
                            Please specify the name of the table to process.""");
                    sc.nextLine();
                    String tableName = sc.nextLine();
                    System.out.printf("%s table content:\n", tableName);
                    Dynamodb.scanDynamoTable(ddb, tableName);
                }
                case '5' -> {
                    System.out.println("""
                            Deleting a DynamoDB table.
                            Please specify the name of the table to be deleted:""");
                    sc.nextLine();
                    String tableName = sc.nextLine();
                    System.out.printf("Deleting the DynamoDB table %s.", tableName);
                    Dynamodb.deleteDynamoDBTable(ddb, tableName);
                }
                case '6' -> {
                    System.out.println("Listing all tables.");
                    Dynamodb.listAllTables(ddb);
                }
                case '7' -> System.exit(0);
            }
        } while (1 == 1);
    }
}