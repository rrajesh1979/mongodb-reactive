package com.learn;

import com.google.gson.Gson;
import com.mongodb.reactivestreams.client.MongoClient;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import spark.Request;
import spark.Response;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;

public class APIRoutes {

    private final MongoClient mongoClient;

    private final JsonWriterSettings plainJSON = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED)
            .binaryConverter((value, writer) -> writer.writeString(Base64.getEncoder().encodeToString(value.getData())))
            .dateTimeConverter((value, writer) -> {
                ZonedDateTime zonedDateTime = Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC);
                writer.writeString(DateTimeFormatter.ISO_DATE_TIME.format(zonedDateTime));
            }).decimal128Converter((value, writer) -> writer.writeString(value.toString()))
            .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
            .symbolConverter((value, writer) -> writer.writeString(value)).build();

    public APIRoutes(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    public String getBooks(Request request, Response response) {
        BookDAL bookDAL;
        bookDAL = new BookDAL(mongoClient);

        List<Document> books = bookDAL.getBooks();

        response.status(200);
        return new Gson().toJson(books);
    }

    public String addBook(Request request, Response response) {
        BookDAL bookDAL;
        bookDAL = new BookDAL(mongoClient);

        String bookId = request.splat()[0];
        String bookName = request.splat()[1];
        String authorName = request.splat()[2];

        Document book = bookDAL.addBook(bookId, bookName, authorName);

        response.status(200);
        return new Gson().toJson(book);
    }
}
