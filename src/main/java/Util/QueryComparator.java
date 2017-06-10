package Util;

import application.Record;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.bson.Document;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by bhushan on 5/31/17.
 */
public class QueryComparator {

    private static class JSONKeyVal {
        public String key;
        public Object value;
        JSONKeyVal(String key, Object value) {
            this.key = key;
            this.value = value;
        }
    }

    public enum Comparator {
        EQUAL, GT, LT, GTE, LTE, RANGE
    }

//    public static String createQueryString(Record record, Comparator comparator) {
//        if(comparator == Comparator.EQUAL)
//            return record.getJson();
//
//        Document doc = Document.parse(record.getJson());
//        Document newDoc = new Document();
//        for(String key : doc.keySet()) {
//            System.out.println(doc.get(key).getClass());
//            switch (comparator) {
//                case GT:
//                    newDoc.append(key, new Document().append("$gt", doc.get(key)));
//                    break;
//                case LT:
//                    newDoc.append(key, new Document().append("lt", doc.get(key)));
//                    break;
//                case GTE:
//                    newDoc.append(key, new Document().append("$gte", doc.get(key)));
//                    break;
//                case LTE:
//                    newDoc.append(key, new Document().append("$lte", doc.get(key)));
//                    break;
//            }
//        }
//        return newDoc.toJson();
//    }
//
//    public static String createQueryString(Record lhsRecord, Record rhsRecord, boolean lhsInclusive, boolean rhsInclusive) {
//        Document doc1 = Document.parse(lhsRecord.getJson());
//        Document doc2 = Document.parse(rhsRecord.getJson());
//        if(!(doc1.keySet().containsAll(doc2.keySet()) && doc2.keySet().containsAll(doc1.keySet())))
//            return null;
//
//        Document newDoc = new Document();
//        for(String key : doc1.keySet()) {
//            if(lhsInclusive)
//                if(rhsInclusive)
//                    newDoc.append(key, new Document().append("$gte", doc1.get(key)).append("$lte", doc2.get(key)));
//                else
//                    newDoc.append(key, new Document().append("$gte", doc1.get(key)).append("$lt", doc2.get(key)));
//            else
//                if(rhsInclusive)
//                    newDoc.append(key, new Document().append("$gt", doc1.get(key)).append("$lte", doc2.get(key)));
//                else
//                    newDoc.append(key, new Document().append("$gt", doc1.get(key)).append("$lt", doc2.get(key)));
//        }
//
//        return newDoc.toJson();
//    }


    public static String createQueryString(Record record, Comparator comparator) {
        try {
            List<JSONKeyVal> jsonKeyVals = parseJson(record.getJson());
            Document newDoc = new Document();
            for(int i = 0; i < jsonKeyVals.size(); i++) {
                System.out.println(jsonKeyVals.get(i).value.getClass());
                switch (comparator) {
                    case GT:
                        newDoc.append(jsonKeyVals.get(i).key, new Document().append("$gt", jsonKeyVals.get(i).value));
                        break;
                    case LT:
                        newDoc.append(jsonKeyVals.get(i).key, new Document().append("lt", jsonKeyVals.get(i).value));
                        break;
                    case GTE:
                        newDoc.append(jsonKeyVals.get(i).key, new Document().append("$gte", jsonKeyVals.get(i).value));
                        break;
                    case LTE:
                        newDoc.append(jsonKeyVals.get(i).key, new Document().append("$lte", jsonKeyVals.get(i).value));
                        break;
                    case EQUAL:
                        newDoc.append(jsonKeyVals.get(i).key, jsonKeyVals.get(i).value);
                        break;
                    default:
                        break;
                }
            }
            return newDoc.toJson();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String createQueryString(Record lhsRecord, Record rhsRecord, boolean lhsInclusive, boolean rhsInclusive) {
        Document doc1 = Document.parse(lhsRecord.getJson());
        Document doc2 = Document.parse(rhsRecord.getJson());
        if(!(doc1.keySet().containsAll(doc2.keySet()) && doc2.keySet().containsAll(doc1.keySet())))
            return null;

        try {
            List<JSONKeyVal> lhsJsonKeyVals = parseJson(lhsRecord.getJson());
            List<JSONKeyVal> rhsJsonKeyVals = parseJson(rhsRecord.getJson());
            Document newDoc = new Document();
            for(int i = 0; i < lhsJsonKeyVals.size(); i++) {
                if( !lhsJsonKeyVals.get(i).key.equals(rhsJsonKeyVals.get(i).key) )
                    return null;

                if(lhsInclusive)
                    if(rhsInclusive)
                        newDoc.append(lhsJsonKeyVals.get(i).key, new Document().append("$gte", lhsJsonKeyVals.get(i).value).append("$lte", rhsJsonKeyVals.get(i).value));
                    else
                        newDoc.append(lhsJsonKeyVals.get(i).key, new Document().append("$gte", lhsJsonKeyVals.get(i).value).append("$lt", rhsJsonKeyVals.get(i).value));
                else
                if(rhsInclusive)
                    newDoc.append(lhsJsonKeyVals.get(i).key, new Document().append("$gt", lhsJsonKeyVals.get(i).value).append("$lte", rhsJsonKeyVals.get(i).value));
                else
                    newDoc.append(lhsJsonKeyVals.get(i).key, new Document().append("$gt", lhsJsonKeyVals.get(i).value).append("$lt", rhsJsonKeyVals.get(i).value));
            }
            return newDoc.toJson();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }




    static List<JSONKeyVal> parseJson(String json) throws IOException {
        List<JSONKeyVal> parsedJsonList = new ArrayList<JSONKeyVal>();
        JsonReader reader = new JsonReader(new StringReader(json));
        reader.setLenient(true);
        while (true) {
            JsonToken token = reader.peek();
            switch (token) {
                case BEGIN_ARRAY:
                    reader.beginArray();
                    break;
                case END_ARRAY:
                    reader.endArray();
                    break;
                case BEGIN_OBJECT:
                    reader.beginObject();
                    break;
                case END_OBJECT:
                    reader.endObject();
                    break;
                case NAME:
                    reader.nextName();
                    break;
                case STRING:
                    String s = reader.nextString();
                    parsedJsonList.add( new JSONKeyVal(formatString(reader.getPath()), s));
                    break;
                case NUMBER:
//                    String n = reader.nextString();
                    try {
                        int n = reader.nextInt();
                        parsedJsonList.add(new JSONKeyVal(formatString(reader.getPath()), n));
                    } catch (NumberFormatException e) {
                        double n = reader.nextDouble();
                        parsedJsonList.add(new JSONKeyVal(formatString(reader.getPath()), n));
                    }
                    break;
                case BOOLEAN:
                    boolean b = reader.nextBoolean();
                    parsedJsonList.add( new JSONKeyVal(formatString(reader.getPath()), b));
                    break;
                case NULL:
                    reader.nextNull();
                    break;
                case END_DOCUMENT:
                    return parsedJsonList;
            }
        }
    }

    static private String formatString(String path) {
        path = path.substring(2);
        path = PATTERN.matcher(path).replaceAll("");
        return path;
    }

    static private String quote(String s) {
        return new StringBuilder()
                .append('"')
                .append(s)
                .append('"')
                .toString();
    }

    static final String REGEX = "\\[[0-9]+\\]";
    static final Pattern PATTERN = Pattern.compile(REGEX);

}
