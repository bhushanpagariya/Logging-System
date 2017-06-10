package Util;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bhushan on 6/9/17.
 */
public class QueryGen {
    private Document doc;
    private List<QuerySpec> querySpecList;

    public QueryGen() {
        this.doc = null;
    }

    public QueryGen append(QuerySpec query) {
        if(this.doc == null) {
            this.doc = new Document();
            this.querySpecList = new ArrayList<>();
        }
        this.querySpecList.add(query);
        if(query.getValue()!=null)
            createQueryString(query);
        else
            createRangeQueryString(query);
        return this;
    }

    public String toString() {
        return this.doc.toJson();
    }

    public List<QuerySpec> getQuerySpecList() {
        return this.querySpecList;
    }

    private void createQueryString(QuerySpec querySpec) {
        switch (querySpec.getOperator()) {
            case GT:
                this.doc.append(querySpec.getKey(), new Document().append("$gt", querySpec.getValue()));
                break;
            case LT:
                this.doc.append(querySpec.getKey(), new Document().append("lt", querySpec.getValue()));
                break;
            case GTE:
                this.doc.append(querySpec.getKey(), new Document().append("$gte", querySpec.getValue()));
                break;
            case LTE:
                this.doc.append(querySpec.getKey(), new Document().append("$lte", querySpec.getValue()));
                break;
            case EQUAL:
                this.doc.append(querySpec.getKey(), querySpec.getValue());
                break;
            default:
                break;
        }
    }

    private void createRangeQueryString(QuerySpec querySpec) {
        if(querySpec.isLhsInclusive())
            if(querySpec.isRhsInclusive())
                this.doc.append(querySpec.getKey(), new Document().append("$gte", querySpec.getLhsValue()).append("$lte", querySpec.getRhsValue()));
            else
                this.doc.append(querySpec.getKey(), new Document().append("$gte", querySpec.getLhsValue()).append("$lt", querySpec.getRhsValue()));
        else
            if(querySpec.isRhsInclusive())
                this.doc.append(querySpec.getKey(), new Document().append("$gt", querySpec.getLhsValue()).append("$lte", querySpec.getRhsValue()));
            else
                this.doc.append(querySpec.getKey(), new Document().append("$gt", querySpec.getLhsValue()).append("$lt", querySpec.getRhsValue()));
    }

    public static void main(String[] args) {
        QueryGen queryGen = new QueryGen();
        queryGen.append(new QuerySpec("name", "bhushan", QuerySpec.Operator.EQUAL)).
                append(new QuerySpec("age", 24, QuerySpec.Operator.GTE));

        System.out.println(queryGen.toString());
    }

}
