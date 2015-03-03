/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.richeninfo.mongodbutils;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import java.util.HashMap;
import java.util.Map;
import net.sf.json.JSONSerializer;
import org.bson.BasicBSONCallback;

/**
 *
 * @author xiaolie
 */
public class QueryInfo {
    public String dbName;
    public String collName;
    public String action;
    public DBObject query;
    public Long limit;
    public Long skip;
    public DBObject order;
    public DBObject updateObj;
    
    public void setQuery(String json) {
        query = (DBObject)JSON.parse(json, new BasicBSONCallback());
        System.out.println(query);
    }
    
    public void setSort(String json) {
        order = (DBObject)JSON.parse(json, new BasicBSONCallback());
    }
    
    public String debugStr() {
        return "dbName:" + dbName + 
                "  collName:" + collName + 
                "  action:" + action + 
                " query:" + query + 
                "  limit:" + limit + 
                "  sort:" + order +
                "  updateObj:" + updateObj;
    }
}
