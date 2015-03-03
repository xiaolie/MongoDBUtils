/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.shenjitang.mongodbutils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DefaultDBCallback;
import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import com.mongodb.util.JSON;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.bson.BasicBSONObject;

/**
 *
 * @author xiaolie
 */
public class MongoDbOperater {

    public MongoDbOperater() {
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public void setMongoClient(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }
    private MongoClient mongoClient;
    
    public void insert(String dbName, String colName, Map map) {
        DB db = mongoClient.getDB(dbName);
        DBCollection coll = db.getCollection(colName);
        BasicDBObject record = new BasicDBObject(map);
        coll.insert(record);
    }

    public void insert(String dbName, String colName, Object obj) throws Exception {
        insert(dbName, colName, BeanUtilEx.transBean2Map(obj));
    }

    public void insert(String dbName, String colName, String json) {
        DB db = mongoClient.getDB(dbName);
        DBCollection coll = db.getCollection(colName);
        BasicBSONObject obj = (BasicBSONObject)JSON.parse(json, new DefaultDBCallback(coll));
        BasicDBObject record = new BasicDBObject(obj);
        coll.insert(record);
    }
    
    public void update(String dbName, String colName, DBObject findObj, Object obj) throws Exception {
        DB db = mongoClient.getDB(dbName);
        DBCollection coll = db.getCollection(colName);
        BasicDBObject record = new BasicDBObject(BeanUtilEx.transBean2Map(obj));
        coll.update(findObj, new BasicDBObject("$set", record), false, true);
    }

    public void update(String dbName, String colName, DBObject findObj, Map map) throws Exception {
        DB db = mongoClient.getDB(dbName);
        DBCollection coll = db.getCollection(colName);
        BasicDBObject record = new BasicDBObject(map);
        coll.update(findObj, new BasicDBObject("$set", record), false, true);
    }

    public void update(String dbName, String colName, DBObject findObj, DBObject record) throws Exception {
        DB db = mongoClient.getDB(dbName);
        DBCollection coll = db.getCollection(colName);
        coll.update(findObj, record, false, true);
    }

    public void update(String dbName, String colName, String findJson, String json) {
        DB db = mongoClient.getDB(dbName);
        DBCollection coll = db.getCollection(colName);
        BasicBSONObject locateObj = (BasicBSONObject)JSON.parse(findJson, new DefaultDBCallback(coll));
        BasicDBObject locate = new BasicDBObject(locateObj);
        BasicBSONObject obj = (BasicBSONObject)JSON.parse(json, new DefaultDBCallback(coll));
        BasicDBObject record = new BasicDBObject(obj);
        coll.update(locate, record, false, true);
    }
    
    public void update(String dbName, String sql, Object obj) throws Exception {
        QueryInfo query = sql2QueryInfo(dbName, sql);
        update(dbName, query.collName, query.query, obj);
    }
    
    public void update(String dbName, String sql) throws Exception {
        QueryInfo query = sql2QueryInfo(dbName, sql);
        update(dbName, query.collName, query.query, (DBObject)(new BasicDBObject("$set", query.updateObj)));
    }
    
    public void remove (String dbName, String sql) throws JSQLParserException {
        QueryInfo query = sql2QueryInfo(dbName, sql);
        DB db = mongoClient.getDB(dbName);
        DBCollection coll = db.getCollection(query.collName);
        coll.remove(query.query);
    }
    
    public void remove(String dbName, String colName, Map query) {
        DB db = mongoClient.getDB(dbName);
        DBCollection coll = db.getCollection(colName);
        coll.remove(new BasicDBObject(query));
    }

    public List find(String dbName, String sql) throws JSQLParserException {
        QueryInfo query = sql2QueryInfo(dbName, sql);
        return find(query);
    }
    
    public Map findOne(String dbName, String sql) throws JSQLParserException {
        QueryInfo query = sql2QueryInfo(dbName, sql);
        return findOne(query);
    }

    public <T> T findOneObj(String dbName, String sql, Class<T> clazz) throws JSQLParserException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Map map = findOne(dbName, sql);
        if (map == null) {
            return null;
        }
        T obj = clazz.newInstance();
        ConvertUtils.register(new DateConverter(null), Date.class);
        BeanUtils.populate(obj, map);
        return obj;
    }

    public Map findOne(QueryInfo queryInfo) {
        DB db = mongoClient.getDB(queryInfo.dbName);
        DBCollection coll = db.getCollection(queryInfo.collName);
        DBObject record = null;
        //if (queryInfo.query != null) {
            record = coll.findOne(queryInfo.query, null, queryInfo.order);
        //} else {
        //    record = coll.findOne();
        //}
        if (record ==  null) {
            return null;
        }
        return record.toMap();
    }

    public List find(QueryInfo queryInfo) {
        DB db = mongoClient.getDB(queryInfo.dbName);
        DBCollection coll = db.getCollection(queryInfo.collName);
        DBCursor cursor = null;
        if (queryInfo.query != null) {
            cursor = coll.find(queryInfo.query);
        } else {
            cursor = coll.find();
        }
        if (queryInfo.order != null) {
            cursor = cursor.sort(queryInfo.order);
        }
        if (queryInfo.limit != null) {
            cursor.limit(queryInfo.limit.intValue());
        }
        if (queryInfo.skip != null) {
            cursor.skip(queryInfo.skip.intValue());
        }
        return cursor2list(cursor);
    }

    public List find(String dbname, String collName, Map queryMap, int start,int limit) {
        DB db = mongoClient.getDB(dbname);
        DBCollection coll = db.getCollection(collName);
        BasicDBObject query = new BasicDBObject(queryMap);
        return find(coll, query,start,limit);
    }

    public List find(String dbname, String collName, Map queryMap,Map orderMap, int start,int limit) {
        DB db = mongoClient.getDB(dbname);
        DBCollection coll = db.getCollection(collName);
        BasicDBObject query = new BasicDBObject(queryMap);
        BasicDBObject order = new BasicDBObject(orderMap);
        return find(coll, query,order,start,limit);
    }

    public List find(String dbname, String collName, Map queryMap) {
        DB db = mongoClient.getDB(dbname);
        DBCollection coll = db.getCollection(collName);
        BasicDBObject query = new BasicDBObject(queryMap);
        return find(coll, query);
    }
    
    public List<Map> findAll(String dbname, String collName) {
        DB db = mongoClient.getDB(dbname);
        DBCollection coll = db.getCollection(collName);
        return find(coll, null);
    }
    public long count(String dbname, String collName,Map queryMap){
        DB db = mongoClient.getDB(dbname);
        DBCollection coll = db.getCollection(collName);
        long count;
        if (queryMap == null) {
            count = coll.count();
        } else {
            BasicDBObject query = new BasicDBObject(queryMap);
            count = coll.count(query);
        }
        return count;
    }
    public long count(String dbname, String collName){
        return count(dbname, collName, null);
    }
    public <T> T findOneObj(String dbName, String collName, Map queryMap, Class<T> clazz) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        DB db = mongoClient.getDB(dbName);
        DBCollection coll = db.getCollection(collName);
        BasicDBObject query = new BasicDBObject(queryMap);
        DBObject map = coll.findOne(query);
        if (map == null) {
            return null;
        }
        T obj = clazz.newInstance();
        ConvertUtils.register(new DateConverter(null), Date.class);
        BeanUtils.populate(obj, map.toMap());
        return obj;
    }
    
    public Map findOne(String dbName, String collName, Map queryMap) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        DB db = mongoClient.getDB(dbName);
        DBCollection coll = db.getCollection(collName);
        BasicDBObject query = new BasicDBObject(queryMap);
        DBObject map = coll.findOne(query);
        if (map == null) {
            return  null;
        }
        return map.toMap();
    }

    public List<Map> find(DBCollection coll, DBObject query) {
        DBCursor cursor = null;
        if (query == null) {
            cursor = coll.find();
        } else {
            cursor = coll.find(query);
        }
        return cursor2list(cursor);
    }
    public List<Map> find(DBCollection coll, DBObject query, int start,int limit) {
        DBCursor cursor = null;
        if (query == null) {
            cursor = coll.find();
        } else if(start == 0) {
            cursor = coll.find(query).limit(limit);
        } else {
            cursor = coll.find(query).skip(start).limit(limit);
        }
        return cursor2list(cursor);
    }

    public List<Map> find(DBCollection coll, DBObject query, DBObject order, int start,int limit) {
        DBCursor cursor = null;
        if (query == null) {
            cursor = coll.find();
        } else {
            cursor = coll.find(query);
        }

        if (order != null) cursor=cursor.sort(order);

        if(start != 0 && limit != 0){
            cursor.skip(start).limit(limit);
        }
        if(start == 0 && limit !=0 ){
            cursor.limit(limit);
        }

        return cursor2list(cursor);
    }

    private List<Map> cursor2list(DBCursor cursor) {
        List<Map> list = new ArrayList();
        try {
            while (cursor.hasNext()) {
                DBObject obj = cursor.next();
                list.add(obj.toMap());
            }
        } finally {
            cursor.close();
        }
        return list;
    }
    
    public static Pattern dateP = Pattern.compile("^[12][09][0-9][0-9]\\-[0-9][0-9]\\-[0-9][0-9]$");
    public static Pattern timeP = Pattern.compile("^[12][09][0-9][0-9]\\-[0-9][0-9]\\-[0-9][0-9]\\s[0-9][0-9]\\:[0-9][0-9]\\:[0-9][0-9]$");
    
    public QueryInfo sql2QueryInfo(String dbName, String sql) throws JSQLParserException {
        QueryInfo queryInfo = new QueryInfo();
        queryInfo.dbName = dbName;
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Statement statement = parserManager.parse(new StringReader(sql));
        Expression whereExpression = null;
        List<OrderByElement> orderList = null;
        if (statement instanceof Select) {
            queryInfo.action = "select";
            Select select = (Select)statement;
            PlainSelect selectBody = (PlainSelect) select.getSelectBody();
            whereExpression = selectBody.getWhere();
            queryInfo.collName = selectBody.getFromItem().toString();
            orderList = selectBody.getOrderByElements();
            Limit limit = selectBody.getLimit();
            //Long limit = selectBody.getLimit().getRowCount();
            if (null != limit) {
                queryInfo.skip = limit.getOffset();
                queryInfo.limit = limit.getRowCount();
            }
        } else if (statement instanceof Delete) {
            queryInfo.action = "delete";
            Delete delete = (Delete) statement;
            whereExpression = delete.getWhere();
            queryInfo.collName = delete.getTable().getName();
        } else if (statement instanceof Update) {
            //throw new RuntimeException("update 暂时不支持");
            queryInfo.action = "update";
            Update update = (Update)statement;
            whereExpression = update.getWhere();
            queryInfo.collName = update.getTable().getName();
            List<Column> columnList = update.getColumns();
            List<Expression> expressionList = update.getExpressions();
            queryInfo.updateObj = new BasicDBObject();
            for (int i = 0; i < columnList.size(); i++) {
                String v = expressionList.get(i).toString();
                String columnName = columnList.get(i).getColumnName();
                if (v.startsWith("'") && v.endsWith("'")) {
                    String v1 = v.substring(1, v.length() - 1);
                    try {
                        if (dateP.matcher(v1).find()) {
                            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                            queryInfo.updateObj.put(columnList.get(i).getColumnName(), df.parse(v1));
                        } else if (timeP.matcher(v1).find()) {
                            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            queryInfo.updateObj.put(columnList.get(i).getColumnName(), df.parse(v1));
                        } else {
                            queryInfo.updateObj.put(columnList.get(i).getColumnName(), v1);
                        }
                    } catch (ParseException e) {
                        throw new JSQLParserException("不正确的日期格式：yyyy-MM-dd 或 yyyy-MM-dd HH:mm:ss value:" + v1, e);
                    }
                } else {
                    if (v.trim().equalsIgnoreCase("true") || v.trim().equalsIgnoreCase("false")) {
                        queryInfo.updateObj.put(columnName, BooleanUtils.toBooleanObject(v.trim()));
                    } else if (v.contains(".")) {
                        try {
                            Double dv = Double.valueOf(v);
                            queryInfo.updateObj.put(columnName, dv);
                        } catch (Exception ee) {
                            queryInfo.updateObj.put(columnName, v);
                        }
                    } else {
                        try {
                            Integer iv = Integer.valueOf(v.trim());
                            queryInfo.updateObj.put(columnName, iv);
                        } catch (Exception e) {
                            queryInfo.updateObj.put(columnName, v);
                        }
                    }
                }
            }
        } else if (statement instanceof Insert) {
            throw new RuntimeException("insert 不支持");
        } else {
            throw new JSQLParserException("不支持的sql语句:" + sql);
        }
        if (whereExpression != null) {
            Sql2MongoExpressVisitor visitor = new Sql2MongoExpressVisitor();
            whereExpression.accept(visitor);
            queryInfo.query = visitor.getQuery();
        }
        if (orderList != null) {
           queryInfo.order = new BasicDBObject();
            for (OrderByElement ele : orderList) {
                queryInfo.order.put(ele.getExpression().toString(), ele.isAsc()?1:-1);
            }
        }
        
        
//        System.out.println(queryInfo.debugStr());
        return queryInfo;
    }
    
    /**
     * 上传文件到mongodb
     * @param dbName  数据库名称
     * @param fsName GridFS名称，缺省是fs，如果是null，就用缺省的。
     * @param file 要上传的文件。
     * @param fsFileName 在GridFS中的文件名，如果是null，就是file的名称（不带路径）。
     * @throws FileNotFoundException
     * @throws IOException 
     */
    public void upload2GridFS(String dbName, String fsName, File file, String fsFileName) throws FileNotFoundException, IOException {
        if (fsFileName == null) {
            fsFileName = file.getName();
        }
        InputStream input = new FileInputStream(file);    
        upload2GridFS(dbName, fsName, input, fsFileName);
        input.close();
    }
    
    /**
     * 上传文件到mongodb
     * @param dbName 数据库名称
     * @param fsName  GridFS名称，缺省是fs，如果是null，就用缺省的。
     * @param input 文件数据流
     * @param fsFileName 在GridFS中的文件名
     * @throws FileNotFoundException
     * @throws IOException 
     */
    public void upload2GridFS(String dbName, String fsName, InputStream input, String fsFileName) throws FileNotFoundException, IOException {
        DB db = mongoClient.getDB(dbName);
        if (fsName == null) {
            fsName = "fs";
        }
        GridFS fs = new GridFS(db, fsName);
        GridFSInputFile fsFile = fs.createFile(input);
        if (StringUtils.isBlank(fsFileName)) {
            throw new FileNotFoundException("gridfs中文件名不能为空。请检查参数fsFileName");
        }
        fsFile.setFilename(fsFileName);
        fsFile.save();
    }
    
    public void upload2GridFS(String dbName, String fsName, byte[] bytes, String fsFileName) throws FileNotFoundException, IOException {
        DB db = mongoClient.getDB(dbName);
        if (fsName == null) {
            fsName = "fs";
        }
        GridFS fs = new GridFS(db, fsName);
        GridFSInputFile fsFile = fs.createFile(bytes);
        if (StringUtils.isBlank(fsFileName)) {
            throw new FileNotFoundException("gridfs中文件名不能为空。请检查参数fsFileName");
        }
        fsFile.setFilename(fsFileName);
        fsFile.save();
    }
    
    public void upload2GridFS(String dbName, String fsName, String content, String fsFileName) throws FileNotFoundException, IOException {
        upload2GridFS(dbName, fsName, content.getBytes("utf-8"), fsFileName);
    }
    /**
     * 从GridFS中下载文件
     * @param dbName 数据库名称
     * @param fsName GridFS名称，缺省是fs，如果是null，就用缺省的。
     * @param fsFileName 在GridFS中的文件名
     * @param fileName 保存下载的文件名，如果为空，就以fsFileName作为文件名。如果是目录，就以fsFileName作为文件名放到此目录中
     * @return 下载下来的本地文件名
     * @throws FileNotFoundException
     * @throws IOException 
     */
    public File downloadFsFile(String dbName, String fsName, String fsFileName, String fileName) throws FileNotFoundException, IOException {
        if (StringUtils.isBlank(fileName)) {
            fileName = fsFileName;
        }
        File saveFile = new File(fileName);
        if (saveFile.isDirectory()) {
            fileName = saveFile.getPath() + "/" + fsFileName;
        }
        DB db = mongoClient.getDB(dbName);
        if (fsName == null) {
            fsName = "fs";
        }
        GridFS fs = new GridFS(db, fsName);
        GridFSDBFile gfile = fs.findOne(fsFileName);
        if (gfile == null) {
            throw new FileNotFoundException("gridfs中没有文件：" + fsFileName);
        }
        InputStream input = gfile.getInputStream();
        try {
            File f = new File(fileName);
            OutputStream output = new FileOutputStream(f);
            byte[] bytes = new byte[1024];
            int read = 0;
            while ((read = input.read(bytes)) != -1) {
                output.write(bytes, 0, read);
            }
            output.flush();
            output.close();
            return f;
        } finally {
            input.close();
        }
        
    }
    /**
     * 从GridFS中删除文件
     * @param dbName 数据库名称
     * @param fsName GridFS名称，缺省是fs，如果是null，就用缺省的。
     * @param queryMap GridFS文件的检索条件
     */
    public void removeFsFile(String dbName,String fsName, Map queryMap){
        DB db = mongoClient.getDB(dbName);
        if (fsName == null) {
            fsName = "fs";
        }
        GridFS fs = new GridFS(db,fsName);
        DBObject query = new BasicDBObject(queryMap);
        fs.remove(query);
    }

    public byte[] getFsFileBytes(String dbName, String fsName, String fsFileName) throws FileNotFoundException, IOException {
        DB db = mongoClient.getDB(dbName);
        if (fsName == null) {
            fsName = "fs";
        }
        GridFS fs = new GridFS(db, fsName);
        GridFSDBFile gfile = fs.findOne(fsFileName);
        if (gfile == null) {
            throw new FileNotFoundException("gridfs中没有文件：" + fsFileName);
        }
        InputStream input = gfile.getInputStream();
        try {
            byte[] b = new byte[(int) gfile.getLength()];
            int readCount = 0;
            while (true) {
                int count = input.read(b, readCount, 255);
                if (count <= 0) {
                    break;
                }
                readCount += count;
            }
            return b;
        } finally {
            input.close();
        }
        
    }
    
    
    public static void main(String[] args) throws Exception {
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        DB db = mongoClient.getDB("test");
        DBCollection coll = db.getCollection("rztest");
        String sql = "select * from table1 where name='aaa' and age>=18 and foot in (1,2,3) and abc in ('a1','a2','a3') order by age,name limit 123";
        MongoDbOperater ope = new MongoDbOperater();
        ope.setMongoClient(mongoClient);
        QueryInfo info = ope.sql2QueryInfo("abc", sql);
        System.out.println(info.debugStr());

        sql = "select * from algoflow_instance_log where functionName='f1' and reuseResult=false and returnCode=0 and action='LEAVE' order by timestamp desc limit 100";
        info = ope.sql2QueryInfo("abc", sql);
        System.out.println(info.debugStr());

        String updateSql = "update table1 set a=1, b='sdaf', c='2012-11-23 17:45:32' where name='aaa' and age>=18 and foot in (1,2,3) and abc in ('a1','a2','a3')";
        QueryInfo uinfo = ope.sql2QueryInfo("abc", updateSql);
        System.out.println(uinfo.debugStr());

        String json = "{\"a\":123,\"b\":345}";
        BasicBSONObject obj = (BasicBSONObject)JSON.parse(json, new DefaultDBCallback(coll));
        BasicDBObject dbObj = new BasicDBObject(obj);
        System.out.println(dbObj);
    }
    

}
