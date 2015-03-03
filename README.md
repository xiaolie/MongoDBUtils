# MongoDBUtils
MongoDB  access and operate tools, suport use sql statement to ope mongodb (select, update, insert, delete)

普通构造<br>
MongoDbOperater ope = new MongoDbOperater();<br>
ope.setMongoClient(client);
<br>
spring 构造<br>
  \<bean id="mongoClient" class="com.mongodb.MongoClient"\\> <br>
  ... <br>
  \<\\bean\> <br>
  \<bean id="mongoDbOperation" class="com.richeninfo.mongodbutils.MongoDbOperater"\> <br>
<br>  
使用：
String sql = "select * from colName where name='tom' and age>15 and score between (80, 100) and city in ("shanghai", "beijing", "guangzhou");<br>
Persion persion = ope.findOneObj(dbName, sql, Persion.class);<br>
List<Persion> persionList = ope.findAllObj(dbName, sql, Persion.class); <br>
...
