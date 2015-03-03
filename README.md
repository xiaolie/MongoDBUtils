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
  \<bean id="ope" class="com.richeninfo.mongodbutils.MongoDbOperater"\> <br>
<br>  
特性：<br>  
1，支持sql语法的增删改查<br>  
2，支持原生mongodb操作<br>  
3，自动转换bean<br>  
4，支持GridFS操作<br>  

使用（假设数据库名为 db1）：
String sql = "select * from colName where name='tom' and age>15 and score between (80, 100) and city in ("shanghai", "beijing", "guangzhou");<br>
Persion persion = ope.findOneObj("db1", sql, Persion.class);<br>
List<Persion> persionList = ope.findAllObj("db1", sql, Persion.class); <br>
...
支持的sql样例:<br>
sql = "select * from table1 where name='aaa' and age>=18 and foot in (1,2,3) and abc in ('a1','a2','a3') order by age,name limit 123"; <br>
sql = "select * from algoflow_instance_log where functionName='f1' and reuseResult=false and returnCode=0 and action='LEAVE' order by timestamp desc limit 100";<br>

更新：<br>
sql = "update table1 set a=1, b='sdaf', c='2012-11-23 17:45:32' where name='aaa' and age>=18 and foot in (1,2,3) and abc in ('a1','a2','a3')";，<br>
ope.update("db1", sql);<br>
sql = "delete from table1 where name='aaa' and age>=18 and foot in (1,2,3) and abc in ('a1','a2','a3') order by age,name limit 123"; <br>
ope.remove("db1", sql);<br>
以上sql都可以执行：QueryInfo uinfo = ope.sql2QueryInfo("db1", sql);
