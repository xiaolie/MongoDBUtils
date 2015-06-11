/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.shenjitang.mongodbutils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
/**
 *
 * @author xiaolie
 */
public class Sql2MongoExpressVisitor extends ExpressionVisitorAdapter {
    private DBObject query = new BasicDBObject();
    private DBObject currentObj;
//    private String currentColumn;

    public Sql2MongoExpressVisitor() {
    }

    public DBObject getQuery() {
        return query;
    }

    public void setQuery(DBObject query) {
        this.query = query;
    }
    
    private void toMongoQuery(Expression left, Expression right) {
        if (right instanceof Column) {
            String rightV = ((Column) right).getColumnName();
            if (rightV.equalsIgnoreCase("true")) {
                query.put(left.toString(), Boolean.TRUE);
            } else if (rightV.equalsIgnoreCase("false")) {
                query.put(left.toString(), Boolean.FALSE);
            } else {
                throw new RuntimeException("不能识别的表达式：" + right.toString());
            }
        } else {
            try {
                Method getValueMethod = right.getClass().getMethod("getValue");
                if (getValueMethod != null) {
                    Object value = getValueMethod.invoke(right);
                    query.put(left.toString(), value);
                }
            } catch (Exception e) {
                throw new RuntimeException("表达式值没有value", e);
            }
        }
    }

    private void toMongoQuery(Expression left, Object right, String opt) {
        try {
            Method getValueMethod = right.getClass().getMethod("getValue");
            if (getValueMethod != null) {
                Object value = getValueMethod.invoke(right);
                String field = left.toString();
                if (query.containsField(field)) {
                    ((DBObject)query.get(field)).put(opt, value);
                } else {
                    DBObject optObj = new BasicDBObject(opt, value);
                    query.put(field, optObj);
                }
            } else {
                String field = left.toString();
                if (query.containsField(field)) {
                    ((DBObject)query.get(field)).put(opt, right);
                } else {
                    DBObject optObj = new BasicDBObject(opt, right);
                    query.put(field, optObj);
                }
            }
        } catch (Exception e) {
            String field = left.toString();
            if (query.containsField(field)) {
                ((DBObject)query.get(field)).put(opt, right);
            } else {
                DBObject optObj = new BasicDBObject(opt, right);
                query.put(field, optObj);
            }
            //throw new RuntimeException("表达式值没有value", e);
        }
    }

    @Override
    public void visit(EqualsTo expr) {  
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        toMongoQuery(left, right);
        visitBinaryExpression(expr);
    }
    

    @Override
    public void visit(GreaterThan expr) {
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        toMongoQuery(left, right, "$gt");
        visitBinaryExpression(expr);
    }

    @Override
    public void visit(Between expr) {
        Expression left = expr.getLeftExpression();
        Expression start = expr.getBetweenExpressionStart();
        Expression end = expr.getBetweenExpressionEnd();
        toMongoQuery(left, start, "$gte");
        toMongoQuery(left, end, "$lte");
    }
    
    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        Expression left = greaterThanEquals.getLeftExpression();
        Expression right = greaterThanEquals.getRightExpression();
        toMongoQuery(left, right, "$gte");
        visitBinaryExpression(greaterThanEquals);
	}

    @Override
    public void visit(MinorThan expr) {
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        toMongoQuery(left, right, "$lt");
        visitBinaryExpression(expr);
	}
    
    @Override
    public void visit(MinorThanEquals expr) {
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        toMongoQuery(left, right, "$lte");
		visitBinaryExpression(expr);
	}
    
    @Override
    public void visit(InExpression expr) {
        Expression left = expr.getLeftExpression();
        ItemsList list = expr.getRightItemsList();
        String listStr = list.toString();
        String str = listStr.substring(listStr.indexOf("(") + 1, listStr.lastIndexOf(")"));
        String[] items = str.split(",");
        trimQuotation(items);
        toMongoQuery(left, items, "$in");
//		expr.getLeftExpression().accept(this);
//		expr.getRightItemsList().accept(this);
	}
    
    private void trimQuotation(String[] items) {
        for (int i = 0; i < items.length; i++) {
            String item = items[i].trim();
            if (item.startsWith("'") && item.endsWith("'")) {
                item = item.substring(1, item.length() - 1);
                items[i] = item;
            }
        }
    }

}
