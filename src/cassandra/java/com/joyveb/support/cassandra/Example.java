package com.joyveb.support.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hom.HectorObjectMapper;

import org.apache.commons.lang3.StringUtils;
/**
 * 条件表达式
 * K表的主键类型
 * V表对应column的值的类型
 */
public class Example<K> {

	private List<Expression<?>> expressions = new ArrayList<Expression<?>>();
	
	/**
	 * 增加>=表达式
	 * @param <V>
	 * @param columnName
	 * @param value
	 * @return
	 */
	public <V> Example<K> addGteExpress(String columnName, V value){
		expressions.add(this.new GteExpression<V>(columnName, value));
		return this;
	}
	
	/**
	 * 增加>表达式
	 * @param <V>
	 * @param columnName
	 * @param value
	 * @return
	 */
	public <V> Example<K> addGtExpress(String columnName, V value){
		expressions.add(this.new GtExpression<V>(columnName, value));
		return this;
	}
	
	/**
	 * 增加=表达式
	 * @param <V>
	 * @param columnName
	 * @param value
	 * @return
	 */
	public <V> Example<K> addEqExpress(String columnName, V value){
		expressions.add(this.new EqualsExpression<V>(columnName, value));
		return this;
	}
	
	/**
	 * 增加<=表达式
	 * @param <V>
	 * @param columnName
	 * @param value
	 * @return
	 */
	public <V> Example<K> addLteExpress(String columnName, V value){
		expressions.add(this.new LteExpression<V>(columnName, value));
		return this;
	}
	
	/**
	 * 增加<表达式
	 * @param <V>
	 * @param columnName
	 * @param value
	 * @return
	 */
	public <V> Example<K> addLtExpress(String columnName, V value){
		expressions.add(this.new LtExpression<V>(columnName, value));
		return this;
	}
	
	public void appendExp2Query(
			RangeSlicesQuery<K, String, ByteBuffer> rangeSlicesQuery) {
		for(Expression<?> expression : expressions){
			expression.addExpression(rangeSlicesQuery);
		}
	}
	
	/**
	 * 条件表达式
	 */
	public abstract class Expression<V>{
		
		private String fieldName;
		
		private V value;
		
		protected Expression(String fieldName, V value) {
			super();
			this.fieldName = fieldName;
			this.value = value;
		}

		private void addExpression(RangeSlicesQuery<K, String, ByteBuffer> rangeSlicesQuery) {
			String columnName = StringUtils.upperCase(fieldName);
			if(value !=null){
				Serializer serializer = HectorObjectMapper.determineSerializer(value.getClass());
				add(rangeSlicesQuery, columnName, serializer.toByteBuffer(value));
			}
		}
		
		protected abstract void add(RangeSlicesQuery<K, String, ByteBuffer> rangeSlicesQuery, String columnName, ByteBuffer value);
	}
	
	/**
	 *	>=
	 * @param <V>
	 */
	public class GteExpression<V> extends Expression<V>{

		public GteExpression(String fieldName, V value) {
			super(fieldName, value);
		}

		@Override
		protected void add(RangeSlicesQuery<K, String, ByteBuffer> rangeSlicesQuery, String columnName, ByteBuffer value) {
			rangeSlicesQuery.addGteExpression(columnName, value );
		}
	}
	
	/**
	 *	<=
	 * @param <V>
	 */
	public class LteExpression<V> extends Expression<V>{
		public LteExpression(String fieldName, V value) {
			super(fieldName, value);
		}

		@Override
		protected void add(RangeSlicesQuery<K, String, ByteBuffer> rangeSlicesQuery, String columnName, ByteBuffer value) {
			rangeSlicesQuery.addLteExpression(columnName, value );
		}
	}
	
	/**
	 *	>
	 * @param <V>
	 */
	public class GtExpression<V> extends Expression<V>{
		@Override
		protected void add(RangeSlicesQuery<K, String, ByteBuffer> rangeSlicesQuery, String columnName, ByteBuffer value) {
			rangeSlicesQuery.addGtExpression(columnName, value );
		}

		public GtExpression(String fieldName, V value) {
			super(fieldName, value);
		}
	}
	
	/**
	 *	<
	 * @param <V>
	 */
	public class LtExpression<V> extends Expression<V>{
		@Override
		protected void add(RangeSlicesQuery<K, String, ByteBuffer> rangeSlicesQuery, String columnName, ByteBuffer value) {
			rangeSlicesQuery.addLtExpression(columnName, value );
		}

		public LtExpression(String fieldName, V value) {
			super(fieldName, value);
		}
	}
	
	/**
	 *	=
	 * @param <V>
	 */
	public class EqualsExpression<V> extends Expression<V>{
		@Override
		protected void add(RangeSlicesQuery<K, String, ByteBuffer> rangeSlicesQuery, String columnName, ByteBuffer value) {
			rangeSlicesQuery.addEqualsExpression(columnName, value );
		}

		public EqualsExpression(String fieldName, V value) {
			super(fieldName, value);
		}
	}
}
