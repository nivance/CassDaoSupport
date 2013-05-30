package com.joyveb.support.cassandra;

import org.apache.commons.lang3.StringUtils;

public class ExampleByCQL {

	private StringBuffer whereStmt = new StringBuffer();// where statement
	private final String splitter = " ";

	/**
	 * 增加>=表达式
	 * 
	 * @param <V>
	 * @param columnName
	 * @param value
	 * @return
	 */
	public <V> ExampleByCQL addGteExpress(String columnName, V value) {
		this.new GteExpression<V>(columnName, value).appendExpression();
		return this;
	}

	/**
	 * 增加>表达式
	 * 
	 * @param <V>
	 * @param columnName
	 * @param value
	 * @return
	 */
	public <V> ExampleByCQL addGtExpress(String columnName, V value) {
		this.new GtExpression<V>(columnName, value).appendExpression();
		return this;
	}

	/**
	 * 增加=表达式
	 * 
	 * @param <V>
	 * @param columnName
	 * @param value
	 * @return
	 */
	public <V> ExampleByCQL addEqExpress(String columnName, V value) {
		this.new EqualsExpression<V>(columnName, value).appendExpression();
		return this;
	}

	/**
	 * 增加<=表达式
	 * 
	 * @param <V>
	 * @param columnName
	 * @param value
	 * @return
	 */
	public <V> ExampleByCQL addLteExpress(String columnName, V value) {
		this.new LteExpression<V>(columnName, value).appendExpression();
		return this;
	}

	/**
	 * 增加<表达式
	 * 
	 * @param <V>
	 * @param columnName
	 * @param value
	 * @return
	 */
	public <V> ExampleByCQL addLtExpress(String columnName, V value) {
		this.new LtExpression<V>(columnName, value).appendExpression();
		return this;
	}

	/**
	 * 增加in表达式
	 * 
	 * @param <V>
	 * @param columnName
	 * @param value
	 * @return
	 */
	public <V> ExampleByCQL addInExpress(String columnName, V... value) {
		this.new InExpression<V>(columnName, value).appendExpression();
		return this;
	}

	public String toString() {
		int length = whereStmt.length();
		return length > 0 ? whereStmt.substring(0, length - 4) : "";// delete
																	// tail AND
	}

	/**
	 * 条件表达式
	 */
	public abstract class Expression<V> {
		private String fieldName;
		private V[] value;

		private Expression(String fieldName, V... value) {
			super();
			this.fieldName = fieldName;
			this.value = value;
		}

		protected void appendExpression() {
			String columnName = StringUtils.upperCase(fieldName);
			if (value != null) {
				add(columnName, value);
				whereStmt.append(splitter).append("AND").append(splitter);
			}
		}

		protected abstract void add(String columnName, V... value);
	}

	public class GteExpression<V> extends Expression<V> {
		@SuppressWarnings("unchecked")
		public GteExpression(String fieldName, V value) {
			super(fieldName, value);
		}

		@Override
		protected void add(String columnName, V... value) {
			whereStmt.append(columnName).append(" >= ").append(value[0]);
		}

	}

	public class LteExpression<V> extends Expression<V> {
		@SuppressWarnings("unchecked")
		public LteExpression(String fieldName, V value) {
			super(fieldName, value);
		}

		@Override
		protected void add(String columnName, V... value) {
			whereStmt.append(columnName).append(" <= ").append(value[0]);
		}
	}

	public class GtExpression<V> extends Expression<V> {
		@SuppressWarnings("unchecked")
		public GtExpression(String fieldName, V value) {
			super(fieldName, value);
		}

		@Override
		protected void add(String columnName, V... value) {
			whereStmt.append(columnName).append(" > ").append(value[0]);
		}
	}

	public class LtExpression<V> extends Expression<V> {
		@SuppressWarnings("unchecked")
		public LtExpression(String fieldName, V value) {
			super(fieldName, value);
		}

		@Override
		protected void add(String columnName, V... value) {
			whereStmt.append(columnName).append(" < ").append(value[0]);
		}
	}

	public class EqualsExpression<V> extends Expression<V> {
		@SuppressWarnings("unchecked")
		public EqualsExpression(String fieldName, V value) {
			super(fieldName, value);
		}

		@Override
		protected void add(String columnName, V... value) {
			whereStmt.append(columnName);
			if (value[0] instanceof String) {
				whereStmt.append(" = '").append(value[0]).append("'");
			} else {
				whereStmt.append(" = ").append(value[0]);
			}
		}
	}

	/**
	 *	貌似暂不支持in操作
	 * @param <V>
	 */
	public class InExpression<V> extends Expression<V> {
		public InExpression(String fieldName, V... values) {
			super(fieldName, values);
		}

		@Override
		protected void add(String columnName, V... values) {
			whereStmt.append(columnName).append(" IN (");
			for (V v : values) {
				if (v instanceof String) {
					whereStmt.append("'").append(v).append("'");
				} else {
					whereStmt.append(v);
				}
				whereStmt.append(",");
			}
			whereStmt.deleteCharAt(whereStmt.length() - 1).append(")");
		}
	}
}
