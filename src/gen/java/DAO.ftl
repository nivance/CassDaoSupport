package ${packageName}.dao;

import java.util.List;

import com.joyveb.support.cassandra.BaseCassandraDao;
import com.joyveb.support.cassandra.CassandraList;
import com.joyveb.support.cassandra.Example;
import com.joyveb.support.cassandra.ExampleByCQL;

import ${packageName}.domain.${domainClazz};

public interface ${domainClazz}Dao extends BaseCassandraDao<String, ${domainClazz}> {

	public void insert(${domainClazz} ${domainObject});
	
	public void batchInsert(List<${domainClazz}> list);

	public void updateByPrimaryKey(${domainClazz} ${domainObject});
	
	public void batchUpdate(List<${domainClazz}> list);
	
	public void delete(String key);

	public ${domainClazz} selectByPrimaryKey(String _key);

	public List<${domainClazz}> selectByCQL(String cql);

	public List<${domainClazz}> selectByExample(Example<String> example);
	
	public List<${domainClazz}> selectByExampleWithCQL(ExampleByCQL example);
	
	public CassandraList<String, ${domainClazz}> findByPages(Example<String> example,
			int pagesize, String startKey, String endKey);
	
}



