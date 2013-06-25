package ${packageName}.dao.impl;

import lombok.extern.slf4j.Slf4j;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;

import com.joyveb.support.cassandra.CassandraDaoSupport;
import ${packageName}.dao.${domainClazz}Dao;
import ${packageName}.domain.${domainClazz};


@Slf4j
public class ${domainClazz}DaoImpl extends CassandraDaoSupport<String, ${domainClazz}> implements
		${domainClazz}Dao {

	public ${domainClazz}DaoImpl(Keyspace keyspace, String columnFamilyName) {
		super(keyspace, columnFamilyName, StringSerializer.get());
	}

	@Override
	protected ${domainClazz} createPOJO() {
		return new ${domainClazz}();
	}

}
