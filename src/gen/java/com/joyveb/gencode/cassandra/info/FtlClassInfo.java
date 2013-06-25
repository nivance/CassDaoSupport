package com.joyveb.gencode.cassandra.info;

import lombok.Data;

@Data
public class FtlClassInfo {
	String domainObject;
	String domainClass;
	String packageName;
}
