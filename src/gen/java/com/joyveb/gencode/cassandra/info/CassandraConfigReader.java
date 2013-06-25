package com.joyveb.gencode.cassandra.info;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.XPath;
import org.dom4j.io.SAXReader;

public final class CassandraConfigReader {
	
	private final static CassandraConfigReader instance = new CassandraConfigReader();
	
	private CassandraConfigReader(){};
	
	public static CassandraConfigReader getInstance(){
		return instance;
	}

	public KeySpaceContext read(String xmlLocation) throws DocumentException {
		Document doc = this.getDoc(xmlLocation);
		return parseDoc(doc);
	}

	private KeySpaceContext parseDoc(Document doc) {
		String projectName = doc.selectSingleNode(
				"/generatorConfiguration/context/@id").getText();
		String host = doc.selectSingleNode(
				"/generatorConfiguration/context/host").getText();
		String cluster = doc.selectSingleNode(
				"/generatorConfiguration/context/cluster").getText();
		String keyspace = doc.selectSingleNode(
				"/generatorConfiguration/context/keyspace").getText();
		String targetSource = doc.selectSingleNode(
				"/generatorConfiguration/context/targetSource").getText();
		String targetPackage = doc.selectSingleNode(
				"/generatorConfiguration/context/targetPackage").getText();
		XPath path = doc
				.createXPath("/generatorConfiguration/context/ColumnFamilys/ColumnFamily");
		Map<String, String> cfs = new HashMap<String, String>();
		List<Element> games = path.selectNodes(doc);
		for (Element el : games) {
			String name = el.element("name").getText();
			String domainObjectName = el.element("domainObjectName").getText();
			cfs.put(name, domainObjectName);
		}
		KeySpaceContext context = new KeySpaceContext(projectName, host,
				cluster, keyspace, targetSource, targetPackage, cfs);
		return context;
	}

	private Document getDoc(String xmlLocation) throws DocumentException {
		SAXReader reader = new SAXReader();
		File file = new File(xmlLocation);
		System.out.println(file.getAbsolutePath());
		Document doc = reader.read(file);
		doc.normalize();
		return doc;
	}

//	public static void main(String[] args) {
//		CassandraConfigReader reader = new CassandraConfigReader();
//		try {
//			reader.read("cass-genConfig.xml");
//		} catch (DocumentException e) {
//			e.printStackTrace();
//		}
//
//	}
}
