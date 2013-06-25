package com.joyveb.gencode.cassandra;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.commons.lang.StringUtils;
import org.dom4j.DocumentException;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.joyveb.gencode.cassandra.info.CassandraConfigReader;
import com.joyveb.gencode.cassandra.info.FtlClassInfo;
import com.joyveb.gencode.cassandra.info.KeySpaceContext;

import freemarker.template.DefaultObjectWrapper;
import freemarker.template.Template;

public class GoGenerate {
	private static String CLUSTER = null;
	private static String KEYSPACE_NAME = null;
	private static String HOST_CONFIG = null;
	private static Cluster cluster;
	static StringSerializer ss = StringSerializer.get();
	private static BiMap<String, String> dataTypeMap = HashBiMap.create();
	
	static{
		dataTypeMap.put(Boolean.class.getCanonicalName(), BooleanType.class.getCanonicalName());
		dataTypeMap.put(ByteBuffer.class.getCanonicalName(), BytesType.class.getCanonicalName());
		dataTypeMap.put(Date.class.getCanonicalName(), DateType.class.getCanonicalName());
		dataTypeMap.put(BigDecimal.class.getCanonicalName(), DecimalType.class.getCanonicalName());
		dataTypeMap.put(Double.class.getCanonicalName(), DoubleType.class.getCanonicalName());
		dataTypeMap.put(Void.class.getCanonicalName(), EmptyType.class.getCanonicalName());
		dataTypeMap.put(Float.class.getCanonicalName(), FloatType.class.getCanonicalName());
		dataTypeMap.put(Integer.class.getCanonicalName(), Int32Type.class.getCanonicalName());
		dataTypeMap.put(Long.class.getCanonicalName(), LongType.class.getCanonicalName());
		dataTypeMap.put(InetAddress.class.getCanonicalName(), InetAddressType.class.getCanonicalName());
		dataTypeMap.put(BigInteger.class.getCanonicalName(), IntegerType.class.getCanonicalName());
		dataTypeMap.put(String.class.getCanonicalName(), UTF8Type.class.getCanonicalName());
		dataTypeMap.put(UUID.class.getCanonicalName(), LexicalUUIDType.class.getCanonicalName());
		dataTypeMap.put(List.class.getCanonicalName(), ListType.class.getCanonicalName());
		dataTypeMap.put(Map.class.getCanonicalName(), MapType.class.getCanonicalName());
		dataTypeMap.put(Set.class.getCanonicalName(), SetType.class.getCanonicalName());
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length < 1){
			System.out.println("Usage: GoGenerate Cassandra-genInfo.xml");
			System.exit(0);
		}
		CassandraConfigReader reader = CassandraConfigReader.getInstance();
		for(String cassConfig: args){
			try {
				KeySpaceContext ksc = reader.read(cassConfig);
				initCassandra(ksc);
				gotoGenerateCode(ksc);
			} catch (DocumentException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void initCassandra(KeySpaceContext ksc){
		HOST_CONFIG = ksc.getHost();
		CLUSTER = ksc.getCluster();
		KEYSPACE_NAME = ksc.getKeyspace();
		cluster = HFactory.getOrCreateCluster(CLUSTER,	HOST_CONFIG);
	}
	
	private static void gotoGenerateCode(KeySpaceContext ksc) {
		String projectName = ksc.getProjectName();
		String targetSource = ksc.getTargetSource();
		String targetPackage = ksc.getTargetPackage();
		
		KeyspaceDefinition kspDef = cluster.describeKeyspace(KEYSPACE_NAME);
		List<ColumnFamilyDefinition> cfds = kspDef.getCfDefs();
		for (ColumnFamilyDefinition cfd : cfds) {
			if(ksc.getCfs().containsKey(cfd.getName())){//只处理配置cf
				List<ColumnDefinition> cds = cfd.getColumnMetadata();
				String objectName = ksc.getCfs().get(cfd.getName());
				String content = getFormattedDomainJavaFile(targetPackage, objectName, cds);
				if(StringUtils.isNotBlank(content)){
					File directory = getDirectory(projectName, targetSource, targetPackage, "domain");
					File targetFile = new File(directory, objectName + ".java");
					try {
						writeFile(targetFile, content);
						FtlClassInfo ftlClassInfo = new FtlClassInfo();
						ftlClassInfo.setDomainClass(objectName);
						ftlClassInfo.setDomainObject(StringUtils.uncapitalize(objectName));//domainObjectName
						ftlClassInfo.setPackageName(targetPackage);
						File daoDir = getDirectory(projectName, targetSource, targetPackage, "dao");
						genFtl(ftlClassInfo, targetSource, "DAO.ftl", daoDir, "Dao");
						File daoImplDir = getDirectory(projectName, targetSource, targetPackage, "dao.impl");
						genFtl(ftlClassInfo, targetSource, "DAOImpl.ftl", daoImplDir, "DaoImpl");
					} catch (IOException e) {
						e.printStackTrace();
					}							
				}
			}
		}
	}
	
	private static String newline = "\n";
	//almost hard code
	public static String getFormattedDomainJavaFile(String targetPackage, String objectName, List<ColumnDefinition> cds){
		StringBuilder sb = new StringBuilder();
		if (StringUtils.isNotBlank(targetPackage)) {
			sb.append("package "); //$NON-NLS-1$
			sb.append(targetPackage);
			sb.append(".domain;");
			sb.append(newline);
			sb.append(newline);
		}
		sb.append("import lombok.Data;");// use lombok
		sb.append(newline);
		sb.append("import lombok.EqualsAndHashCode;");
		sb.append(newline);
		sb.append(newline);
		sb.append("import com.joyveb.support.cassandra.CassandraPrimaryKey;");
		sb.append(newline);
		sb.append(newline);
		sb.append("@Data");
		sb.append(newline);
		sb.append("@EqualsAndHashCode(callSuper=false)");
		sb.append(newline);
		sb.append("public class ").append(objectName).append(" extends CassandraPrimaryKey<String>{");
		sb.append(newline);
		int cdsLength = cds.size();
		for (int i = 0; i < cdsLength; i++) {
			String validateclass = cds.get(i).getValidationClass();
			String name = ss.fromByteBuffer(cds.get(i).getName());
			BiMap<String, String> biMap = dataTypeMap.inverse();
			String type = biMap.get(validateclass);
			if(StringUtils.isNotBlank(type)){
				sb.append("\tprivate ");
				sb.append(type);
				sb.append(" ");
				sb.append(StringUtils.lowerCase(name));
				sb.append(";");
				sb.append(newline);
			}else{
				System.out.println("name:" + name
						+ ", validateclass:" + validateclass
						+ "没有对应的数据类型");
				
			}
		}
		sb.append("}");
        return sb.toString();
	}

	public static File getDirectory(String targetProject, String targetSource, String targetPackage, String domain) {
        File project = new File(targetSource);
		//File project = new File(targetProject + File.separatorChar +targetSource);
        StringBuilder sb = new StringBuilder();
        StringTokenizer st = new StringTokenizer(targetPackage, "."); //$NON-NLS-1$
        while (st.hasMoreTokens()) {
            sb.append(st.nextToken());
            sb.append(File.separatorChar);
        }
        st = new StringTokenizer(domain, "."); //$NON-NLS-1$
        while (st.hasMoreTokens()) {
            sb.append(st.nextToken());
            sb.append(File.separatorChar);
        }
        File directory = new File(project, sb.toString());
        if (!directory.isDirectory()) {
            boolean rc = directory.mkdirs();
            if (!rc) {
                throw new RuntimeException("can't create " + directory.getAbsolutePath());
            }
        }
        return directory;
    }
	
	private static void writeFile(File file, String content) throws IOException {
		System.out.println(file.getAbsolutePath());
        BufferedWriter bw = new BufferedWriter(new FileWriter(file, false));
        bw.write(content);
        bw.close();
    }
	
	public static void genFtl(FtlClassInfo clazzinfo, String targetSource,
			String tmplname, File daoDir, String end) {
		try {
			freemarker.template.Configuration cfg = new freemarker.template.Configuration();
			cfg.setDirectoryForTemplateLoading(new File(targetSource));
			DefaultObjectWrapper ow = new DefaultObjectWrapper();
			cfg.setObjectWrapper(ow);
			Template temp = cfg.getTemplate(tmplname);
			/* Create a data-model */
			Map<String, Object> root = new HashMap<String, Object>();
			root.put("domainClazz", clazzinfo.getDomainClass());
			root.put("domainObject", clazzinfo.getDomainObject());
			root.put("packageName",
					clazzinfo.getPackageName().replaceAll("\\"+File.separator, "."));
			
			File dst = new File(daoDir, clazzinfo.getDomainClass() + end + ".java");
			System.out.println(dst.getAbsolutePath());
			dst.getParentFile().mkdirs();
			FileOutputStream fout = new FileOutputStream(dst);
			Writer out = new OutputStreamWriter(fout);
			temp.process(root, out);
			out.flush();
			fout.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
