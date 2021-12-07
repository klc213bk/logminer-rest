package com.transglobe.streamingetl.logminer.rest.service;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.logminer.rest.bean.ApplyLogminerSync;


@Service
public class LogminerService {
	static final Logger LOG = LoggerFactory.getLogger(LogminerService.class);

	@Value("${tglminer.db.driver}")
	private String tglminerDbDriver;

	@Value("${tglminer.db.url}")
	private String tglminerDbUrl;

	@Value("${tglminer.db.username}")
	private String tglminerDbUsername;

	@Value("${tglminer.db.password}")
	private String tglminerDbPassword;

	@Value("${connector.name}")
	private String connectorName;

	@Value("${connect.rest.port}")
	private String connectRestPort;

	@Value("${connect.rest.url}")
	private String connectRestUrl;

	@Value("${connector.start.script}")
	private String connectorStartScript;

	private Process connectorStartProcess;
	private ExecutorService connectorStartExecutor;
	private AtomicBoolean connectorStartFinished = new AtomicBoolean(false);
	private AtomicBoolean connectorStopFinished = new AtomicBoolean(false);

	private BasicDataSource tglminerConnPool;

	private ExecutorService executor;

	@PreDestroy
	public void destroy() {
		LOG.info(">>>> PreDestroy Kafka Service....");

	}
	public void startConnector() throws Exception {
		LOG.info(">>>>>>>>>>>> logminerService.startConnector starting");
		try {
			if (connectorStartProcess == null || !connectorStartProcess.isAlive()) {
				LOG.info(">>>>>>>>>>>> connectorStartProcess.isAlive={} ", (connectorStartProcess == null)? null : connectorStartProcess.isAlive());
				connectorStartFinished.set(false);
				ProcessBuilder builder = new ProcessBuilder();
				//	String script = "./start-connector.sh";
				//builder.command("sh", "-c", script);
				builder.command(connectorStartScript);

				builder.directory(new File("."));
				connectorStartProcess = builder.start();

				connectorStartExecutor = Executors.newSingleThreadExecutor();
				connectorStartExecutor.submit(new Runnable() {

					@Override
					public void run() {
						BufferedReader reader = new BufferedReader(new InputStreamReader(connectorStartProcess.getInputStream()));
						reader.lines().forEach(line -> {
							LOG.info(line);
							if (line.contains("INFO Kafka startTimeMs")) {
								connectorStartFinished.set(true);
								LOG.info("@@@@@@@@   connectorStartFinished set true");
							}  else if (line.contains("Kafka Connect stopped")) {
								connectorStopFinished.set(true);
								LOG.info("@@@@@@@@   connectorStopFinished set true");
							}
						});
					}

				});

				while (!connectorStartFinished.get()) {
					LOG.info(">>>>>>WAITING 1 sec FOR FINISH");
					Thread.sleep(1000);
				}

				LOG.info(">>>>>>>>>>>> LogminerService.startConnector End");
			} else {
				LOG.warn(" >>> connectorStartProcess is currently Running.");
			}
		} catch (IOException e) {
			LOG.error(">>> Error!!!, startConnector, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void stopConnector() throws Exception {
		LOG.info(">>>>>>>>>>>> stopConnector");
		if (connectorStartProcess == null) {
			LOG.warn(">>>>>>>>>>>> connectorStartProcess is null, Cannot stop !!!");
		} else {
			if (connectorStartProcess.isAlive()) {
				connectorStartProcess.destroy();
				connectorStartExecutor.shutdown();
				if (!connectorStartExecutor.isTerminated()) {
					connectorStartExecutor.shutdownNow();

					try {
						connectorStartExecutor.awaitTermination(180, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
						LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
								ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
					}

				}

			} else {
				LOG.info(">>> connector is NOT alive, cannot stop");
			}
		}

		// kill process
		ProcessBuilder builder = new ProcessBuilder();
		String script = String.format("kill -9 $(lsof -t -i:%d -sTCP:LISTEN)", Integer.valueOf(connectRestPort));
		LOG.info(">>> stop script={}", script);

		builder.command("bash", "-c", script);
		//builder.command(script);

		//	builder.directory(new File(streamingetlHome));
		Process logminerShutdownProcess = builder.start();
		int exitVal = logminerShutdownProcess.waitFor();
		if (exitVal == 0) {
			LOG.info(">>> Success!!! kill Logminer connector");
		} else {
			LOG.error(">>> Error!!! kill Logminer  connector, exitcode={}", exitVal);
		}

		LOG.info(">>> Logminer  connector Stopped !!");
	}
	public String applyLogminerSync(ApplyLogminerSync applySync) throws Exception {
		LOG.info(">>> ApplyLogminerSync={}", ToStringBuilder.reflectionToString(applySync));
		Map<String,String>  configmap = getConnectorConfig(connectorName);
		LOG.info(">>> original configmap={}", configmap);

		LOG.info(">>> updatedConnectorConfigMap");

		String[] tableArr = applySync.getTableListStr().split(",");
		List<String> tableList = Arrays.asList(tableArr);
		Set<String> tableSet = new HashSet<>(tableList);

		updatedConnectorConfigMap(configmap, applySync.getResetOffset(), applySync.getApplyOrDrop(), tableSet);

		LOG.info(">>> updated configmap={}", configmap);

		LOG.info(">>>> pause connector");
		pauseConnector(connectorName);

		//		LOG.info(">>>> delete connector");
		//		deleteConnector(connectorName);

		LOG.info(">>>> add sync table to config's whitelist");

		LOG.info(">>>> create connector");
		boolean result =createConnector(connectorName, configmap);
		LOG.info(">>>> create connector result={}", result);


		LOG.info(">>>> resume connector");
		resumeConnector(connectorName);


		String status = getConnectorStatus(connectorName);
		LOG.info(">>>> connector status={}", status);

		// update logminer offset status
		
		
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		String sql = null;
		try {
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);

			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValueAsString(configmap);
			
			
			sql = "update TM_LOGMINER_OFFSET SET TABLE_WHITE_LIST=?,KAFKA_TOPICS=?,STATUS=? where start_time = \n" +
					" (select start_time from TM_LOGMINER_OFFSET order by start_time desc \n" +
					" fetch next 1 row only)";
			
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, mapper.writeValueAsString(configmap));
			pstmt.setString(2, "");
			pstmt.setString(3, status);
			pstmt.executeUpdate();
			pstmt.close();
		} finally {
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}



		return status;

	}
	
	public String getConnectorStatus(String connector) throws Exception {
		String urlStr = connectRestUrl+"/connectors/" + connector+ "/status";
		HttpURLConnection httpCon = null;
		//		ConnectorStatus connectorStatus = null;
		try {
			URL url = new URL(urlStr);
			httpCon = (HttpURLConnection)url.openConnection();
			httpCon.setRequestMethod("GET");
			int responseCode = httpCon.getResponseCode();
			LOG.info(">>>>> getConnectorStatus responseCode:" + responseCode);

			String readLine = null;

			BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream(), "UTF-8"));
			StringBuffer response = new StringBuffer();
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			LOG.info(">>>>> getConnectorStatus response:" + response.toString());

			return response.toString();

			//			ObjectMapper objectMapper = new ObjectMapper();
			//			objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			//			connectorStatus = objectMapper.readValue(response.toString(), ConnectorStatus.class);

			//			JsonNode jsonNode = objectMapper.readTree(response.toString());
			//			JsonNode connectorNode = jsonNode.get("connector");
			//			JsonNode stateNode = connectorNode.get("state");
			//			String state = stateNode.asText();
			//
			//			if (httpCon.HTTP_OK == responseCode) {
			//				connectorStatus = new ConnectorStatus(connector, state, null);
			//			} else {
			//				connectorStatus = new ConnectorStatus(connector, state, response.toString());
			//			}
		} finally {
			if (httpCon != null ) httpCon.disconnect();
		}
	}
	public Map<String,String> getConnectorConfig(String connectorName) throws Exception {


		Map<String,String> configmap = new HashMap<>();
		String urlStr = String.format(connectRestUrl+"/connectors/%s/config", connectorName);
		LOG.info(">>>>>>>>>>>> urlStr={} ", urlStr);
		HttpURLConnection httpCon = null;
		try {
			URL url = new URL(urlStr);
			httpCon = (HttpURLConnection)url.openConnection();
			httpCon.setRequestMethod("GET");
			int responseCode = httpCon.getResponseCode();
			String readLine = null;
			//			if (httpCon.HTTP_OK == responseCode) {
			BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream()));
			StringBuffer response = new StringBuffer();
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			LOG.info(">>>>> CONNECT REST responseCode={},response={}", responseCode, response.toString());

			configmap = new ObjectMapper().readValue(response.toString(), HashMap.class);

		} finally {
			if (httpCon != null ) httpCon.disconnect();
		}
		return configmap;

	}
	public void updatedConnectorConfigMap(Map<String,String> configmap, Boolean resetOffset, int applyOrDrop, Set<String> tableSet) throws Exception {

		LOG.info(">>>> configmap={}", configmap);

		if (Boolean.TRUE.equals(resetOffset)) {
			configmap.put("reset.offset", "true");
		} else {
			configmap.put("reset.offset", "false");
		}

		if (applyOrDrop == 1) {
			String newsyncTables = String.join(",", tableSet);	
			LOG.info(">>>> add sync table:{}", newsyncTables);

			// "reset.offset", "table.whitelist"
			String newtableWhitelist = "";
			newtableWhitelist = configmap.get("table.whitelist") + "," + newsyncTables;
			newtableWhitelist = StringUtils.strip(newtableWhitelist, ",");
			configmap.put("table.whitelist", newtableWhitelist);


		} else if (applyOrDrop == -1) {
			LOG.info(">>>> remove sync tableSet:{}", String.join(",", tableSet));

			String[] tableArr = configmap.get("table.whitelist").split(",");
			List<String> tableList = Arrays.asList(tableArr);
			LOG.info(">>>> existing sync tableList:{}", String.join(",", tableList));

			String newtableWhitelist = tableList.stream().filter(s -> !tableSet.contains(s)).collect(Collectors.joining(","));
			newtableWhitelist = StringUtils.strip(newtableWhitelist, ",");
			LOG.info(">>>> new newtableWhitelist={}", newtableWhitelist);

			configmap.put("table.whitelist", newtableWhitelist);


		} 

		LOG.info(">>>> new configmap={}", configmap);
	}
	public boolean pauseConnector(String connectorName) throws Exception{
		String urlStr = connectRestUrl + "/connectors/" + connectorName + "/pause";
		LOG.info(">>>>> connector urlStr:" + urlStr);

		HttpURLConnection httpConn = null;
		//		DataOutputStream dataOutStream = null;
		int responseCode = -1;
		try {
			URL url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("PUT");

			responseCode = httpConn.getResponseCode();
			LOG.info(">>>>> pause responseCode={}",responseCode);

			String readLine = null;

			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream(), "UTF-8"));
			StringBuffer response = new StringBuffer();
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			LOG.info(">>>>> pause response={}",response.toString());

			if (202 == responseCode) {
				return true;
			} else {
				return false;
			}
		} finally {

			if (httpConn != null )httpConn.disconnect();
		}
	}
	public boolean resumeConnector(String connectorName) throws Exception{
		String urlStr = connectRestUrl + "/connectors/" + connectorName + "/resume";
		LOG.info(">>>>> connector urlStr:" + urlStr);

		HttpURLConnection httpConn = null;
		//		DataOutputStream dataOutStream = null;
		int responseCode = -1;
		try {
			URL url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("PUT");

			responseCode = httpConn.getResponseCode();
			LOG.info(">>>>> pause responseCode={}",responseCode);

			String readLine = null;

			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream(), "UTF-8"));
			StringBuffer response = new StringBuffer();
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			LOG.info(">>>>> pause response={}",response.toString());

			if (202 == responseCode) {
				return true;
			} else {
				return false;
			}
		} finally {

			if (httpConn != null )httpConn.disconnect();
		}
	}
	public boolean deleteConnector(String connectorName) throws Exception{
		String urlStr = connectRestUrl + "/connectors/" + connectorName +"/";
		//		LOG.info(">>>>> connector urlStr:" + urlStr);
		HttpURLConnection httpConn = null;
		try {
			URL url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("DELETE");
			int responseCode = httpConn.getResponseCode();

			LOG.info(">>>>> DELETE responseCode={}",responseCode);

			String readLine = null;

			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream(), "UTF-8"));
			StringBuffer response = new StringBuffer();
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			LOG.info(">>>>> delete response={}",response.toString());

			if (204 == responseCode) {
				return true;
			} else {
				return false;
			}

		} finally {
			if (httpConn != null )httpConn.disconnect();
		}
	}
	public boolean createConnector(String connectorName, Map<String, String> configmap) throws Exception {
		LOG.info(">>>>>>>>>>>> createNewConnector");

		HttpURLConnection httpConn = null;
		DataOutputStream dataOutStream = null;
		try {

			//			Map<String, Object> map = new HashMap<>();
			//			map.put("name", connectorName);
			//			map.put("config", configmap);

			ObjectMapper objectMapper = new ObjectMapper();
			String configStr = objectMapper.writeValueAsString(configmap);


			String urlStr = connectRestUrl+"/connectors/" + connectorName + "/config";

			LOG.info(">>>>> connector urlStr={},reConfigStr={}", urlStr, configStr);

			URL url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("PUT"); 
			httpConn.setDoInput(true);
			httpConn.setDoOutput(true);
			httpConn.setRequestProperty("Content-Type", "application/json");
			httpConn.setRequestProperty("Accept", "application/json");

			dataOutStream = new DataOutputStream(httpConn.getOutputStream());
			dataOutStream.writeBytes(configStr);

			dataOutStream.flush();

			int responseCode = httpConn.getResponseCode();
			LOG.info(">>>>> createNewConnector responseCode={}",responseCode);

			String readLine = null;

			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream(), "UTF-8"));
			StringBuffer response = new StringBuffer();
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();
			LOG.info(">>>>> create connenctor response={}",response.toString());

			if (200 == responseCode || 201 == responseCode) {
				return true;
			} else {
				return false;
			}

		}  finally {
			if (dataOutStream != null) {
				try {
					dataOutStream.flush();
					dataOutStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (httpConn != null )httpConn.disconnect();

		}
	}

	public boolean stopHeartbeat() {
		LOG.info(">>>>>>>>>>>> stopHeartbeat ");
		boolean result = true;
		if (executor != null) {

			try {
				if (tglminerConnPool != null) tglminerConnPool.close();
			} catch (Exception e) {
				result = false;
				LOG.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}

			executor.shutdown();
			if (!executor.isTerminated()) {
				executor.shutdownNow();

				try {
					executor.awaitTermination(300, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					result = false;
					LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
							ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}

			}

		}

		LOG.info(">>>>>>>>>>>> stopHeartbeat done !!!");

		return result;
	}

}
