package com.transglobe.streamingetl.logminer.rest.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.logminer.rest.bean.ApplyLogminerSync;

public class LogminerUtils {
	static final Logger LOG = LoggerFactory.getLogger(LogminerUtils.class);

	private static final String URL_APPLY_LOGMINER_SYNC = "http://localhost:9102/logminer/applyLogminerSync";

	private static final String URL_GET_CONNECTOR_STATUS = "http://localhost:8083/connectors/oracle-logminer-connector/status";

	private static final String URL_GET_CONNECTOR_TASK_STATUS = "http://localhost:8083/connectors/oracle-logminer-connector/tasks/0/status";
	
	private static String restartLogminerConnector(ApplyLogminerSync applySync) throws Exception{
		LOG.info(">>>>>>> restartLogminerConnector ...");

		String applySyncUrl = URL_APPLY_LOGMINER_SYNC;

		ObjectMapper mapper = new ObjectMapper();
		String jsonStr = mapper.writeValueAsString(applySync);

		LOG.info(">>>>>>> applySyncUrl={}, jsonStr={}", applySyncUrl, jsonStr); 
		String response = HttpUtils.restPostService(applySyncUrl, jsonStr);

		LOG.info(">>>>>>> applyLogminerSync response={}", response);

		return response;
	}
	public static String getConnectorState() throws Exception{

		String urlStr = URL_GET_CONNECTOR_STATUS;

		String response = HttpUtils.restService(urlStr, "GET");

		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode jsonNode = objectMapper.readTree(response);
		JsonNode connector = jsonNode.get("connector");
		String state = connector.get("state").asText();

		return state;
	}
	public static String getConnectorTaskState() throws Exception{
		String urlStr = URL_GET_CONNECTOR_TASK_STATUS;
		String state = "";

		String response = HttpUtils.restService(urlStr, "GET");

		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode jsonNode = objectMapper.readTree(response);
		state = jsonNode.get("state").asText();


		return state;
	}
}
