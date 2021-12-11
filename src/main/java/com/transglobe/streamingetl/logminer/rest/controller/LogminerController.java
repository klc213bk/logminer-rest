package com.transglobe.streamingetl.logminer.rest.controller;

import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.transglobe.streamingetl.logminer.rest.bean.ApplyLogminerSync;
import com.transglobe.streamingetl.logminer.rest.service.LogminerService;
import com.transglobe.streamingetl.logminer.rest.util.LogminerUtils;

@RestController
@RequestMapping("/logminer")
public class LogminerController {
	static final Logger LOG = LoggerFactory.getLogger(LogminerController.class);

	@Autowired
	private LogminerService logminerService;

	@Autowired
	private ObjectMapper mapper;

	@PostMapping(path="/startConnector/reset", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> startConnectorReset() throws Exception {
		LOG.info(">>>>controller startConnectorReset is called");

		ObjectNode objectNode = mapper.createObjectNode();

		try {
			logminerService.startConnector(true);


			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
			throw e;
		}

		LOG.info(">>>>controller startConnector finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/startConnector", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> startConnector() throws Exception {
		LOG.info(">>>>controller startConnector is called");

		ObjectNode objectNode = mapper.createObjectNode();

		try {
			logminerService.startConnector();



//			logminerService.updateTMLogminerOffset();

			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
			throw e;
		}

		LOG.info(">>>>controller startConnector finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/stopConnector", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> stopConnector() {
		LOG.info(">>>>controller stopConnector is called");

		ObjectNode objectNode = mapper.createObjectNode();

		try {
			logminerService.stopConnector();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		LOG.info(">>>>controller stopConnector finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/restartConnector", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> restartConnector() {
		LOG.info(">>>>controller restartConnector is called");

		ObjectNode objectNode = mapper.createObjectNode();

		try {
			logminerService.restartConnector();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		LOG.info(">>>>controller restartConnector finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/applyLogminerSync", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> applyLogminerSync(@RequestBody ApplyLogminerSync applySync) throws Exception {
		LOG.info(">>>>controller applyLogminerSync is called");

		ObjectNode objectNode = mapper.createObjectNode();

		try {
			//			LOG.info(">>>>applySync={}", ToStringBuilder.reflectionToString(applySync));

			Boolean result = logminerService.applyLogminerSync(applySync);

//			Thread.sleep(5000);
//			
//			String connectorState = LogminerUtils.getConnectorState();
//			String taskState = LogminerUtils.getConnectorTaskState();
//			if (!"RUNNING".equals(connectorState)
//					|| !"RUNNING".equals(taskState)) {
//				LOG.info(">>>>Restarting connector, connectorState={}, taskState={}", connectorState, taskState);
//				logminerService.restartConnector();
//			} 
//			LOG.info(">>>>No Restarting connector, connectorState={}, taskState={}", connectorState, taskState);
//			
//			int cnt = 0;
//			while (true) {
//				connectorState = LogminerUtils.getConnectorState();
//				taskState = LogminerUtils.getConnectorTaskState();
//				LOG.info(">>>>keep getting connector status, connectorState={}, taskState={}", connectorState, taskState);
//				if ("RUNNING".equals(connectorState)
//						&& "RUNNING".equals(taskState)) {
//					break;
//				} 
//				Thread.sleep(5000);
//				cnt++;
//				if (cnt > 12) {
//					throw new Exception("Connector does not start successfully!!!!!");
//				}
//			}
			
//			logminerService.updateTMLogminerOffset();

			objectNode.put("returnCode", "0000");
			objectNode.put("status", (result == null)? Boolean.FALSE.toString() : result.toString());
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
			throw e;
		}

		LOG.info(">>>>controller applyLogminerSync finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}

}
