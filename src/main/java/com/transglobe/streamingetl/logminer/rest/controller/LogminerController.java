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
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.transglobe.streamingetl.logminer.rest.bean.ApplyLogminerSync;
import com.transglobe.streamingetl.logminer.rest.service.LogminerService;

@RestController
@RequestMapping("/logminer")
public class LogminerController {
	static final Logger LOG = LoggerFactory.getLogger(LogminerController.class);

	@Autowired
	private LogminerService logminerService;
	
	@Autowired
	private ObjectMapper mapper;

	@PostMapping(path="/startConnector", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> startConnector() {
		LOG.info(">>>>controller startConnector is called");

		ObjectNode objectNode = mapper.createObjectNode();

		try {
			logminerService.startConnector();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
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
	@PostMapping(path="/applyLogminerSync", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> applyLogminerSync(@RequestBody ApplyLogminerSync applySync) {
		LOG.info(">>>>controller applyLogminerSync is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
//			LOG.info(">>>>applySync={}", ToStringBuilder.reflectionToString(applySync));
			
			String status = logminerService.applyLogminerSync(applySync);
			
			objectNode.put("returnCode", "0000");
			objectNode.put("status", status);
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}
		
		LOG.info(">>>>controller applyLogminerSync finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
}
