package com.transglobe.streamingetl.logminer.rest.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PreDestroy;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


@Service
public class LogminerService {
	static final Logger LOG = LoggerFactory.getLogger(LogminerService.class);

	@Value("${connect.rest.port}")
	private String connectRestPort;
	
	private Process connectorStartProcess;
	private ExecutorService connectorStartExecutor;
	private AtomicBoolean connectorStartFinished = new AtomicBoolean(false);
	private AtomicBoolean connectorStopFinished = new AtomicBoolean(false);
	private Process connectorStopProcess;

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
				String script = "./start-connector.sh";
				//builder.command("sh", "-c", script);
				builder.command(script);

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
}
