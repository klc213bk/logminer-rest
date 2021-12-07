package com.transglobe.streamingetl.logminer.rest.bean;

import java.sql.Timestamp;

public class HealthStatus {
	public static enum HealthState {
		ACTIVE, STANDBY;
	}
	private String etlName;
	private HealthState healthState;
	private Timestamp updateTimestamp;

	

	public HealthState getHealthState() {
		return healthState;
	}

	public void setHealthState(HealthState healthState) {
		this.healthState = healthState;
	}

	public String getEtlName() {
		return etlName;
	}

	public void setEtlName(String etlName) {
		this.etlName = etlName;
	}

	public Timestamp getUpdateTimestamp() {
		return updateTimestamp;
	}

	public void setUpdateTimestamp(Timestamp updateTimestamp) {
		this.updateTimestamp = updateTimestamp;
	}
	
	
}
