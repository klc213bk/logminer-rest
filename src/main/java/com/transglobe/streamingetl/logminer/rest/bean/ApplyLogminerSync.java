package com.transglobe.streamingetl.logminer.rest.bean;

public class ApplyLogminerSync {

	private Boolean resetOffset;
	
	private Long startScn;
	
	private Integer applyOrDrop; // 1 :appply, -1: drop
	
	private String tableListStr; // table separate with ','

	public ApplyLogminerSync() {}
	
	public Boolean getResetOffset() {
		return resetOffset;
	}

	public void setResetOffset(Boolean resetOffset) {
		this.resetOffset = resetOffset;
	}

	public Long getStartScn() {
		return startScn;
	}

	public void setStartScn(Long startScn) {
		this.startScn = startScn;
	}

	public Integer getApplyOrDrop() {
		return applyOrDrop;
	}

	public void setApplyOrDrop(Integer applyOrDrop) {
		this.applyOrDrop = applyOrDrop;
	}

	public String getTableListStr() {
		return tableListStr;
	}

	public void setTableListStr(String tableListStr) {
		this.tableListStr = tableListStr;
	}
	
	
}
