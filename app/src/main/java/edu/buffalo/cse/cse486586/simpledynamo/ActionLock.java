package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.Map;

public class ActionLock{
	int totalRowsDeleted = 0;
		boolean isResponseReceived = false;
		Map<String, String> queryDump = null;
		public ActionLock(){
			this.isResponseReceived = false;
			this.totalRowsDeleted = 0;
			this.queryDump = null;
		}
	}