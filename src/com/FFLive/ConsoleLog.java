package com.FFLive;

import java.util.Calendar;


//I have written this log class as I had need to leave some logging messages out but I also wanted to use console levels and carriage return
public class ConsoleLog {

	//Log Levels 0=NONE, 1=CRITICAL, 2=ERROR, 3=WARNING, 4=PARTIAL, 5=NORMAL, 6=FINE, 7=FINER, 8=FINEST, 9=DEBUG, 10=ALL 
	public int logLevel = 5;
	
	public ConsoleLog() {
		
	}
	
	public ConsoleLog(int loggingLevel) {
		if(loggingLevel > 10 || loggingLevel < 0) {
			System.err.println("Invalid Logging Level - Must be between 0-10");
			System.err.println("Log Levels 0=NONE, 1=CRITICAL, 2=ERROR, 3=WARNING, 4=PARTIAL, 5=NORMAL, 6=FINE, 7=FINER, 8=FINEST, 9=DEBUG, 10=ALL");
		}
		else {
		logLevel = loggingLevel;
		}
	}
	
	public void logLevel(int loggingLevel) {
		if(loggingLevel > 10 || loggingLevel < 0) {
			log(2,"Invalid Logging Level - Must be between 0-10, Logging Level Remaining at " + logLevel);
			log(2,"Log Levels 0=NONE, 1=CRITICAL, 2=ERROR, 3=WARNING, 4=PARTIAL, 5=NORMAL, 6=FINE, 7=FINER, 8=FINEST, 9=DEBUG, 10=ALL");
		}
		else {
			logLevel = loggingLevel;
		}
	}
	
	public void log (int level, String message) {
		Calendar now = Calendar.getInstance();
		//Dont print if you are logging at a 'higher' level
		if (level <= logLevel) {
			if(level == 1) {
				System.err.print(now.getTime() + " [crit] " + message);
			}
			else if(level == 2) {
				System.err.print(now.getTime() + " [err ] " + message);
			}
			else if(level == 3) {
				System.out.print(now.getTime() + " [warn] " + message);
			}
			else {
				System.out.print(now.getTime() + " " + message);
			}
		}
	}
	
	//No Date
	public void log (int level, String message, int noDate) {
		//Dont print if you are logging at a 'higher' level
		if (level <= logLevel) {
			if(level == 1) {
				System.err.print("[crit] " + message);
			}
			else if(level == 2) {
				System.err.print("[err ] " + message);
			}
			else if(level == 3) {
				System.err.print("[warn] " + message);
			}
			else {
				System.out.print(message);
			}
		}
	}
	
	public void log (int level, Exception e) {
		Calendar now = Calendar.getInstance();
		System.err.print(now.getTime() + " ");
		e.printStackTrace();
	}
	
	public void ln (int level) {
		if (level <= logLevel) {
			System.out.print("\n");
		}
	}
}
