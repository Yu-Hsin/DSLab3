import java.io.Serializable;


public class MapReduceTask implements Serializable{

    private static final long serialVersionUID = 1L;
    
    private int mapperNum;
    private int reducerNum;
    private String[] mapperIP = null;
    private String[] reducerIP = null;
    private int[] mapperPort = null;
    private int[] mapperStatusPort = null;
    private int[] reducerPort = null;
    private int[] reducerPortToMapper = null;
    private int[] reducerStatusPort = null;
    
    /*
     *  Back up master node
     */
    private String[] backMapperIP = null;
    private int[] backMapperPort = null;
    private int[] backMapperStatusPort = null;
    
    /*
     *  Back up reducer node
     */
    private String[] backReducerIP = null;
    private int[] backReducerPort = null;
    private int[] backReducerPortToMapper = null;
    private int[] backReducerStatusPort = null;
    
    private String mapperClass = null;
    private String mapperFunc = null;
    private String reducerClass = null;
    private String reducerFunc = null;
    
    private String jobID = null;
    private static int timestamp = 0;
    
    private int machineNum;
    
    public MapReduceTask() {}
    
    public MapReduceTask(MapReduceTask t) {
	mapperNum = t.mapperNum;
	reducerNum = t.reducerNum;
	mapperIP = t.mapperIP;
	reducerIP = t.reducerIP;
	mapperPort = t.mapperPort;
	reducerPort = t.reducerPort;
	reducerPortToMapper = t.reducerPortToMapper;
	
	mapperClass = t.mapperClass;
	mapperFunc = t.mapperFunc;
	reducerClass = t.reducerClass;
	reducerFunc = t.reducerFunc;
	
	mapperStatusPort = t.mapperStatusPort;
	reducerStatusPort = t.reducerStatusPort;
	
	backMapperIP = t.backMapperIP;
	backMapperPort = t.backMapperPort;
	backMapperStatusPort = t.backMapperStatusPort;
	
	backReducerIP = t.backReducerIP;
	backReducerPort = t.backReducerPort;
	backReducerPortToMapper = t.backReducerPortToMapper;
	backReducerStatusPort = t.backReducerStatusPort;
	
	jobID = t.jobID;
    }
    
    public String getJobID() { return jobID; }
    public int getMapperNum() { return mapperNum; }
    public int getReducerNum() { return reducerNum; }
    public String[] getMapperIP() { return mapperIP; }
    public String[] getReducerIP() { return reducerIP; }
    public int[] getMapperPort() { return mapperPort; }
    public int[] getReducerPort() { return reducerPort; }
    public int[] getReducerPortToMapper() { return reducerPortToMapper; }
    
    public String getMapperClass() { return mapperClass; } 
    public String getMapperFunc() { return mapperFunc; }
    public String getReducerClass() { return reducerClass; }
    public String getReducerFunc() { return reducerFunc; }
    public int getMachineNum() { return machineNum; }
    
    public String[] getBackReducerIP() { return backReducerIP; }
    public int[] getBackReducerPort() { return backReducerPort; }
    public int[] getBackReducerPortToMapper() { return backReducerPortToMapper; }
    public int[] getBackReducerStatusPort() { return backReducerStatusPort; }
    
    public int[] getMapperStatusPort() { return mapperStatusPort; }
    public int[] getReducerStatusPort() { return reducerStatusPort; }
    
    public String[] getBackMapperIP() { return backMapperIP; }
    public int[] getBackMapperPort() { return backMapperPort; }
    public int[] getBackMapperStatusPort() { return backMapperStatusPort; }
    //==================================================================
    
    public void setJobID() { jobID = "job"+timestamp; timestamp++; }
    public void setMapperNum(int n) { mapperNum = n; }
    public void setReducerNum(int n) { reducerNum = n; }
    public void setMapperIP(String[] s) { mapperIP = s; }
    public void setReducerIP(String[] s) { reducerIP = s; }
    public void setMapperPort(int[] p) { mapperPort = p; }
    public void setReducerPort(int[] p) { reducerPort = p; }
    public void setReducerPortToMapper(int[] p) { reducerPortToMapper = p; }
    
    public void setMapperClass(String s) { mapperClass = s; }
    public void setMapperFunc(String s) { mapperFunc = s; }
    public void setReducerClass(String s) { reducerClass = s; }
    public void setReducerFunc(String s) { reducerFunc = s; }
    public void setMachineNum(int i) { machineNum = i; }
    
    public void setBackReducerIP(String[] s) { backReducerIP = s; }
    public void setBackReducerPort(int[] p) { backReducerPort = p; }
    public void setBackReducerPortToMapper(int[] p) { backReducerPortToMapper = p; }
    public void setBackReducerStatusPort(int[] p) { backReducerStatusPort = p; }
    
    public void setMapperStatusPort(int[] s) { mapperStatusPort = s; }
    public void setReducerStatusPort(int[] s) { reducerStatusPort = s; }
    
    public void setBackMapperIP(String[] s) { backMapperIP = s; }
    public void setBackMapperPort(int[] p) { backMapperPort = p; }
    public void setBackMapperStatusPort(int[] p) { backMapperStatusPort = p; }
}
