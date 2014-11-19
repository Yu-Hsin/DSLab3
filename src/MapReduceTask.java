import java.io.Serializable;


public class MapReduceTask implements Serializable{

    private static final long serialVersionUID = 1L;
    
    private int mapperNum;
    private int reducerNum;
    private String[] mapperIP = null;
    private String[] reducerIP = null;
    private int mapperPort;
    private int reducerPort;
    
    public MapReduceTask() {}
    
    public int getMapperNum() { return mapperNum; }
    public int getReducerNum() { return reducerNum; }
    public String[] getMapperIP() { return mapperIP; }
    public String[] getReducerIP() { return reducerIP; }
    public int getMapperPort() { return mapperPort; }
    public int getReducerPort() { return reducerPort; }
    
    public void setMapperNum(int n) { mapperNum = n; }
    public void setReducerNum(int n) { reducerNum = n; }
    public void setMapperIP(String[] s) { mapperIP = s; }
    public void setReducerIP(String[] s) { reducerIP = s; }
    public void setMapperPort(int p) { mapperPort = p; }
    public void setReducerPort(int p) { reducerPort = p; }
}
