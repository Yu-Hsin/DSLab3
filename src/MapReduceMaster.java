import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;


public class MapReduceMaster {

    private static final String configfile = "slaves";
    private static final int masterPort = 8000;
    private static int mapperNum;
    private static int mapperPort;
    private static int mapperStatusPort;
    private static int reducerNum;
    private static int reducerPort;
    private static int reducerStatusPort;

    private static HashMap<String, Boolean> availableMapper = null;
    private static HashMap<String, Boolean> availableReducer = null;

    private static HashMap<String, Integer> loadingMapper = null;
    private static HashMap<String, Integer> loadingReducer = null;
    
    public static void main(String[] args) {
	/*
	 *  1. Read in configuration file,
	 *     which contains the number of mapper and reducer
	 *     and their IP and port
	 */
	System.out.println("Initialization...");
	try {
	    BufferedReader configbr = new BufferedReader(new FileReader(configfile));
	    availableMapper = new HashMap<String, Boolean>();
	    availableReducer = new HashMap<String, Boolean>();
	    loadingMapper = new HashMap<String, Integer>();
	    loadingReducer = new HashMap<String, Integer>();

	    String[] strs = configbr.readLine().split("\\s+");
	    mapperNum = Integer.valueOf(strs[1]);
	    strs = configbr.readLine().split("\\s+");
	    mapperPort = Integer.valueOf(strs[1]);
	    strs = configbr.readLine().split("\\s+");
	    mapperStatusPort = Integer.valueOf(strs[1]);

	    for (int i = 0; i < mapperNum; i++) {
		String s = configbr.readLine();
		availableMapper.put(s, false);
		loadingMapper.put(s, 0);
	    }

	    strs = configbr.readLine().split("\\s+");
	    reducerNum = Integer.valueOf(strs[1]);
	    strs = configbr.readLine().split("\\s+");
	    reducerPort = Integer.valueOf(strs[1]);
	    strs = configbr.readLine().split("\\s+");
	    reducerStatusPort = Integer.valueOf(strs[1]);

	    for (int i = 0; i < reducerNum; i++) {
		String s = configbr.readLine();
		availableReducer.put(s, false);
		loadingReducer.put(s, 0);
	    }

	    configbr.close();
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}

	System.out.println("Current System:  " + mapperNum + " Mappers, " + reducerNum + " Reducers.");

	
	/*
	 *  2. Check which nodes are active, which are dead.
	 */
	System.out.println("Check the status of each working nodes...");

	Socket connectSocket;
	int activeMapper = 0, activeReducer = 0;
	for (Map.Entry<String, Boolean> e : availableMapper.entrySet()) {
	    try {
		connectSocket = new Socket(e.getKey(), mapperStatusPort);

		DataOutputStream dos = new DataOutputStream(connectSocket.getOutputStream());
		dos.writeUTF("status");
		dos.flush();
		
		DataInputStream din = new DataInputStream(connectSocket.getInputStream());
		String result = din.readUTF();
		if (result.equals("idle")) {
		    e.setValue(true);
		    activeMapper++;
		}
	    } catch (UnknownHostException e1) {
		e1.printStackTrace();
	    } catch (IOException e1) {
		System.out.println("Node " + e.getKey() + " is inactive...");
	    } 
	}

	for (Map.Entry<String, Boolean> e : availableReducer.entrySet()) {
	    try {
		connectSocket = new Socket(e.getKey(), reducerStatusPort);
		
		DataOutputStream dos = new DataOutputStream(connectSocket.getOutputStream());
		dos.writeUTF("status");
		dos.flush();
		
		DataInputStream din = new DataInputStream(connectSocket.getInputStream());
		String result = din.readUTF();
		if (result.equals("idle")) {
		    e.setValue(true);
		    activeReducer++;
		}
	    } catch (UnknownHostException e1) {
		e1.printStackTrace();
	    } catch (IOException e1) {
		System.out.println("Node " + e.getKey() + " is inactive...");
	    } 
	}
	
	System.out.println("Current active mapper: " + activeMapper + ", active reducer: " + activeReducer);
	
	/*
	 *  3. Start an Server Socket to accept Map Reduce tasks.
	 */
	try {
	    ServerSocket mServer = new ServerSocket(masterPort);

	    while(true) {
		System.out.println("Wait for Map Reduce requests...");
		Socket mapreduceRequest = mServer.accept();

		/* Here the master starts to handle different requests. */
		ObjectInputStream ois = new ObjectInputStream(mapreduceRequest.getInputStream());
		Object obj = ois.readObject();
		
		if (obj instanceof MapReduceTask) {
		    /* Read the task request */
		    System.out.println("Receive a MapReduce task, request " + 
			    		((MapReduceTask)obj).getMapperNum() + " mappers.");
		    assignResource((MapReduceTask)obj);
		    
		    /* Process the map reduce request */
		    ObjectOutputStream oos = new ObjectOutputStream(mapreduceRequest.getOutputStream());
		    oos.writeObject(obj);
		    oos.flush();
		}
		
		/* Wait for the task to be completed and release resources */
		//ois = new ObjectInputStream(mapreduceRequest.getInputStream());
		//obj = ois.readObject();
		
		mapreduceRequest.close();
	    }

	} catch (IOException e) {
	    e.printStackTrace();
	} catch (ClassNotFoundException e1) {
	    e1.printStackTrace();
	}


	/*
	 *  4. Use another thread to read standard input commands.
	 */
    }

    /**
     * This function first reads how many mapper or reducer are required in the task object,
     * then assign mappers, reducers to this task by setting its data members.
     * @param mTask
     */
    private static void assignResource(MapReduceTask mTask) {
	
	int requestMapper = mTask.getMapperNum();
	int requestReducer = mTask.getReducerNum();
	String[] resultMapper = new String[requestMapper];
	String[] resultReducer = new String[requestReducer];
	int mapIdx = 0, redIdx = 0;
	
	for (Map.Entry<String, Boolean> e : availableMapper.entrySet()) {
	    if (e.getValue()) {
		resultMapper[mapIdx++] = e.getKey();
		loadingMapper.put(e.getKey(), loadingMapper.get(e.getKey())+1);
		
		if (mapIdx == requestMapper) break;
	    }
	}
	
	for (Map.Entry<String, Boolean> e : availableReducer.entrySet()) {
	    if (e.getValue()) {
		resultReducer[redIdx++] = e.getKey();
		loadingReducer.put(e.getKey(), loadingReducer.get(e.getKey())+1);
		
		if (redIdx == requestReducer) break;
	    }
	}
	
	/* Setting required informations */
	mTask.setMapperIP(resultMapper);
	mTask.setMapperPort(mapperPort);
	mTask.setReducerIP(resultReducer);
	mTask.setReducerPort(reducerPort);
    }
}
