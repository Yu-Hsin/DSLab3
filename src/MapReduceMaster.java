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
import java.util.*;


public class MapReduceMaster {

    /*
     *  All information to connect to slaves
     */
    private static final String configfile = "slaves";
    private static final int masterPort = 8000;
    private static int mapperNum;
    private static int mapperPort;
    private static int mapperStatusPort;
    private static int reducerNum;
    private static int reducerPort;
    private static int reducerStatusPort;

    /*
     *  Here maintain the availability and loading of all slaves
     */
    private static HashMap<String, Boolean> availableMapper = null;
    private static HashMap<String, Boolean> availableReducer = null;

    private static HashMap<String, Integer> loadingMapper = null;
    private static HashMap<String, Integer> loadingReducer = null;

    /*
     *  Here maintains the load-balancing algorithm.
     */
    private static LinkedList<String> roundrobinMapQueue = null;
    private static LinkedList<String> roundrobinReduceQueue = null;
    private static Object resourceLock = new Object();

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
	    roundrobinMapQueue = new LinkedList<String>();
	    roundrobinReduceQueue = new LinkedList<String>();

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
		roundrobinMapQueue.add(s);
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
		roundrobinReduceQueue.add(s);
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
		System.out.println("Receive a request... Create a new thread...");
		Thread t = new Thread(new TaskRequestThread(mapreduceRequest));
		t.start();
	    }

	} catch (IOException e) {
	    e.printStackTrace();
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

	/*
	 *  Add the most recently used IP to the end of queue.
	 *  ** Here is critical section => lock
	 */
	synchronized (resourceLock) {
	    while(mapIdx < requestMapper) {
		String s = roundrobinMapQueue.poll();
		if (availableMapper.get(s)) {
		    resultMapper[mapIdx++] = s;
		    loadingMapper.put(s, loadingMapper.get(s)+1);
		}

		roundrobinMapQueue.add(s);
	    }

	    while(redIdx < requestReducer) {
		String s = roundrobinReduceQueue.poll();
		if (availableReducer.get(s)) {
		    resultReducer[redIdx++] = s;
		    loadingReducer.put(s, loadingReducer.get(s)+1);
		}

		roundrobinReduceQueue.add(s);
	    }
	}


	/* Setting required informations */
	mTask.setMapperIP(resultMapper);
	mTask.setMapperPort(mapperPort);
	mTask.setReducerIP(resultReducer);
	mTask.setReducerPort(reducerPort);
    }

    /**
     * This function release mapper and reducer by decrementing the loading number.
     * @param maps
     * @param reduces
     */
    private static void releaseResource(String[] maps, String[] reduces) {
	synchronized(resourceLock) {
	    for (String s : maps) loadingMapper.put(s, loadingMapper.get(s)-1);
	    for (String s : reduces) loadingReducer.put(s, loadingReducer.get(s)-1);
	}
    }

    private static class TaskRequestThread implements Runnable {
	Socket mSocket = null;

	public TaskRequestThread(Socket s) {
	    mSocket = s;
	}

	@Override
	public void run() {

	    /* Here the master starts to handle different requests. */
	    ObjectInputStream ois;
	    try {
		ois = new ObjectInputStream(mSocket.getInputStream());
		Object obj = ois.readObject();

		if (obj instanceof MapReduceTask) {
		    /* Read the task request */
		    System.out.println("Receive a MapReduce task, request " + 
			    ((MapReduceTask)obj).getMapperNum() + " mappers.");
		    assignResource((MapReduceTask)obj);

		    /* Process the map reduce request */
		    ObjectOutputStream oos = new ObjectOutputStream(mSocket.getOutputStream());
		    oos.writeObject(obj);
		    oos.flush();
		}

		/* Wait for the task to be completed and release resources */
		ois = new ObjectInputStream(mSocket.getInputStream());
		obj = ois.readObject();
		if (obj instanceof MapReduceTask) {
		    System.out.println("Receice a message from completed task...  Now release the resource...");
		    String[] mapperRelease = ((MapReduceTask)obj).getMapperIP();
		    String[] reducerRelease = ((MapReduceTask)obj).getReducerIP();

		    releaseResource(mapperRelease, reducerRelease);
		}

		mSocket.close();

	    } catch (IOException e) {
		e.printStackTrace();
	    } catch (ClassNotFoundException e) {
		e.printStackTrace();
	    }

	}
    }
}
