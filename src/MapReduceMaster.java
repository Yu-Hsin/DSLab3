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
import java.util.*;


public class MapReduceMaster {

    /*
     *  All information to connect to slaves
     */
    private static final String configfile = "slaves";
    private static final int masterPort = 5566;
    private static int mapperNum;
    private static int reducerNum;

    /*
     *  Here maintain the availability and loading of all slaves
     */
    private static HashMap<Addr, Boolean> availableMapper = null;
    private static HashMap<Addr, Boolean> availableReducer = null;

    private static HashMap<Addr, Integer> loadingMapper = null;
    private static HashMap<Addr, Integer> loadingReducer = null;

    /*
     *  Here maintains the load-balancing algorithm.
     */
    private static LinkedList<Addr> roundrobinMapQueue = null;
    private static LinkedList<Addr> roundrobinReduceQueue = null;
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
	    availableMapper = new HashMap<Addr, Boolean>();
	    availableReducer = new HashMap<Addr, Boolean>();
	    loadingMapper = new HashMap<Addr, Integer>();
	    loadingReducer = new HashMap<Addr, Integer>();
	    roundrobinMapQueue = new LinkedList<Addr>();
	    roundrobinReduceQueue = new LinkedList<Addr>();

	    String[] strs = configbr.readLine().split("\\s+");
	    mapperNum = Integer.valueOf(strs[1]);

	    for (int i = 0; i < mapperNum; i++) {
		strs = configbr.readLine().split("\\s+");
		Addr addr = new Addr(strs[0], Integer.valueOf(strs[1]), Integer.valueOf(strs[2]));
		availableMapper.put(addr, false);
		loadingMapper.put(addr, 0);
		roundrobinMapQueue.add(addr);
	    }

	    strs = configbr.readLine().split("\\s+");
	    reducerNum = Integer.valueOf(strs[1]);

	    for (int i = 0; i < reducerNum; i++) {
		strs = configbr.readLine().split("\\s+");
		Addr addr = new Addr(strs[0], Integer.valueOf(strs[2]), Integer.valueOf(strs[3]));
		addr.portToMapper = Integer.valueOf(strs[1]);
		availableReducer.put(addr, false);
		loadingReducer.put(addr, 0);
		roundrobinReduceQueue.add(addr);
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
	for (Map.Entry<Addr, Boolean> e : availableMapper.entrySet()) {
	    try {
		connectSocket = new Socket(e.getKey().ip, e.getKey().statusport);
		
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

	for (Map.Entry<Addr, Boolean> e : availableReducer.entrySet()) {
	    try {
		connectSocket = new Socket(e.getKey().ip, e.getKey().statusport);

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
		System.out.println("Node " + e.getKey().ip + " is inactive...");
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
		t.join();
	    }

	} catch (IOException e) {
	    e.printStackTrace();
	} catch (InterruptedException e1) {
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
	Addr[] resultMapper = new Addr[requestMapper];
	Addr[] resultReducer = new Addr[requestReducer];
	int mapIdx = 0, redIdx = 0;

	/*
	 *  Add the most recently used IP to the end of queue.
	 *  ** Here is critical section => lock
	 */
	synchronized (resourceLock) {
	    while(mapIdx < requestMapper) {
		Addr s = roundrobinMapQueue.poll();
		if (availableMapper.get(s)) {
		    resultMapper[mapIdx++] = s;
		    loadingMapper.put(s, loadingMapper.get(s)+1);
		}

		roundrobinMapQueue.add(s);
	    }
	    
	    while(redIdx < requestReducer) {
		Addr s = roundrobinReduceQueue.poll();
		if (availableReducer.get(s)) {
		    resultReducer[redIdx++] = s;
		    loadingReducer.put(s, loadingReducer.get(s)+1);
		}

		roundrobinReduceQueue.add(s);
	    }
	    
	}

	System.out.println(mapIdx + " " + mapperNum + " " + resultMapper.length);
	/* Setting required informations */
	String[] taskMappers = new String[requestMapper];
	int[] taskMappersPort = new int[requestMapper];
	for (int i = 0; i < requestMapper; i++) {
	    taskMappers[i] = resultMapper[i].ip;
	    taskMappersPort[i] = resultMapper[i].port;
	}
	mTask.setMapperIP(taskMappers);
	mTask.setMapperPort(taskMappersPort);
	
	
	String[] taskReducers = new String[requestReducer];
	int[] taskReducersPort = new int[requestReducer];
	int[] taskReducersPortToMapper = new int[requestReducer];
	for (int i = 0; i < requestReducer; i++) {
	    taskReducers[i] = resultReducer[i].ip;
	    taskReducersPort[i] = resultReducer[i].port;
	    taskReducersPortToMapper[i] = resultReducer[i].portToMapper;
	}
	mTask.setReducerIP(taskReducers);
	mTask.setReducerPort(taskReducersPort);
	mTask.setReducerPortToMapper(taskReducersPortToMapper);
    }

    /**
     * This function release mapper and reducer by decrementing the loading number.
     * @param maps
     * @param reduces
     */
    private static void releaseResource(String[] maps, int[] mapports, String[] reduces, int[] redports) {
	synchronized(resourceLock) {
	    //TODO
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
		
		
		System.out.println("Wait for finish......");

		/* Wait for the task to be completed and release resources */
		obj = ois.readObject();
		if (obj instanceof MapReduceTask) {
		    System.out.println("Receive a message from completed task...  Now release the resource...");
		    String[] mapperRelease = ((MapReduceTask)obj).getMapperIP();
		    String[] reducerRelease = ((MapReduceTask)obj).getReducerIP();
		    int[] mapperIpRelease = ((MapReduceTask)obj).getMapperPort();
		    int[] reducerIpRelease = ((MapReduceTask)obj).getReducerPort();
		    
		    releaseResource(mapperRelease, mapperIpRelease, reducerRelease, reducerIpRelease);
		}

		mSocket.close();

	    } catch (IOException e) {
		e.printStackTrace();
	    } catch (ClassNotFoundException e) {
		e.printStackTrace();
	    }

	}
    }
    
    private static class Addr {
	public String ip;
	public int port;
	public int portToMapper;
	public int statusport;
	public Addr (String i, int p, int s) {
	    ip = i;
	    port = p;
	    statusport = s;
	}
	
	@Override
	public int hashCode() {
	    return (ip+" "+port).hashCode();
	}
	
	@Override
	public boolean equals(Object a) {
	    if (!(a instanceof Addr)) return false;
	    
	    Addr addr = (Addr) a;
	    return ip.equals(addr.ip) && port == addr.port;
	}
    }
}
