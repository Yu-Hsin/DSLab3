import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import Utils.Utility;

public class ReducerClient {

    private static int mapperPort, masterPort, statusPort;
    private static Status status;
    private ServerSocket socketStatus;
    private MapReduceTask mTask;

    private ServerSocket reducerToMapper, reducerToMaster;

    private String reducerClass, reducerFunction; // mapperClass = run
    private static Map<String, List<String>> map;

    public ReducerClient() {
	map = new HashMap<String, List<String>>();
    }

    /**
     * open a socket for connection to the master
     */
    public void openSocket() {
	try {
	    reducerToMapper = new ServerSocket(mapperPort);
	    reducerToMaster = new ServerSocket(masterPort);

	    ConnectionService cs = new ConnectionService(reducerToMapper);
	    new Thread(cs).start();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public void setTask(MapReduceTask m) {
	reducerClass = mTask.getReducerClass();
	reducerFunction = mTask.getReducerFunc();
    }

    /**
     * Get configuration information form the master and call setTask function
     */
    public void getInitialInfo() {
	try {

	    Socket socket = reducerToMaster.accept();

	    System.out
		    .println("Getting initial information for current task...");
	    ObjectInputStream ois = new ObjectInputStream(
		    socket.getInputStream());

	    Object obj = ois.readObject();
	    if (obj instanceof MapReduceTask)
		mTask = (MapReduceTask) obj;
	    setTask(mTask);
	    ois.close();
	    socket.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    /**
     * wait for master's response and then proceed next step
     */
    public void waitForMaster() {
	try {
	    reducerToMaster.accept();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    /**
     * If the master send a reset message, that means the original reducer does
     * not crash and this back-up reducer doesn't need to send the result back
     * to the master. Otherwise, it sends the result back to the master node.
     */
    public void ackMaster() {
	try {
	    
	    Socket s = reducerToMaster.accept();
	    System.out.println("Connect to master!");
	    BufferedReader br = new BufferedReader(new InputStreamReader(
		    s.getInputStream()));
	    
	    String str = br.readLine();
	    
	    System.out.println(str);
	    if (Integer.parseInt(str) == 0) {
		System.out.println("clear back-up result!");
	    } else if (Integer.parseInt(str) == 1) { // error handling
		System.out.println("send the result to the master node");
		sendResult(s);
	    }

	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    /**
     * send the result through the given socket
     * 
     * @param socket
     *            you can communicate with master through this socket
     */
    public void sendResult(Socket socket) {

	try {
	    OutputStreamWriter dOut = new OutputStreamWriter(
		    socket.getOutputStream());
	    BufferedReader br = new BufferedReader(new FileReader(
		    mTask.getJobID() + "_result0"));
	    String str = "";
	    while ((str = br.readLine()) != null) {
		dOut.write(str + "\n");
		dOut.flush();
	    }
	    br.close();
	    dOut.close();
	    socket.close();
	} catch (UnknownHostException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}

    }

    /**
     * This class will instantiate a thread that keeps listining to the mapper
     * nodes
     */
    public class ConnectionService implements Runnable {
	Socket socket;
	ServerSocket ss;

	public ConnectionService(ServerSocket ss) {
	    this.ss = ss;
	}

	@Override
	public void run() {
	    while (true) {
		try {
		    socket = ss.accept();
		    AcceptDataService as = new AcceptDataService(socket);
		    new Thread(as).start();
		} catch (IOException e) {
		    e.printStackTrace();
		}
	    }
	}
    }

    public class AcceptDataService implements Runnable {
	Socket socket;

	public AcceptDataService(Socket s) {
	    socket = s;
	}

	@Override
	public void run() {
	    try {
		BufferedReader br = new BufferedReader(new InputStreamReader(
			socket.getInputStream()));
		String str = "";
		while ((str = br.readLine()) != null) {
		    String[] strArr = str.split("\t");
		    List<String> tmpList = map.get(strArr[0]);
		    if (tmpList == null) {
			tmpList = new ArrayList<String>();
			tmpList.add(strArr[1]);
			map.put(strArr[0], tmpList);
		    } else {
			tmpList.add(strArr[1]);
		    }
		}
	    } catch (IOException e) {
		e.printStackTrace();
	    }

	}
    }

    /**
     * Run the reducer function
     */
    public void execute() {
	Process pro;
	try {
	    System.out.println("ready for execution");
	    Thread.sleep(10000);
	    pro = Runtime.getRuntime().exec("javac " + reducerClass + ".java"); // compile
	    pro.waitFor();
	    Class<?> myClass = Class.forName(reducerClass);
	    Class<?>[] paramsClass = new Class<?>[3];
	    paramsClass[0] = String.class;
	    paramsClass[1] = List.class;
	    paramsClass[2] = Output.class;
	    Constructor<?> myCons = myClass.getConstructor();
	    Object object = myCons.newInstance();
	    Method method = null;

	    method = object.getClass().getMethod(reducerFunction, paramsClass);
	    Output output = new Output(1, mTask.getJobID() + "_result");

	    for (String key : map.keySet()) {
		method.invoke(object, key, map.get(key), output);
	    }
	    output.close();

	} catch (Exception e) {
	    e.printStackTrace();
	}

    }

    /**
     * download java file from the master node
     */
    public void downloadExec() {
	try {
	    Socket s = reducerToMaster.accept();
	    Utility.downloadExec(s);
	} catch (IOException e) {
	    e.printStackTrace();
	}

    }

    /**
     * Instantiae another thread to keep track the current status of the system
     */
    public void statusReportThread() {
	try {
	    socketStatus = new ServerSocket(statusPort);
	    status = new Status(socketStatus);
	    Thread t = new Thread(status);
	    t.start();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public static void main(String[] args) {
	mapperPort = Integer.parseInt(args[0]);
	masterPort = Integer.parseInt(args[1]);
	statusPort = Integer.parseInt(args[2]);

	ReducerClient client = new ReducerClient();

	client.openSocket(); // create a socket for listenting to the mapper
			     // node
	client.statusReportThread(); // start another server to respond the
				     // status request

	while (true) {
	    status.setStatus(true);
	    System.out.println("[Reducer] Status: idle, wating for execution");
	    map.clear();
	    client.getInitialInfo();
	    status.setStatus(false);
	    System.out.println("[Reducer] Status: busy, start executing");
	    client.downloadExec();
	    client.waitForMaster(); // wait for the message from master
	    client.execute(); // exec
	    status.setStatus(true);
	    client.ackMaster(); // tell master that you are done
	    System.out.println("[Reducer] Status: finish execution");
	}
    }

}
