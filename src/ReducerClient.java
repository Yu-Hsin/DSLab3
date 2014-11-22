import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import Utils.Utility;

public class ReducerClient {
    
    private static int mapperPort, masterPort, statusPort;
    
    
    private ServerSocket socketStatus;
    private MapReduceTask mTask;

    private ServerSocket reducerToMapper, reducerToMaster;
    
    private String reducerClass, reducerFunction; //mapperClass = run
    private static Map <String, List <String>> map;


    
    public ReducerClient(){
	map = new HashMap <String, List<String>>();
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
	reducerClass = mTask.getMapperClass();
	reducerFunction = mTask.getMapperFunc();
    }
    
    public void getInitialInfo() {
	try {
	    
	    Socket socket = reducerToMaster.accept();
	    System.out.println("What the hack");
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
    
     
    public void ackMaster() {	
	    try {
		reducerToMaster.accept();
	    } catch (IOException e) {
		e.printStackTrace();
	    }
    }
	        
    
    
    //connection to the mapper
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
		//ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		String str = "";
		while ((str = br.readLine()) != null) {
		    System.out.println(str);
		    String [] strArr = str.split("\t");
		    List <String> tmpList = map.get(strArr[0]);
		    if (tmpList == null) {
			tmpList = new ArrayList <String>();
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
    
    public void execute() {
	Process pro;
	try {
	    pro = Runtime.getRuntime().exec("javac " + reducerClass + ".java" ); //compile
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
	    Output output = new Output(1, "Result");
	    
	    for (String key : map.keySet()) {
		method.invoke(object, key, map.get(key), output);
	    }
	    output.close();
	    
	} catch (Exception e) {
	    e.printStackTrace();
	}
	
    }
    
    public void downloadExec() {
	Utility.downloadExec(reducerToMaster);
    }
    
    public void statusReportThread() {
	try {
	    socketStatus = new ServerSocket(statusPort);
	    Thread t = new Thread(new Status(socketStatus));
	    t.start();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }
    
        
    public static void main (String [] args) {
	mapperPort = Integer.parseInt(args[0]);
	masterPort = Integer.parseInt(args[1]);
	statusPort = Integer.parseInt(args[2]);
	
	ReducerClient client = new ReducerClient();
	
	client.openSocket();//create a socket for listenting to the mapper node
	client.statusReportThread(); //start another server to respond the status request
	client.getInitialInfo();
	client.downloadExec();	
	client.ackMaster(); //wait for the message from master
	client.execute(); //exec
    }
    
}
