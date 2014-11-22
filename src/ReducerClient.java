import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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

public class ReducerClient {
    
    /*
     * 2014/11/18 haopingh
     */
    private static final int statusPort = 7070;
    private ServerSocket socketStatus;
    private boolean isIdle = true;
    

    private ServerSocket reducerToMapper, reducerToMaster;
    private static int port2mapper = 2000, port2master = 2002;
    private String reducerClass = "TestReducer", reducerFunction = "reduce"; //mapperClass = run
    private Map <String, List <String>> map;


    
    public ReducerClient(){
	map = new HashMap <String, List<String>>();
    }
    
    /**
     * open a socket for connection to the master
     */
    public void openSocket() {
	try {
	    reducerToMapper = new ServerSocket(port2mapper);
	    reducerToMaster = new ServerSocket(port2master);
	    
	    ConnectionService cs = new ConnectionService(reducerToMapper);
	    new Thread(cs).start();
	} catch (IOException e) {
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
		ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		BufferedReader br = new BufferedReader(new InputStreamReader(ois));
		String str = "";
		while ((str = br.readLine()) != null) {
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
    
    /**
     *  2014/11/18 haopingh
     */
    public void statusReportThread() {
	try {
	    socketStatus = new ServerSocket(statusPort);
	    Thread t = new Thread(new StatusReportThread(socketStatus));
	    t.start();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    private class StatusReportThread implements Runnable {
	ServerSocket mServer = null;
	
	public StatusReportThread(ServerSocket s) {
	    mServer = s;
	}

	@Override
	public void run() {
	    while(true) {
		try {
		    Socket request = mServer.accept();
		    
		    DataInputStream dis = new DataInputStream(request.getInputStream());
		    DataOutputStream dos = new DataOutputStream(request.getOutputStream());
		    String s = dis.readUTF();
		    
		    if (s.equals("status")) dos.writeUTF(isIdle ? "idle" : "busy");
		    dos.flush();
		    request.close();
		} catch (IOException e) {
		    e.printStackTrace();
		}
	    }
	}
    }
    
    public static void main (String [] args) {
	ReducerClient client = new ReducerClient();
	
	/* Start another server to respond the status request */
	client.statusReportThread();
	
	client.openSocket();//create a socket for listenting to the mapper node
	client.ackMaster(); //wait for the message from master
	client.execute(); //exec
    }
    
}
