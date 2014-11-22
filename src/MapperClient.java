import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import Utils.Utility;

public class MapperClient {

    private static int portToMaster;
    private ServerSocket socketToMaster;

    private static int statusPort;
    private ServerSocket socketStatus;

    private MapReduceTask mTask;
    private Status status;
    private String mapperClass, mapperFunction;
    private int numReducer;
    private String[] reducerIP, backReducerIP;
    private int[] reducerPort, backReducerPort;

    private String jobID;
    private String localFnName = "";

    private Socket toMasterSocket;

    /**
     * open a socket for connection to the master
     */
    public void openSocket() {
	try {
	    socketToMaster = new ServerSocket(portToMaster);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public void setTask(MapReduceTask m) {
	backReducerIP = mTask.getBackReducerIP();
	backReducerPort = mTask.getBackReducerPortToMapper();
	numReducer = mTask.getReducerNum();
	mapperClass = mTask.getMapperClass();
	mapperFunction = mTask.getMapperFunc();
	reducerIP = mTask.getReducerIP();
	jobID = mTask.getJobID();
	reducerPort = mTask.getReducerPortToMapper();
    }

    public void getInitialInfo() {
	try {
	    Socket socket = socketToMaster.accept();

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
     * download the file from the master node
     */
    public void downloadFile() {
	try {
	    Socket socket = socketToMaster.accept();
	    ObjectInputStream inputStream = new ObjectInputStream(
		    socket.getInputStream());
	    BufferedReader br = new BufferedReader(new InputStreamReader(
		    inputStream));
	    String str = "";

	    str = br.readLine(); // read the number of the split file
	    String fnName = String.format("%04d", Integer.parseInt(str));
	    BufferedWriter bw = new BufferedWriter(new FileWriter(jobID
		    + "_Part-" + fnName));
	    localFnName = jobID + "_Part-" + fnName;
	    while ((str = br.readLine()) != null) {
		bw.write(str);
		bw.write("\n");
	    }
	    bw.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    /**
     * download the executable file from the master node
     */
    public void downloadExec() {
	try {
	    toMasterSocket = socketToMaster.accept();
	    Utility.downloadExec(toMasterSocket);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

    }

    public void execute() {

	Process pro;
	try {
	    pro = Runtime.getRuntime().exec("javac " + mapperClass + ".java"); // compile
	    pro.waitFor();
	    Class<?> myClass = Class.forName(mapperClass);
	    Class<?>[] paramsClass = new Class<?>[2];
	    paramsClass[0] = String.class;
	    paramsClass[1] = Output.class;
	    Constructor<?> myCons = myClass.getConstructor();
	    Object object = myCons.newInstance();
	    Method method = null;
	    Output output = new Output(numReducer, jobID + "_mapper");
	    method = object.getClass().getMethod(mapperFunction, paramsClass);

	    BufferedReader br = new BufferedReader(new FileReader(localFnName));
	    String str = "";
	    while ((str = br.readLine()) != null) {
		method.invoke(object, str, output);
	    }
	    br.close();
	    output.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public void ackMaster() {
	try {

	    DataOutputStream dout = new DataOutputStream(
		    toMasterSocket.getOutputStream());
	    dout.writeUTF("OK");
	    dout.flush();
	    dout.close();
	    toMasterSocket.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public void distribute() {
	// send files to the reducer
	/*
	for (int i = 0; i < numReducer; i++) {
	    try {
		Socket socket = new Socket(reducerIP[i], reducerPort[i]);

		OutputStreamWriter dOut = new OutputStreamWriter(
			socket.getOutputStream());
		BufferedReader br = new BufferedReader(new FileReader(jobID
			+ "_mapper" + i));
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
	}*/
	sendToReducer(reducerIP, reducerPort);
	sendToReducer(backReducerIP, backReducerPort);
	ackMaster();
    }
    
    
    public void sendToReducer(String [] inputIP, int [] inputPort) {
	for (int i = 0; i < inputIP.length; i++) {
	    try {
		Socket socket = new Socket(inputIP[i], inputPort[i]);

		OutputStreamWriter dOut = new OutputStreamWriter(
			socket.getOutputStream());
		BufferedReader br = new BufferedReader(new FileReader(jobID
			+ "_mapper" + i));
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
    }

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
	portToMaster = Integer.parseInt(args[0]);
	statusPort = Integer.parseInt(args[1]);
	MapperClient client = new MapperClient();
	client.openSocket(); //create a socket for listenting to the master
	client.statusReportThread(); //start another server to respond the status request
	while (true) {
	    
	    System.out.println("[Mapper] Status: idle, wating for execution");
	    client.getInitialInfo();
	    System.out.println("[Mapper] Status: busy, start executing");
	    client.downloadFile(); //download the split file from the master
	    client.downloadExec(); //download the java file from the master
	    client.execute(); //execute the java file, generate intermediate files
	    client.distribute(); //send same keys to same reducers
	    System.out.println("[Mapper] Status: finish execution");
	}
    }

}
