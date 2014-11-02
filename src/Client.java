import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Client {

    private ObjectInputStream inputStream;
    private ServerSocket socketclient;
    private static int port2client = 2000;

    /**
     * open a socket for connection to the master
     */
    public void openSocket() {
	try {
	    socketclient = new ServerSocket(port2client);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    /**
     * download the file from the master node
     */
    public void downloadFile() {
	try {
	    Socket socket = socketclient.accept();
	    inputStream = new ObjectInputStream(socket.getInputStream());
	    BufferedReader br = new BufferedReader(new InputStreamReader(
		    inputStream));
	    String str = "";
	    str = br.readLine(); // read the number of the split file
	    String fnName = String.format("%04d", Integer.parseInt(str));
	    BufferedWriter bw = new BufferedWriter(new FileWriter(fnName));
	    while ((str = br.readLine()) != null) {
		System.out.println(str);
		bw.write(str);
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
	    Socket socket = socketclient.accept();
	    inputStream = new ObjectInputStream(socket.getInputStream());
	    BufferedReader br = new BufferedReader(new InputStreamReader(
		    inputStream));
	    String str = "";
	    String execName = br.readLine();
	} catch (IOException e) {
	    e.printStackTrace();
	}
	
    }

    public static void main(String[] args) {
	Client client = new Client();
	client.openSocket();   // create a socket for listenting to the master node
	client.downloadFile(); // download the split file from the master node
	//client.downloadExec();
    }

}
