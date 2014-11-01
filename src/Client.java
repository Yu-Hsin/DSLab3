import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Client {

    private ObjectInputStream inputStream;
    private ServerSocket socketclient;
    private static int port2client;

    // open a socket for connetion to the master
    public void openSocket() {
	try {
	    socketclient = new ServerSocket(port2client);
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public void downloadFile() {
	try {
	    Socket socket = socketclient.accept();
	    inputStream = new ObjectInputStream(socket.getInputStream());
	    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
	    String str = "";
	    while ((str = br.readLine()) != null) {
		System.out.println(str);
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }


    public static void main(String[] args) {
	Client client = new Client();
	client.openSocket();
	client.downloadFile();
    }

}
