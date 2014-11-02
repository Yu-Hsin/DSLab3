import java.io.*;
import java.net.*;

public class Master {

    public static void main(String[] args) {
	try {
	    Client mSocketRunnable = new Client(new Socket(args[0],
		    Integer.valueOf(args[1])), args[2]);
	    Thread t = new Thread(mSocketRunnable);
	    t.start();

	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public static class Client implements Runnable {
	private Socket mSocket;
	private String filename;

	public Client(Socket s, String fname) {
	    mSocket = s;
	    filename = fname;
	}

	@Override
	public void run() {

	    try {
		ObjectOutputStream out = new ObjectOutputStream(
			mSocket.getOutputStream());
		byte[] buffer = new byte[1024];

		int count = 0;
		BufferedInputStream bin = new BufferedInputStream(
			new FileInputStream(filename));
		while ((count = bin.read(buffer)) > 0) {
		    out.write(buffer, 0, count);
		    out.flush();
		}

		bin.close();
		mSocket.close();
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	}
    }

}
