package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	private ContentResolver mContentResolver;
	private Uri mUri;
	private int msgKey = 0;
	private String localPort;
	private String localHash;
	private HashMap<String,String> localMsgKey = new HashMap<String,String>();
	private HashMap<String,String> waitingQuery = new HashMap<String,String>();
	private boolean wait = true;
	private TreeMap<String,String> allPortHash = new TreeMap<String, String>();
	private int totalPorts = 5;
	private String[] linkedPorts = new String[totalPorts];
	private String deadAvd = null;
	boolean revived = false;

	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("*")){
			deleteAll();
			for(Map.Entry<String,String> m:  allPortHash.entrySet()){
				String portToDelete = m.getValue();
				if(portToDelete.equals(localPort)){
					continue;
				}
				String msgToSend = selection+"|"+localPort+"|"+"DELETE";
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, portToDelete);
			}
		}
		else if(selection.equals("@")){
			deleteAll();
		}
		else{
			String[] portsToDelete = new String[3];
			int portNum1 = bestPort(selection);
			portsToDelete [0] = linkedPorts[portNum1];
			int portNum2 = bestNext(portNum1);
			portsToDelete [1] = linkedPorts[portNum2];
			int portNum3 = bestNext(portNum2);
			portsToDelete [2] = linkedPorts[portNum3];

			for(int p = 0; p<portsToDelete.length; p++){
				if(portsToDelete[p].equals(localPort)){
					getContext().deleteFile(selection);
					localMsgKey.remove(selection);
					Log.v("delete","deleting Successful for "+selection);

				}
				else{
					Log.v("delete","sending to for deleting key "+selection);
					String msgToSend = selection+"|"+localPort+"|"+"DELETE";
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, portsToDelete[p]);
				}
			}
		}
		return 0;
	}

	public void deleteAll(){
		String[] allFiles = getContext().fileList();
		for(int i = 0; i<allFiles.length; i++){
			String filename = allFiles[i];
			getContext().deleteFile(filename);
			Log.v("deleteAll","deleting successfull "+filename);
		}
		localMsgKey = new HashMap<String, String>();

	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String k = "";
		String v = "";
		Set<Map.Entry<String, Object>> allVals = values.valueSet();
		for(Map.Entry<String,Object> ent: allVals){
			String elem = ent.getKey();
			Log.v("insert","here getkey is "+elem);
			if(elem.equals("key")){
				k = ent.getValue().toString();
			}
			else{
				v = ent.getValue().toString();
			}
		}

		String[] portsToSave = new String[3];
		int portNum1 = bestPort(k);
		portsToSave [0] = linkedPorts[portNum1];
		int portNum2 = bestNext(portNum1);
		portsToSave [1] = linkedPorts[portNum2];
		int portNum3 = bestNext(portNum2);
		portsToSave [2] = linkedPorts[portNum3];
		for(int p =0; p<portsToSave.length; p++){
			if(portsToSave[p].equals(localPort)){
				Log.v("insert","here in local port for key "+k);
				finalInsert(k,v,portsToSave[0]);
			}
			else{
				Log.v("insert","sending to another port for key "+k);
				String msgToSend = k+"|"+v+"|"+"INSERT"+"|"+portsToSave[0];
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, portsToSave[p]);
			}
		}
		return uri;
	}

	public void finalInsert(String key, String value, String bestPort){
		String filename = key;
		String strTowrite = value + "\n";
		FileOutputStream outputStream;
		try {
			outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
			outputStream.write(strTowrite.getBytes());
			outputStream.close();
			localMsgKey.put(key,bestPort);
			Log.v("insert","key is "+key+" Value is "+value+" and filename "+filename);
		} catch (Exception e) {
			Log.e("insert writing", "File write failed");
		}
	}


	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		/*My code begins */
		Log.v("Content Provider","I am being visited Content onCreate");

		mContentResolver = getContext().getContentResolver();
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
		uriBuilder.scheme("content");
		mUri = uriBuilder.build();

		/*Hack by Steve ko for creating AVD connection
		 */
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		Log.v("in Provider",myPort);
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			Log.v(TAG, "Next line with issue");
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}

		/*Sending off the message that avd -myPort- is live*/
		try {
			String portHash = genHash(portStr);
			localPort = myPort;
			localHash = portHash;
			//allPortHash.put(portHash,localPort);
			for(int i = 0; i<totalPorts; i++){
				String hash = genHash(""+(5554+i*2));
				String p = ""+(11108+i*4);
				allPortHash.put(hash,p);
			}
			/*Call client to check if it is revived*/
			Log.v("Create","allPorthash size "+allPortHash.size());
			deleteAll();
            Log.v("Create","Cleaned up the old files ");

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portHash, myPort);
		} catch(NoSuchAlgorithmException nsae){
			Log.v("on create","No such algo error");
		}

		return false;
	}

	public int bestPort(String k){
		try{
			linkedPorts =  new String[allPortHash.size()];
			String key = genHash(k);
			int best = -1;
			int i = 0;
			for(Map.Entry<String,String> port : allPortHash.entrySet()){
				if(key.compareTo(port.getKey()) < 0){
					if(best == -1){
						Log.v("bestPort","hash: "+port.getKey()+" port: "+port.getValue());
						best = i;
					}
				}
				linkedPorts[i] = port.getValue();
				i++;
			}
			if(best < 0){
				return 0;
			}
			return best;
		} catch(NoSuchAlgorithmException nsae){
			Log.v("bestPort","NSAE exception");
			return -1;
		}
	}

	public int bestNext(int port){
		int retval = port+1;
		if(retval>=totalPorts){
			return 0;
		}
		return retval;
	}

	public int bestPrev(int port){
		int retval = port-1;
		if(retval < 0){
			return totalPorts-1;
		}
		return retval;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		// TODO Auto-generated method stub
		MatrixCursor mc = null;
		String filename = selection;
		Log.v("selection", "File name is "+selection);
		if(selection.equals("*")){
			mc = new MatrixCursor(new String[]{"key","value"});
			queryAll(mc);
			for(Map.Entry<String,String> m:  allPortHash.entrySet()){
				String portToQuery = m.getValue();
				if(portToQuery.equals(localPort)){
					continue;
				}
				String msgToSend = selection+"|"+localPort+"|"+"QUERY";
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, portToQuery);

				while(wait){
					//Wait for all the query to be received
					//Log.v("query","Looping.......");
					if(deadAvd!=null && deadAvd.equals(portToQuery)){
						Log.v("query","Dead Avd detected "+deadAvd);
						wait = false;
					}
				}
				for(Map.Entry<String,String> me : waitingQuery.entrySet()){
					mc.addRow(new String[]{me.getKey(),me.getValue()});
				}
				//Removed all the saved values from it
				wait = true;
				waitingQuery = new HashMap<String, String>();

			}
		}
		else if(selection.equals("@")){
			mc = new MatrixCursor(new String[]{"key","value"});
			queryAll(mc);
		}
		else{
			String[] portsToQuery = new String[3];
			int portNum1 = bestPort(selection);
			portsToQuery [0] = linkedPorts[portNum1];
			int portNum2 = bestNext(portNum1);
			portsToQuery [1] = linkedPorts[portNum2];
			int portNum3 = bestNext(portNum2);
			portsToQuery [2] = linkedPorts[portNum3];

			for(int p =0; p<2; p++){
				if(deadAvd!=null && deadAvd.equals(portsToQuery[p])){
					continue;
				}
				if(portsToQuery[p].equals(localPort)){
					String val = finalQuery(selection);
					mc = new MatrixCursor(new String[]{"key","value"});
					String data[] = new String[2];
					data[0] = filename;
					data[1] = val;
					mc.addRow(data);
					Log.v("query", "from key "+filename);
					Log.v("query", "value "+val);
					Log.v("query","message retrived successfully");
				}
				else{
					Log.v("query","asking port "+portsToQuery[p]+" for value of "+selection);
					String msgToSend = selection+"|"+localPort+"|"+"QUERY";
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, portsToQuery[p]);
					String value;

					while(waitingQuery.get(selection) == null){
						//Wait for the query to be received
						//Log.v("query","Looping.......");
						if(deadAvd!=null && deadAvd.equals(portsToQuery[p])){
							break;
						}
					}
					if(waitingQuery.get(selection)==null){
					    continue;
                    }
					value = waitingQuery.get(selection);
					waitingQuery.remove(selection);
					mc = new MatrixCursor(new String[]{"key","value"});
					String data[] = new String[2];
					data[0] = filename;
					data[1] = value;
					mc.addRow(data);
					Log.v("query","Done querying from port "+portsToQuery[p]);
				}

				break;
			}
		}

		return mc;
	}

	public String finalQuery(String filename){
		String val = null;
		try {
			FileInputStream inputStream;
			inputStream = getContext().openFileInput(filename);
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
			val = br.readLine();
			br.close();
			inputStream.close();
		} catch (IOException e) {
			Log.e("in query", "File reading failed");
			e.printStackTrace();
		}
		return val;
	}

	public String queryAll(MatrixCursor mc){
		String retval = "";
		/*if(localMsgKey.isEmpty()){
			return retval;
		}
		for(Map.Entry<String,String> me : localMsgKey.entrySet()){
			String[] data = new String[]{me.getKey(),finalQuery(me.getKey())};
			if(mc == null){
				retval += data[0]+"&"+data[1]+"$";
			}
			else{
				mc.addRow(data);
			}
		}*/
		String[] allFiles = getContext().fileList();
		if(allFiles.length == 0 || localMsgKey.isEmpty()){
			return retval;
		}
		for(int i = 0; i<allFiles.length; i++){
			String[] data = new String[]{allFiles[i],finalQuery(allFiles[i])};
			if(mc == null){
				retval += data[0]+"&"+data[1]+"$";
			}
			else{
				mc.addRow(data);
			}
		}
		return retval;
	}

	public String querySome(String portNum){
		String retval = "";
		/*for(Map.Entry<String,String> me : localMsgKey.entrySet()){
			if(me.getValue().equals(portNum)){
				retval+=me.getKey()+"&"+finalQuery(me.getKey())+"&"+me.getValue()+"$";
			}
		}*/
		String[] allFiles = getContext().fileList();
		if(allFiles.length == 0 || localMsgKey.isEmpty()){
			return retval;
		}
		for(int i = 0; i<allFiles.length; i++){
			if(localMsgKey.get(allFiles[i]).equals(portNum)){
				String[] data = new String[]{allFiles[i],finalQuery(allFiles[i]),localMsgKey.get(allFiles[i])};
				retval += data[0]+"&"+data[1]+"&"+data[2]+"$";
			}
		}
		return retval;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	/*Server Task is also AsyncTask*/
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			Log.v("doInBackgroung","VISITED");
			/*Send to onProgressUpdate().*/
			Log.v("while loop","VISITED");
			ServerSocket serverSocket = sockets[0];
			try {
				while(true){
					Socket sS = serverSocket.accept();
					DataInputStream dis = new DataInputStream(sS.getInputStream());
					String msg = dis.readUTF();

					// Try write to catch failure
					DataOutputStream dummy = new DataOutputStream(sS.getOutputStream());
					dummy.writeUTF("Hello");
					dummy.flush();

					String[] strArr = msg.split(Pattern.quote("|"));
					if(strArr.length > 2){
						Log.v("serversocket",strArr[0]+" type "+strArr[2]);
						if(strArr[2].equals("INSERT")){
							finalInsert(strArr[0],strArr[1],strArr[3]);
						}
						else if(strArr[2].equals("QUERY")){
							int portNum = Integer.parseInt(strArr[1]);
							String msgToSend = "";
							if(strArr[0].equals("*")){
								msgToSend = strArr[0]+"|"+queryAll(null)+"|"+"QUERYVALUE";
							}
							else {
								msgToSend = strArr[0] + "|" + finalQuery(strArr[0]) + "|" + "QUERYVALUE";
							}
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portNum);
							DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
							dos.writeUTF(msgToSend);
							dos.flush();
							Log.v("query","sent required querying value");
						}
						else if(strArr[2].equals("QUERYVALUE")){
							if(strArr[0].equals("*")){
								if(strArr[1].isEmpty()){
									wait = false;
									continue;
								}
								String[] pairs = strArr[1].split(Pattern.quote("$"));
								for(int i = 0; i <pairs.length; i++){
									String[] kv = pairs[i].split(Pattern.quote("&"));
									Log.v("query values",kv[0]+" "+kv[1]);
									waitingQuery.put(kv[0],kv[1]);
								}
								wait = false;
							}
							else{
								Log.v("query value","done querying value");
								waitingQuery.put(strArr[0],strArr[1]);
							}
						}
						else if(strArr[2].equals("DELETE")){
							if(strArr[0].equals("*")){
								deleteAll();
							}
							else{
								getContext().deleteFile(strArr[0]);
								localMsgKey.remove(strArr[0]);
							}
						}
						else if(strArr[2].equals("PORTS")){
							String[] ports = strArr[0].split(Pattern.quote("$"));
							for(int i =0; i<ports.length; i++){
								String[] kv = ports[i].split(Pattern.quote("&"));
								Log.v("serversocket","before if ports "+kv[1]);
								if(!allPortHash.containsKey(kv[0])){
									Log.v("serversocket","inside if ports "+kv[1]);
									allPortHash.put(kv[0],kv[1]);
								}
							}
							Log.v("serversocket", "size of allPorthash " + allPortHash.size());
						}
						else if(strArr[2].equals("DEAD")){
							Log.d("serversocket","new dead port "+strArr[0]);
							deadAvd = strArr[0];
						}
						else if(strArr[2].equals("REVIVE")){
							if(deadAvd == null){
								Log.v("ServerSocket","New Port is active "+strArr[0]);
								continue;
							}
							deadAvd = null;
							if(strArr[3].equals("JUST")){
								continue;
							}
							Log.d("serversocket","Begin to revive port "+strArr[0]);
							String msgToSend ="";
							if(strArr[3].equals("NEXT")){
								msgToSend = localPort+"|"+querySome(strArr[0])+"|"+"REVIVEDATA";
							}
							else if(strArr[3].equals("PREV")){
								bestPort(strArr[0]);
								for(int i = 0; i<linkedPorts.length; i++){
									if(linkedPorts[i].equals(localPort)){
										msgToSend = localPort+"|"+querySome(linkedPorts[bestPrev(i)])
												+querySome(localPort)+"|"+"REVIVEDATA";
										break;
									}
								}
							}
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(strArr[0]));
							DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
							dos.writeUTF(msgToSend);
							dos.flush();
							Log.v("Reviving","REVIVEDATA sending forward "+strArr[0]);
						}
						else if(strArr[2].equals("REVIVEDATA")){
							String[] kv = strArr[1].split(Pattern.quote("$"));
							if(kv.length<1){
								Log.v("Reviving data","No data here");
								continue;
							}
							for(int i = 0; i<kv.length; i++){
								String[] data = kv[i].split(Pattern.quote("&"));
								if(data.length > 2){
									Log.v("Received data","from "+strArr[0]+" "+kv[i]);
									finalInsert(data[0],data[1],data[2]);
								}
							}
							Log.v("Reviving data","completed reviving");
						}
					}
					else {
						Log.v("serversocket","Weird Case");
					}
				}
			} catch(SocketTimeoutException ste){
				Log.e("SERVER","socket timeout exception ");
			} catch (UnknownHostException e) {
				Log.e("SERVER", "socket UnknownHostException ");
			} catch (IOException e) {
				Log.e("SERVER", "socket IOException ");
				e.printStackTrace();
			} catch (Exception e) {
				Log.e("SERVER", "socket Exception");
				e.printStackTrace();
			}
			return null;
		}

		protected void onProgressUpdate(String...strings) {
			/*
			 * The following code displays what is received in doInBackground().
			 */
			String strReceived = strings[0].trim();
			Log.v("progress","Our received message is "+strReceived+" <-should be here");

			/*Code to save messages in content resolver or provider*/
			ContentValues cv = new ContentValues();
			cv.put("key",msgKey+"");
			msgKey++;
			cv.put("value",strReceived);
			mContentResolver.insert(mUri,cv);
			Log.v(TAG, "saving message went through");
			return;
		}
	}

	public void callClient(String portNum, String type){
		if(type.equals("DEAD")){
			String msgToSend = portNum+"|"+localPort+"|"+type;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, portNum);
		}
	}

	/*CLIENT Task is an AsyncTask to perform sending message */
	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			Log.v("msg to send",msgs[0]);
			Log.v("what port to send ",msgs[1]);
			try {
				String[] strArr = msgs[0].split(Pattern.quote("|"));
				if(strArr.length > 1){
					if(strArr[2].equals("DEAD")){
						for(Map.Entry<String,String> ms : allPortHash.entrySet()){
							if(deadAvd.equals(ms.getValue())){
								continue;
							}
							int portNum = Integer.parseInt(ms.getValue());
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portNum);
							String msgToSend = msgs[0]+"|"+msgs[1];
							DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
							dos.writeUTF(msgToSend);
							dos.flush();
						}
					}
					else{
						int portNum = Integer.parseInt(msgs[1]);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portNum);
						DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
						dos.writeUTF(msgs[0]);
						dos.flush();
						//Try to read now
						DataInputStream dis = null;
						dis = new DataInputStream((socket.getInputStream()));
						String X = null;
						try{
							if(dis != null){
								X = dis.readUTF();
							}
						} catch(IOException e){
							Log.e("Client->read Exception","Caught at "+portNum);
							if(deadAvd == null){
								Log.e("Client->Dead Exception","Caught first time for "+portNum);
								deadAvd = ""+portNum;
								callClient(""+portNum,"DEAD");
							}
						}
						socket.close();
					}
					}

				else{
					Log.v("Create Network","Sending message to avd1 and avd0 from "+msgs[1]);
					bestPort(msgs[1]);
					int next = -1;
					int prev = -1;
					for(int i = 0; i<linkedPorts.length; i++){
						if(linkedPorts[i].equals(msgs[1])){
							int revPort = i;
							prev = Integer.parseInt(linkedPorts[bestPrev(revPort)]);
							next = Integer.parseInt(linkedPorts[bestNext(revPort)]);
							break;
						}
					}

					for(int i = 0; i<linkedPorts.length; i++){
						String revMsg = "";
						int portNum = Integer.parseInt(linkedPorts[i]);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portNum);
						DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
						if(linkedPorts[i].equals(""+prev)){
							revMsg = msgs[1]+"|"+localPort+"|"+"REVIVE"+"|"+"PREV";
							Log.v("reviving","for Prev node "+prev);
						}
						else if(linkedPorts[i].equals(""+next)){
							revMsg = msgs[1]+"|"+localPort+"|"+"REVIVE"+"|"+"NEXT";
							Log.v("reviving","for Next node "+next);
						}
						else{
							revMsg = msgs[1]+"|"+localPort+"|"+"REVIVE"+"|"+"JUST";
						}
						dos.writeUTF(revMsg);
						dos.flush();
					}
				}

			} catch(SocketTimeoutException ste){
				Log.e("CLIENT","socket timeout exception "+msgs[1]);
			} catch (UnknownHostException e) {
				Log.e("CLIENT", "ClientTask UnknownHostException "+msgs[1]);
			} catch (IOException e) {
				Log.e("CLIENT", "ClientTask socket IOException "+msgs[1]);
			} catch (Exception e) {
				Log.e("CLIENT", "socket Exception "+msgs[1]);
				e.printStackTrace();
			}
			return null;
		}
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}
