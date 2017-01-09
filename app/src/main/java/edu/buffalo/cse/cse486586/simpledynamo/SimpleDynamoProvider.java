package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Exchanger;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String PROVIDER_NAME = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	static final String TABLE_NAME = SimpleDynamoDB.TABLE_NAME;
	static final String KEY_COLUM = SimpleDynamoDB.TABLE_COL_KEY;
	static final String URL = "content://" + PROVIDER_NAME;
	private static final Uri CONTENT_URI = Uri.parse(URL);
	private SimpleDynamoDB simpleDynamoDB;
	private static final UriMatcher uriMatcher =
			new UriMatcher(UriMatcher.NO_MATCH);
	static final int TABLE = 1;
	static final int ROW = 2;
	static final int SERVER_PORT = 10000;
	static String myPort = null;
	static Node myPredecessor = null;
	static Node mySuccessor = null;
	static String myAvdPort = null;
	static final String NODE_JOIN = "NODE_JOIN";
	static final String NODE_RECOVER = "NODE_RECOVER";
	static final String NODE_QUERY_MSG = "QUERY";
	static final String NODE_INSERT_MSG = "INSERT";
	static final String NODE_DELETE_MSG=  "DELETE";
	static final String NODE_JOIN_PORT = "11108";
	static final String NODE_QUERY_ALL = "QUERY_ALL";
	static final String NODE_QUERY_ALL_RESPONSE = "QUERY_ALL_RESPONSE";
	static final String NODE_DELETE_RESPONSE="DELETE_RESPONSE";
	static final String NODE_DELETE_ALL_RESPONSE = "DELETE_ALL_RESPONSE";
	static final String NODE_QUERY_RESPONSE="QUERY_RESPONSE";
	static final String NODE_DELETE_ALL = "DELETE_ALL";
	static final String NODE_INSERT_RESPONSE = "INSERT_RESPONSE";
	static final String NODE_RECOVER_SELF = "RECOVER_SELF";
	static final String NODE_INSERT_REPLICA = "INSERT_REPLICA";
	static final String ACKNOWLEDGEMENT = "ACKNOWLEDGEMENT";

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private static final String QUERY_ALL = "*";
	private static final String QUERY_ME = "@";
	static final String REMOTE_PORT0 = "5554";
	static final String REMOTE_PORT1 = "5556";
	static final String REMOTE_PORT2 = "5558";
	static final String REMOTE_PORT3 = "5560";
	static final String REMOTE_PORT4 = "5562";
	private static MatrixCursor matrixCursor = null;
	ActionLock insertLock = null;
	ActionLock deleteLock = null;
	ActionLock queryLock = null;
	ActionLock dbLock = null;
	ActionLock globalQueryLock = null;
	ActionLock globalDeleteLock = null;
	ActionLock recoveryLock = null;
	ActionLock replicaLock1 = null;
	ActionLock replicaLock2 = null;
	boolean queryResponse = false;
	boolean replicaRecovered = false;
	static List<String> avdList = new ArrayList<String>();
	static TreeMap<String, Node> chordMap = new TreeMap<String, Node>(new Comparator<String>() {
		@Override
		public int compare(String lhs, String rhs) {
			return lhs.compareTo(rhs);
		}
	});


	static {
		uriMatcher.addURI(PROVIDER_NAME, TABLE_NAME, TABLE);
		uriMatcher.addURI(PROVIDER_NAME, TABLE_NAME + "/#", ROW);
		avdList.add(getNetworkPort(REMOTE_PORT0));
		avdList.add(getNetworkPort(REMOTE_PORT1));
		avdList.add(getNetworkPort(REMOTE_PORT2));
		avdList.add(getNetworkPort(REMOTE_PORT3));
		avdList.add(getNetworkPort(REMOTE_PORT4));

	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.v("query", selection);
		SQLiteDatabase sqlDB = simpleDynamoDB.getWritableDatabase();
		int rowsDeleted = 0;
		try {
			if (selection.equals(QUERY_ME)) {
				Log.v(TAG, "Deleting myself: " + myAvdPort);
				rowsDeleted = sqlDB.delete(TABLE_NAME, "1", null);
			} else if (selection.equals(QUERY_ALL)) {
				Log.v(TAG, "Deleting myself for all: " + myAvdPort);
				rowsDeleted = sqlDB.delete(TABLE_NAME, "1", null);
				Message nodeMessage = new Message(selection);
				nodeMessage.setFromPort(myPort);
				nodeMessage.setOriginalRequester(getAvdPort(myPort));
				nodeMessage.setMessageType(NODE_DELETE_ALL);
				Log.d(TAG, "Sending delete all to successor: " + mySuccessor.getAvdPort());
				nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
				new ClientThread(nodeMessage).start();
				/*synchronized (globalDeleteLock) {
					try {
						globalDeleteLock.wait();
					} catch (InterruptedException ex) {
						Log.d(TAG, "Delete all Response received: " + globalDeleteLock.isResponseReceived);

					}
					rowsDeleted = globalDeleteLock.totalRowsDeleted;
					globalDeleteLock.isResponseReceived = false;
					globalDeleteLock.totalRowsDeleted = 0;
				}*/
			} else {
				Node owner = getKeyOwner(selection);
				Message nodeMessage = new Message(selection);
				nodeMessage.setFromPort(myPort);
				nodeMessage.setOriginalRequester(myAvdPort);
				nodeMessage.setMessageType(NODE_DELETE_MSG);
				//if key found in myavd return
				if (owner.getAvdPort().equals(myAvdPort)) {
					Log.d(TAG, "Delete Key: " + selection + " found in me: " + owner.getAvdPort());
					Log.d(TAG, "Sending delete key to successor: " + mySuccessor.getAvdPort());
					deleteSingle(selection);
					nodeMessage.setChainLevel(2);
					nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
				} else {
					Log.d(TAG, "Delete key: "+selection+" not found in me...Routing to successor: " + owner.getAvdPort());
					nodeMessage.setChainLevel(3);
					nodeMessage.setToPort(getNetworkPort(owner.getAvdPort()));
				}
				new ClientThread(nodeMessage).start();
				/*synchronized (deleteLock) {
					try {
						deleteLock.wait();
					} catch (InterruptedException ex) {
						Log.d(TAG, "Delete key Response received: " + deleteLock.isResponseReceived);

					}
					rowsDeleted = deleteLock.totalRowsDeleted;
					deleteLock.isResponseReceived = false;
					deleteLock.totalRowsDeleted = 0;
				}*/
			}
			if (rowsDeleted > 0) {
				Log.d(TAG, "Delete Succeeded");
			} else {
				Log.d(TAG, "Delete failed for key: " + selection);
			}
		} catch (Exception ex) {
			Log.d(TAG, "Delete failed for key: " + selection + " ,Exception: " + ex);
		}
		return rowsDeleted;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		try {
			SQLiteDatabase sqlDB = simpleDynamoDB.getWritableDatabase();
			String key = (String) values.get(KEY_FIELD);
			String value = (String) values.get(VALUE_FIELD);
			Node owner = getKeyOwner(key);
			if(owner!=null) {
				// notifying owner to insert msg
				Message nodeMessage = new Message(key);
				nodeMessage.setFromPort(myPort);
				nodeMessage.setOriginalRequester(myAvdPort);
				nodeMessage.setMessageType(NODE_INSERT_MSG);
				Map<String, String> contentValues = new HashMap<String, String>();
				contentValues.put(KEY_FIELD, key);
				contentValues.put(VALUE_FIELD, value);
				nodeMessage.setContentValues(contentValues);
				//if owner is me insert right away
				if(owner.getAvdPort().equals(myAvdPort)) {
					Log.d(TAG, "Insert key: " + key + " found in myself: " + owner.getAvdPort());
					Log.d(TAG, "Sending insert key: "+key+" to successor: " + mySuccessor.getAvdPort());
					insertIntoDb(uri, values);
					nodeMessage.setChainLevel(2);
					nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
				}
				else {
					Log.d(TAG, "Routing Insert key: " + key + " to owner : " + owner.getAvdPort());
					nodeMessage.setChainLevel(3);
					nodeMessage.setToPort(getNetworkPort(owner.getAvdPort()));
				}
				new ClientThread(nodeMessage).start();
				synchronized (insertLock) {
					try {
						insertLock.wait(2000);
					} catch (InterruptedException ex) {
						Log.d(TAG, "Oops...I am interrupted: " + ex.getMessage());
					}
					if(!insertLock.isResponseReceived){
						String[] ports = new String[]{owner.getAvdPort(), owner.getSuccessor().getAvdPort(), owner.getSuccessor().getSuccessor().getAvdPort()};
						for(String port : ports) {
							nodeMessage.setMessageType(NODE_INSERT_REPLICA);
							nodeMessage.setToPort(getNetworkPort(port));
							new ClientThread(nodeMessage).start();
						}
					}
					Log.d(TAG, "Insert key Response received: " + insertLock.isResponseReceived);
					insertLock.isResponseReceived = false;
				}
			}

		} catch (Exception ex) {
			Log.d(TAG, "Insert Row failed: " + values.toString() + " Exception: " + ex);
		}
		return uri;
	}

	private Uri insertIntoDb(Uri uri, ContentValues values) {
		try {
			SQLiteDatabase sqlDB = simpleDynamoDB.getWritableDatabase();
			//Log.d(TAG, "Inserting (key, value) " + values.toString() + "in avd: "+myAvdPort);
			synchronized (dbLock) {
				long rowId = sqlDB.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
				if (rowId > 0) {
					Log.d(TAG, "Insert row success, (key, value) " + "("+values.get(KEY_FIELD)+values.get(VALUE_FIELD)+")" + " RowId: " + rowId);
					getContext().getContentResolver().notifyChange(uri, null);
					return Uri.parse(TABLE_NAME + "/" + rowId);
				} else {
					throw new SQLException("Insert Row failed (key, value) : " + values.toString());
				}
			}
		} catch (Exception ex) {
			Log.d(TAG, "Insert Row failed (key, value): " + values.toString() + " Exception: " + ex);
		}
		return uri;
	}

	private Node getKeyOwner(String key){
		Iterator<Map.Entry<String, Node>> it = chordMap.entrySet().iterator();
		String predecessorNodeId = it.next().getKey();
		try {
			String hashedKey = genHash(key);
			while (it.hasNext()) {
				Map.Entry<String, Node> entry = it.next();
				String currentNodeId = entry.getKey();
				if (hashedKey.compareTo(predecessorNodeId)>0 && hashedKey.compareTo(currentNodeId)<=0) {
					return entry.getValue();
				}
				predecessorNodeId = currentNodeId;
			}
			if(hashedKey.compareTo(chordMap.lastKey())>0){
				return chordMap.firstEntry().getValue();
			}
			else if(hashedKey.compareTo(chordMap.firstKey())<=0){
				return chordMap.firstEntry().getValue();
			}
		}
		catch (NoSuchAlgorithmException ex){
			Log.d(TAG, "Hashing failed for key: "+key);
		}
		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		// If you need to perform any one-time initialization task, please do it here.
		myPort = getMyPort();
		myAvdPort = getAvdPort(myPort);
		insertLock = new ActionLock();
		deleteLock = new ActionLock();
		queryLock = new ActionLock();
		dbLock = new ActionLock();
		globalDeleteLock = new ActionLock();
		globalQueryLock = new ActionLock();
		recoveryLock = new ActionLock();
		//Log.d(TAG, "Initializing DB in provider...");
		Context context = getContext();
		simpleDynamoDB = new SimpleDynamoDB(context);
		//Initialising Dynamo....
		Log.d(TAG, "Node: " + myAvdPort + " is Online");
		try {
			chordMap.put(genHash(REMOTE_PORT0), new Node(REMOTE_PORT0));
			chordMap.put(genHash(REMOTE_PORT1), new Node(REMOTE_PORT1));
			chordMap.put(genHash(REMOTE_PORT2), new Node(REMOTE_PORT2));
			chordMap.put(genHash(REMOTE_PORT3), new Node(REMOTE_PORT3));
			chordMap.put(genHash(REMOTE_PORT4), new Node(REMOTE_PORT4));
		}
		catch(NoSuchAlgorithmException ex){
			Log.d(TAG, "Error in generating hash..."+ex.getMessage());
		}
		buildChord(chordMap);
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, new Object[]{serverSocket, this});
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}
		//printChord(chordMap);
		//I am alive guys...notifying my successor and predecessors...
		// wait for other ops to complete
		try {
			Thread.sleep(1500);
		}
		catch (Exception ex){

		}
		new NodeRecoveryTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,null);
		// wait for
		//waiting for node to recover
		/*synchronized (recoveryLock) {
			try {
				recoveryLock.wait();
			} catch (InterruptedException ex) {
				Log.d(TAG, "I'm interrupted: " + ex.getMessage());
			}
			Log.d(TAG, "Oops....I'm recovered...Response Received: "+recoveryLock.isResponseReceived);
			recoveryLock.isResponseReceived = false;
		}*/

		return simpleDynamoDB.getWritableDatabase() == null ? false : true;
	}


	private class NodeRecoveryTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... msgs) {
			Message multicastMessage = new Message(null);
			multicastMessage.setFromPort(myPort);
			multicastMessage.setMessageType(NODE_JOIN);
			for(String remotePort:avdList) {
				Socket socket = null;
				try {
					if (!remotePort.equals(getNetworkPort(myAvdPort))) {
						/*String avdPort = getAvdPort(remotePort);
						if (mySuccessor.getAvdPort().equals(avdPort)) {
							multicastMessage.setMessageType(NODE_RECOVER_SELF);
						} else if (myPredecessor.getAvdPort().equals(avdPort) || myPredecessor.getPredecessor().equals(avdPort)) {
							multicastMessage.setMessageType(NODE_RECOVER_REPLICA);
						}*/
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remotePort));
						//socket.setSoTimeout(500);
						OutputStream os = socket.getOutputStream();
						ObjectOutputStream oos = new ObjectOutputStream(os);
						oos.writeObject(multicastMessage);
						oos.flush();
						oos.close();
					}
				} catch (SocketTimeoutException ex) {
					Log.e(TAG+"-ClientMultiCastTask", "Client: " + remotePort + " is dead");
				} catch (UnknownHostException ex) {
					Log.e(TAG+"-ClientMultiCastTask", "Client: " + remotePort + " is not up yet");
				} catch (IOException ex) {
					Log.e(TAG+"-ClientMultiCastTask", "IOException: " + remotePort+" Message Type: "+multicastMessage.getMessageType()+" Exception: "+ex.getMessage());
				} catch (Exception ex) {
					Log.e(TAG+"-ClientMultiCastTask", "Exception: "+remotePort+" Message Type: "+multicastMessage.getMessageType()+" Exception: "+ex.getMessage());
				}
				finally{
					try{
						socket.close();
					}
					catch(Exception ex){
						//ex.printStackTrace();
					}
				}

			}
			return null;
		}
	}

	private void printChord(TreeMap<String, Node> chordMap){
		Iterator<Map.Entry<String, Node>> itr = chordMap.entrySet().iterator();
		while (itr.hasNext()) {
			Node currentNode = itr.next().getValue();
			Log.d(TAG, "Node: "+currentNode.getAvdPort()+" Node Predecessor: "+currentNode.getPredecessor().getAvdPort()+" Node Successor: "+currentNode.getSuccessor().getAvdPort());
		}
	}

	private void buildChord(TreeMap<String, Node> chordMap) {
		Iterator<Map.Entry<String, Node>> itr = chordMap.entrySet().iterator();
		while (itr.hasNext()) {
			Node currentNode = itr.next().getValue();
			updateSuccessorAndPredecessor(currentNode, chordMap);
		}
	}

	private void updateSuccessorAndPredecessor(Node node, TreeMap<String, Node> chordMap){
		Iterator<Map.Entry<String, Node>> itr = chordMap.entrySet().iterator();
		Node predecessor = null;
		int track = 1;
		while(itr.hasNext()){
			Node currentNode = itr.next().getValue();
			if(node.getNodeId().equals(currentNode.nodeId)) {
				if (itr.hasNext()) {
					currentNode.setSuccessor(itr.next().getValue());
				} else {
					currentNode.setSuccessor(chordMap.firstEntry().getValue());
				}
				if (track == 1) {
					currentNode.setPredecessor(chordMap.lastEntry().getValue());
				} else {
					currentNode.setPredecessor(predecessor);
				}
				if (currentNode.getAvdPort().equals(myAvdPort)) {
					mySuccessor = currentNode.getSuccessor();
					myPredecessor = currentNode.getPredecessor();
				}
				break;
			}
			predecessor = currentNode;
			track++;
		}
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
									 String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.v("query", selection);
		Cursor cursor = null;
		SQLiteDatabase sqlDB = simpleDynamoDB.getWritableDatabase();
		try {
			if (selection.equals(QUERY_ME)) {
				//Log.d(TAG, "Query Me TimeStamp: "+ new Timestamp(Calendar.getInstance().getTime().getTime()));
				Log.d(TAG, "Query me: " + myAvdPort);
				synchronized (dbLock) {
					cursor = sqlDB.query(TABLE_NAME, projection, null, null, null, null, null);
					//Log.d(TAG, "Query Me TimeStamp: "+ new Timestamp(Calendar.getInstance().getTime().getTime()));
				}
			} else if (selection.equals(QUERY_ALL)) {
				//Log.d(TAG, "Query All TimeStamp: "+ new Timestamp(Calendar.getInstance().getTime().getTime()));
				Log.d(TAG, "Query me for all: " + myAvdPort);
				Message nodeMessage = new Message(selection);
				nodeMessage.setFromPort(myPort);
				nodeMessage.setOriginalRequester(myAvdPort);
				nodeMessage.setMessageType(NODE_QUERY_ALL);
				nodeMessage.setToPort(getNetworkPort(nodeMessage.getOriginalRequester()));
				new ClientThread(nodeMessage).start();
				synchronized (globalQueryLock) {
					try {
						//Log.d(TAG, "I'm waiting for all avd's to respond......");
						globalQueryLock.wait(3000);
					} catch (InterruptedException ex) {
						//Log.d(TAG, "Response received: " + globalQueryLock.isResponseReceived);

					}
					//Log.d(TAG, "Query All TimeStamp: "+ new Timestamp(Calendar.getInstance().getTime().getTime()));
					Log.d(TAG, "Query all response received: " + globalQueryLock.isResponseReceived);
					cursor = createCursor(globalQueryLock.queryDump);
					globalQueryLock.isResponseReceived = false;
					globalQueryLock.queryDump = null;
				}
			} else {
				Log.d(TAG, "Query Key: " + selection + " TimeStamp: " + new Timestamp(Calendar.getInstance().getTime().getTime()));
				Node owner = getKeyOwner(selection);
				//Log.d(TAG, "Key: "+selection+" found in owner: "+owner.getAvdPort());
				//if key found in myavd return
				if (owner.getAvdPort().equals(myAvdPort)) {
					Log.d(TAG, "Query key: " + selection + " found in myself: " + owner.getAvdPort());
					synchronized (dbLock) {
						cursor = sqlDB.query(TABLE_NAME, projection, KEY_COLUM + "=?", new String[]{selection}, null, null, null);
					}
				} else {
					Map<String, String> queryResult = null;
					Message nodeMessage = new Message(selection);
					nodeMessage.setFromPort(myPort);
					nodeMessage.setOriginalRequester(myAvdPort);
					nodeMessage.setMessageType(NODE_QUERY_MSG);
					Log.d(TAG, "Routing query key: " + selection + " to owner: " + owner.getAvdPort());
					nodeMessage.setToPort(getNetworkPort(owner.getAvdPort()));
					new ClientThread(nodeMessage).start();
					synchronized (queryLock) {
						try {
							Log.d(TAG, "Query Key wait: " + selection + " TimeStamp: " + new Timestamp(Calendar.getInstance().getTime().getTime()));
							queryLock.wait(2000);
							Log.d(TAG, "Query Key wait Time Over: " + selection + " TimeStamp: " + new Timestamp(Calendar.getInstance().getTime().getTime()));
						} catch (InterruptedException ex) {
							//Log.d(TAG, "Response received: " + globalQueryLock.isResponseReceived);
						}
						Log.d(TAG, "query dump: " + queryLock.queryDump);
						if (queryLock.queryDump != null && queryLock.queryDump.size() > 0) {
							cursor = createCursor(queryLock.queryDump);
							queryLock.queryDump = null;
							queryLock.isResponseReceived = false;
						} else {
							Log.d(TAG, "Querying failed for: " + selection);
							nodeMessage.setToPort(getNetworkPort(owner.getSuccessor().getAvdPort()));
							Log.d(TAG, "Sending query fail request to: " + nodeMessage.getToPort() + " ,Message Type: " + nodeMessage.getMessageType());
							new ClientThread(nodeMessage).start();
							nodeMessage.setToPort(getNetworkPort(owner.getSuccessor().getSuccessor().getAvdPort()));
							queryLock.wait(3000);
							if (queryLock.queryDump != null && queryLock.queryDump.size() > 0) {
								cursor = createCursor(queryLock.queryDump);
								queryLock.queryDump = null;
								queryLock.isResponseReceived = false;
							}
						}
					}
				}
			}
		} catch (Exception ex) {
			Log.d(TAG, "Query failed for key: " + selection + " ,Exception: " + ex);
		}
		return cursor;
	}

	private Cursor createCursor(Map<String, String> queryDump){
		MatrixCursor cursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
		Iterator<Map.Entry<String, String>> itr = queryDump.entrySet().iterator();
		while(itr.hasNext()){
			Map.Entry<String,String> entry = itr.next();
			cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
		}
		return cursor;
	}

	private Map<String, String> queryAll(){
		Log.v(TAG, "Querying all: " + myAvdPort);
		Cursor cursor = null;
		Map<String, String> myResults = new HashMap<String, String>();
		SQLiteDatabase sqlDB = simpleDynamoDB.getWritableDatabase();
		synchronized (dbLock) {
			cursor = sqlDB.query(TABLE_NAME, null, null, null, null, null, null);
		}
		while(cursor.moveToNext()){
			int keyIndex = cursor.getColumnIndex(KEY_FIELD);
			int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
			String key = cursor.getString(keyIndex);
			String value = cursor.getString(valueIndex);
			myResults.put(key, value);
		}
		return myResults;
	}

	private Map<String, String> querySingle(String selection){
		Log.v(TAG, "Querying key: "+selection +" avd: " + myAvdPort);
		Cursor cursor = null;
		SQLiteDatabase sqlDB = simpleDynamoDB.getWritableDatabase();
		synchronized (dbLock) {
			cursor = sqlDB.query(TABLE_NAME, null, KEY_COLUM + "=?", new String[]{selection}, null, null, null);
		}
		Map<String, String> myQueryResult = new HashMap<String, String>();
		while(cursor.moveToNext()){
			int keyIndex = cursor.getColumnIndex(KEY_FIELD);
			int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
			String key = cursor.getString(keyIndex);
			String value = cursor.getString(valueIndex);
			myQueryResult.put(key, value);
		}
		return myQueryResult;
	}

	private int deleteSingle(String selection){
		Log.v(TAG, "Deleting key: "+selection +" avd: " + myAvdPort);
		SQLiteDatabase sqlDB = simpleDynamoDB.getWritableDatabase();
		return sqlDB.delete(TABLE_NAME, KEY_COLUM + "=?", new String[]{selection});
	}

	private int deleteAll(String selection){
		Log.v(TAG, "Deleting all avd: " + myAvdPort);
		SQLiteDatabase sqlDB = simpleDynamoDB.getWritableDatabase();
		return sqlDB.delete(TABLE_NAME, "1", null);
	}

	private Map<String,String> addMyRowsToDump(Map<String,String> source, Map<String,String> destination){
		Iterator<Map.Entry<String, String>> itr = source.entrySet().iterator();
		while(itr.hasNext()){
			Map.Entry<String,String> entry = itr.next();
			destination.put(entry.getKey(), entry.getValue());
		}
		return destination;
	}
	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
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

	private class ClientThread extends Thread {
		public Message messageToSend;

		public ClientThread(Message messageToSend) {
			this.messageToSend = messageToSend;
		}

		@Override
		public void run() {
			Socket remoteSocket = null;
			String remotePort = messageToSend.getToPort();
			boolean failure = false;
			try {
				Log.d(TAG, "Sending request to: "+getAvdPort(remotePort));
				remoteSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(remotePort));
				remoteSocket.setSoTimeout(1000);
				OutputStream os = remoteSocket.getOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(os);
				oos.writeObject(messageToSend);
				/*InputStream is = remoteSocket.getInputStream();
				ObjectInputStream ois = new ObjectInputStream(is);
				Message ackMsg = (Message) ois.readObject();
				if(ackMsg!=null&&ackMsg.getMessage().equals(ACKNOWLEDGEMENT)){
					Log.d(TAG, "Acknowledgement Received from: "+remotePort);
					oos.flush();
					oos.close();
				}
				else{
					Log.d(TAG, "Acknowledgement failed from: "+remotePort);
					handleNodeFailure(messageToSend);
				}*/

			} catch (SocketTimeoutException ex) {
				Log.e(TAG+"-ClientThread", "Socket Timeout in  connecting to remote port: " + remotePort);
				failure = true;
				handleNodeFailure(messageToSend);
			} catch (EOFException ex) {
				Log.e(TAG+"-ClientThread", "EOFException: " + remotePort+" Message Type: "+messageToSend.getMessageType()+" Exception: "+ex.getMessage());
				failure = true;
				handleNodeFailure(messageToSend);
			} catch (SocketException ex) {
				Log.e(TAG+"-ClientThread", "SocketException: " + remotePort+" Message Type: "+messageToSend.getMessageType()+" Exception: "+ex.getMessage());
				failure = true;
				handleNodeFailure(messageToSend);
			} catch (UnknownHostException ex) {
				Log.e(TAG+"-ClientThread", "UnknownHostException: " + remotePort+" Message Type: "+messageToSend.getMessageType()+" Exception: "+ex.getMessage());
				failure = true;
				handleNodeFailure(messageToSend);
			} catch (IOException ex) {
				Log.e(TAG+"-ClientThread", "IOException: " + remotePort+" Message Type: "+messageToSend.getMessageType()+" Exception: "+ex.getMessage());
				failure = true;
				handleNodeFailure(messageToSend);
			} catch (Exception ex) {
				Log.e(TAG+"-ClientThread", "Exception: " + remotePort+" Message Type: "+messageToSend.getMessageType()+" Exception: "+ex.getMessage());
				failure = true;
				handleNodeFailure(messageToSend);
			} finally {
				try {
					remoteSocket.close();
				} catch (Exception ex) {
					Log.e(TAG+"-ClientThread", "Socket Close Exception: "+ remotePort+" Message Type: "+messageToSend.getMessageType()+" Exception: "+ex.getMessage());
				}
			}
		}
	}

	private void handleNodeFailure(Message message) {
		//Log.d(TAG, "Failed Node: "+getAvdPort(message.getToPort())+" Message Type: "+message.getMessageType());
		String succPort = getSuccessorNode(message.getToPort()).getAvdPort();
		//Log.d(TAG, "Routing to successor: "+succPort);
		switch (message.messageType){
			case NODE_JOIN:
				Log.d(TAG, "Error in connecting to avd: "+message.getToPort());
			case NODE_INSERT_MSG:
				String key = message.getContentValues().get(KEY_FIELD);
				if(message.getChainLevel()==3 || message.getChainLevel()==2){
					Log.d(TAG, "Node Insert Failure: "+message.getToPort()+", Key: "+key+", successor avd: "+succPort);
					message.setToPort(getNetworkPort(succPort));
					message.setChainLevel(message.getChainLevel()-1);
					new ClientThread(message).start();
				}
				else if(message.getChainLevel()==1){
					Log.d(TAG, "Node Insert Failure Response: "+message.getToPort()+", Key: "+key+", to requester: "+message.getOriginalRequester());
					message.setFromPort(myPort);
					message.setToPort(getNetworkPort(message.getOriginalRequester()));
					message.setMessageType(NODE_INSERT_RESPONSE);
					new ClientThread(message).start();
				}
				break;
			case NODE_QUERY_MSG:
				String keyMsg = message.getMessage();
				Log.d(TAG, "Node query Failure: "+message.getToPort()+", Key: "+keyMsg+", to successor avd: "+succPort);
				message.setToPort(getNetworkPort(succPort));
				new ClientThread(message).start();
				break;
			case NODE_QUERY_ALL:
				if(succPort.equals(message.getOriginalRequester())) {
					message.setToPort(getNetworkPort(message.getOriginalRequester()));
					message.setMessageType(NODE_QUERY_ALL_RESPONSE);
				}
				else{
					message.setToPort(getNetworkPort(succPort));
				}
				new ClientThread(message).start();
				break;
			case NODE_DELETE_MSG:
				String deleteKey = message.getMessage();
				if(message.getChainLevel()==3 || message.getChainLevel()==2){
					Log.d(TAG, "Node Delete failure: "+message.getToPort()+" , Key: "+deleteKey+", to successor avd: "+succPort);
					message.setToPort(getNetworkPort(succPort));
					message.setChainLevel(message.getChainLevel()-1);
					new ClientThread(message).start();
				}
				else if(message.getChainLevel()==1){
					Log.d(TAG, "Node Delete failure response: "+message.getToPort()+" , Key: "+deleteKey+", to owner avd: "+message.getOriginalRequester());
					message.setFromPort(myPort);
					message.setToPort(getNetworkPort(message.getOriginalRequester()));
					message.setMessageType(NODE_DELETE_RESPONSE);
					new ClientThread(message).start();
				}
				break;
			case NODE_DELETE_ALL:
				if(succPort.equals(message.getOriginalRequester())) {
					message.setToPort(getNetworkPort(message.getOriginalRequester()));
					message.setMessageType(NODE_DELETE_ALL_RESPONSE);
				}
				else{
					message.setToPort(getNetworkPort(succPort));
				}
				new ClientThread(message).start();
				break;
		}
	}

	private void handleClientFailure(Message message) {
		if(message!=null) {
			switch (message.messageType) {
				case NODE_JOIN:
					Log.d(TAG, "Node failed: " + message.getFromPort());
				case NODE_INSERT_MSG:
					message.setToPort(myPort);
					new ClientThread(message).start();
					break;
				case NODE_QUERY_ALL:
					message.setToPort(myPort);
					new ClientThread(message).start();
					break;
				case NODE_DELETE_MSG:
					message.setToPort(myPort);
					new ClientThread(message).start();
					break;
				case NODE_DELETE_ALL:
					message.setToPort(myPort);
					new ClientThread(message).start();
					break;
				case NODE_QUERY_RESPONSE:
					synchronized (queryLock) {
						//Log.d(TAG, "Query Response: "+nodeMessage.getQueryDump().get(KEY_FIELD)+" , "+nodeMessage.getQueryDump().get(VALUE_FIELD));
						queryLock.isResponseReceived = true;
						queryLock.queryDump = message.getQueryDump();
						queryLock.notifyAll();
					}
					break;
				case NODE_INSERT_RESPONSE:
					synchronized (insertLock) {
						//Log.d(TAG, "Insert query is replicated....I'm notifying myself to stop waiting....");
						insertLock.isResponseReceived = true;
						insertLock.notify();
					}
					break;
				case NODE_DELETE_ALL_RESPONSE:
					synchronized (globalDeleteLock) {
						globalDeleteLock.isResponseReceived = true;
						globalDeleteLock.totalRowsDeleted = message.getRowsDeleted();
						globalDeleteLock.notify();
					}
					break;
				case NODE_QUERY_ALL_RESPONSE:
					synchronized (globalQueryLock) {
						//Log.d(TAG, "I'm notifying myself to stop waiting....");
						globalQueryLock.queryDump = message.getQueryDump();
						globalQueryLock.isResponseReceived = true;
						globalQueryLock.notify();
					}
					break;
				case NODE_DELETE_RESPONSE:
					//Log.d(TAG, "Delete key: "+nodeMessage.getContentValues().get(KEY_FIELD)+" Response received from: " + getAvdPort(nodeMessage.getFromPort()));
					synchronized (deleteLock) {
						deleteLock.isResponseReceived = true;
						deleteLock.totalRowsDeleted = message.getRowsDeleted();
						deleteLock.notify();
					}
					break;

			}
		}
	}


	private Node getSuccessorNode(String toPort) {
		String avdPort = getAvdPort(toPort);
		Node successor = null;
		try {
			successor = chordMap.get(genHash(avdPort)).getSuccessor();
		}
		catch (NoSuchAlgorithmException ex){
			Log.d(TAG, "Exception in generating hash for key: "+avdPort);
		}
		return successor;
	}

	private void deleteIrrelevantKeys(Map<String,String> queryDump){
		String predPort = myPredecessor.getAvdPort();
		String predPredPort = myPredecessor.getPredecessor().getAvdPort();
		for(String key : queryDump.keySet()){
			if(getKeyOwner(key).avdPort.equals(myAvdPort) || getKeyOwner(key).avdPort.equals(predPort) || getKeyOwner(key).avdPort.equals(predPredPort)){
				continue;
			}
			else{
				deleteSingle(key);
			}
		}
	}

	private void sendAcknowledgement(Message nodeMessage, Socket remoteSocket){
		try {
			OutputStream os = remoteSocket.getOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(os);
			oos.writeObject(nodeMessage);
			oos.flush();
			oos.close();
			remoteSocket.close();
		} catch (Exception ex) {
			Log.d(TAG, "Requested Client failed: "+ex.getMessage());
		}
	}

	private class ServerTask extends AsyncTask<Object[], String, Void> {

		@Override
		protected Void doInBackground(Object[]... objects) {
			Object[] params = objects[0];
			ServerSocket serverSocket = (ServerSocket) params[0];
			SimpleDynamoProvider dhtProvider = (SimpleDynamoProvider) params[1];
			Message originalMessage = null;
			Socket socket = null;
			Message nodeMessage = null;
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

			while (true) {
				try {
					socket = serverSocket.accept();
					InputStream in = socket.getInputStream();
					ObjectInputStream ois = new ObjectInputStream(in);
					nodeMessage = (Message) ois.readObject();
					originalMessage = nodeMessage;
					String messageType = nodeMessage.getMessageType();
					String fromPort = nodeMessage.getFromPort();
					String fromAvdPort = getAvdPort(fromPort);
					Log.d(TAG, "Message Type: " + messageType);
					Message ackMsg = new Message(ACKNOWLEDGEMENT);
					switch (messageType) {
						case NODE_JOIN:
							Log.d(TAG, "Node: " + fromAvdPort + " joined");
							Map<String, String> lostData = new HashMap<String, String>();
							if (myPredecessor.getAvdPort().equals(fromAvdPort) || myPredecessor.getPredecessor().getAvdPort().equals(fromAvdPort)) { //now i'm saviour...send keys to original owner
								Map<String, String> myData = queryAll();
								Log.d(TAG, "Sending Lost Messages to predecessor: " + fromAvdPort);
								for (String key : myData.keySet()) {
									if (getKeyOwner(key).getAvdPort().equals(fromAvdPort)) {
										lostData.put(key, myData.get(key));
										//Log.d(TAG, "Valid Key to sent: "+key+" Owner: "+getKeyOwner(key).getAvdPort());
									}
								}
								nodeMessage.setFromPort(myPort);
								nodeMessage.setToPort(getNetworkPort(fromAvdPort));
								nodeMessage.setMessageType(NODE_RECOVER);
								nodeMessage.setQueryDump(lostData);
								new ClientThread(nodeMessage).start();
							}
							// get all msgs from myself and pred and pass to recovered lad
							else if (mySuccessor.getAvdPort().equals(fromAvdPort) || mySuccessor.getSuccessor().getAvdPort().equals(fromAvdPort)) {
								Map<String, String> myData = queryAll();
								Log.d(TAG, "Sending Lost Messages to successor: " + fromAvdPort);
								for (String key : myData.keySet()) {
									if (getKeyOwner(key).getAvdPort().equals(myAvdPort)) {
										lostData.put(key, myData.get(key));
										//Log.d(TAG, "Valid Key to send: "+key+" Owner: "+getKeyOwner(key).getAvdPort());
									}
								}
								nodeMessage.setFromPort(myPort);
								nodeMessage.setToPort(getNetworkPort(fromAvdPort));
								nodeMessage.setMessageType(NODE_RECOVER);
								nodeMessage.setQueryDump(lostData);
								new ClientThread(nodeMessage).start();
							}
							Log.d(TAG, "Node recover response to avd: "+fromAvdPort);
							break;
						case NODE_RECOVER:
							Log.d(TAG, "Recover Response received from: " + getAvdPort(nodeMessage.getFromPort()));
							Map<String, String> queryDump = nodeMessage.getQueryDump();
							for (String key : queryDump.keySet()) {
								ContentValues contentValues = new ContentValues();
								contentValues.put(KEY_FIELD, key);
								contentValues.put(VALUE_FIELD, queryDump.get(key));
								insertIntoDb(CONTENT_URI, contentValues);
							}
							break;
						case NODE_INSERT_MSG:
							//Log.d(TAG, "Inside insert msg...");
							Map<String, String> rowToInsert = nodeMessage.getContentValues();
							ContentValues contentValues = new ContentValues();
							contentValues.put(KEY_FIELD, rowToInsert.get(KEY_FIELD));
							contentValues.put(VALUE_FIELD, rowToInsert.get(VALUE_FIELD));
							String key = (String) contentValues.get(KEY_FIELD);
							if (nodeMessage.getChainLevel() == 3 || nodeMessage.getChainLevel() == 2) {
								//inserting into my db
								insertIntoDb(CONTENT_URI, contentValues);
								nodeMessage.setFromPort(myPort);
								nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
								nodeMessage.setChainLevel(nodeMessage.getChainLevel() - 1);
								Log.d(TAG, "Node Insert replicate, Key: "+rowToInsert.get(KEY_FIELD)+", avd: "+mySuccessor.getAvdPort());
								new ClientThread(nodeMessage).start();
							} else if (nodeMessage.getChainLevel() == 1) {
								Log.d(TAG, "Node Insert response, Key: "+rowToInsert.get(KEY_FIELD)+", avd: "+nodeMessage.getOriginalRequester());
								insertIntoDb(CONTENT_URI, contentValues);
								nodeMessage.setFromPort(myPort);
								nodeMessage.setToPort(getNetworkPort(nodeMessage.getOriginalRequester()));
								nodeMessage.setMessageType(NODE_INSERT_RESPONSE);
								new ClientThread(nodeMessage).start();
							}
							break;
						case NODE_INSERT_REPLICA:
							Map<String, String> row = nodeMessage.getContentValues();
							ContentValues cv = new ContentValues();
							cv.put(KEY_FIELD, row.get(KEY_FIELD));
							cv.put(VALUE_FIELD, row.get(VALUE_FIELD));
							insertIntoDb(CONTENT_URI, cv);
						case NODE_INSERT_RESPONSE:
							Log.d(TAG, "Insert key: "+nodeMessage.getContentValues().get(KEY_FIELD)+" Response received from: " + getAvdPort(nodeMessage.getFromPort()));
							synchronized (dhtProvider.insertLock) {
								//Log.d(TAG, "Insert query is replicated....I'm notifying myself to stop waiting....");
								dhtProvider.insertLock.isResponseReceived = true;
								dhtProvider.insertLock.notify();
							}
							break;
						case NODE_QUERY_ALL:
							Map<String, String> myResults = queryAll();
							Map<String, String> updatedDump = addMyRowsToDump(myResults, nodeMessage.getQueryDump() == null ? new HashMap<String, String>() : nodeMessage.getQueryDump());
							nodeMessage.setFromPort(myPort);
							nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
							nodeMessage.setQueryDump(updatedDump);
							if (!mySuccessor.getAvdPort().equals(nodeMessage.originalRequester)) {
								new ClientThread(nodeMessage).start();
							} else {
								nodeMessage.setMessageType(NODE_QUERY_ALL_RESPONSE);
								new ClientThread(nodeMessage).start();
							}
							break;
						case NODE_QUERY_ALL_RESPONSE:
							synchronized (dhtProvider.globalQueryLock) {
								//Log.d(TAG, "I'm notifying myself to stop waiting....");
								dhtProvider.globalQueryLock.isResponseReceived = true;
								dhtProvider.globalQueryLock.queryDump = nodeMessage.getQueryDump();
								dhtProvider.globalQueryLock.notify();
							}
							break;
						case NODE_QUERY_MSG:
							Log.d(TAG, "Node Query response, Key: "+nodeMessage.getMessage()+", avd: "+nodeMessage.getOriginalRequester());
							String queryKey = nodeMessage.getMessage();
							nodeMessage.setFromPort(myPort);
							Map<String, String> queryResult = querySingle(queryKey);
							nodeMessage.setQueryDump(queryResult);
							nodeMessage.setMessageType(NODE_QUERY_RESPONSE);
							nodeMessage.setToPort(getNetworkPort(nodeMessage.getOriginalRequester()));
							new ClientThread(nodeMessage).start();
							break;
						case NODE_QUERY_RESPONSE:
							Log.d(TAG, "Query key: "+nodeMessage.getMessage()+" Response received from: " + getAvdPort(nodeMessage.getFromPort()));
							Log.d(TAG, "Query Key response: "+nodeMessage.getMessage()+" TimeStamp: "+ new Timestamp(Calendar.getInstance().getTime().getTime()));
							synchronized (dhtProvider.queryLock) {
								Log.d(TAG, "Query Response: "+nodeMessage.getQueryDump().get(KEY_FIELD)+" , "+nodeMessage.getQueryDump().get(VALUE_FIELD));
								dhtProvider.queryLock.isResponseReceived = true;
								dhtProvider.queryLock.queryDump = nodeMessage.getQueryDump();
								dhtProvider.queryLock.notifyAll();
							}
							break;
						case NODE_DELETE_MSG:
							String deleteKey = nodeMessage.getMessage();
							nodeMessage.setFromPort(myPort);
							nodeMessage.setRowsDeleted(deleteSingle(deleteKey));
							if (nodeMessage.getChainLevel() == 3 || nodeMessage.getChainLevel() == 2) {
								Log.d(TAG, "Node Delete replicate, Key: "+deleteKey+", avd: "+mySuccessor.getAvdPort());
								nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
								nodeMessage.setChainLevel(nodeMessage.getChainLevel() - 1);
							} else if (nodeMessage.getChainLevel() == 1) {
								Log.d(TAG, "Node Delete response, Key: "+deleteKey+", avd: "+nodeMessage.getOriginalRequester());
								nodeMessage.setMessageType(NODE_DELETE_RESPONSE);
								nodeMessage.setToPort(getNetworkPort(nodeMessage.getOriginalRequester()));
							}
							new ClientThread(nodeMessage).start();
							break;
						case NODE_DELETE_RESPONSE:
							//Log.d(TAG, "Delete key: "+nodeMessage.getContentValues().get(KEY_FIELD)+" Response received from: " + getAvdPort(nodeMessage.getFromPort()));
							synchronized (dhtProvider.deleteLock) {
								dhtProvider.deleteLock.isResponseReceived = true;
								dhtProvider.deleteLock.totalRowsDeleted = nodeMessage.getRowsDeleted();
								dhtProvider.deleteLock.notify();
							}
							break;
						case NODE_DELETE_ALL:
							int rowsDeletedTillNow = nodeMessage.getRowsDeleted() + deleteAll(nodeMessage.getMessage());
							nodeMessage.setRowsDeleted(rowsDeletedTillNow);
							nodeMessage.setFromPort(myPort);
							nodeMessage.setToPort(getNetworkPort(mySuccessor.getAvdPort()));
							if (!mySuccessor.getAvdPort().equals(nodeMessage.originalRequester)) {
								new ClientThread(nodeMessage).start();
							} else {
								nodeMessage.setMessageType(NODE_DELETE_ALL_RESPONSE);
								new ClientThread(nodeMessage).start();
							}
							break;
						case NODE_DELETE_ALL_RESPONSE:
							synchronized (dhtProvider.globalDeleteLock) {
								dhtProvider.globalDeleteLock.isResponseReceived = true;
								dhtProvider.globalDeleteLock.totalRowsDeleted = nodeMessage.getRowsDeleted();
								dhtProvider.globalDeleteLock.notify();
							}
							break;
					}
				} catch (ClassNotFoundException ex){
					Log.e(TAG, "Exception in Server Socket: " + socket.getPort() + " ClassNotFoundException: " + ex.getMessage()+" Message Type: "+nodeMessage.getMessageType());
				} catch(StreamCorruptedException ex){
					Log.e(TAG, "Exception in Server Socket: " + socket.getPort() + " SocketException: " + ex.getMessage()+" Message Type: "+nodeMessage.getMessageType());
					handleClientFailure(originalMessage);
					//releaseLocks();
				} catch(SocketException ex){
					Log.e(TAG, "Exception in Server Socket: " + socket.getPort() + " SocketException: " + ex.getMessage()+" Message Type: "+nodeMessage.getMessageType());
					handleClientFailure(originalMessage);
					//releaseLocks();
				} catch(EOFException ex){
					Log.e(TAG, "Exception in Server Socket: " + socket.getPort() + " EOFException: " + ex.getMessage()+" Message Type: "+nodeMessage.getMessageType());
					handleClientFailure(originalMessage);
					//releaseLocks();
				} catch (IOException ex) {
					Log.e(TAG, "Exception in Server Socket: " + socket.getPort() + " IOException: " + ex.getMessage()+" Message Type: "+nodeMessage.getMessageType());
					handleClientFailure(originalMessage);
					//releaseLocks();
				} catch(Exception ex){
					Log.e(TAG, "Exception in Server Socket: " + socket.getPort() + " Exception: " + ex.getMessage()+" Message Type: "+nodeMessage.getMessageType());
					//releaseLocks();
				}
			}
		}
	}


	private void releaseLocks(){
		synchronized (queryLock) {
			queryLock.notify();
		}
		synchronized (deleteLock) {
			deleteLock.notify();
		}
		synchronized (insertLock) {
			insertLock.notify();
		}
		synchronized (globalQueryLock) {
			globalQueryLock.notify();
		}
		synchronized (globalDeleteLock) {
			globalDeleteLock.notify();
		}
	}

	private String getMyPort() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		return myPort;
	}

	private static String getAvdPort(String port){
		return String.valueOf((Integer.parseInt(port)/2));
	}

	private static String getNetworkPort(String avdPort){
		return String.valueOf((Integer.parseInt(avdPort)*2));
	}
}
