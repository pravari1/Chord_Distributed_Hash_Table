
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.HashMap;

// Generated code

import java.util.List;
import java.util.Map;

public class ServerHandler implements FileStore.Iface {
	
	private List<NodeID> fingerTable;
	private final String SERVER_KEY;
	private String IP;
	private final int PORT;
	private NodeID curNode;
	private final String LAST_KEY = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
//	private final String LAST_KEY = "f";
	Map<String,RFile> fileHandlerMap = null;

	public ServerHandler(int portArg) {
		// log = new HashMap<Integer, SharedStruct>();
		PORT = portArg;
		
		IP = "localhost";	
		try {
			IP = InetAddress.getLocalHost().getHostAddress();
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
		SERVER_KEY = SHA_256(IP + ":" + portArg);
		curNode = new NodeID(SERVER_KEY, IP, PORT);
		fileHandlerMap = new HashMap<String,RFile>();
	}

//	public void ping() {
//		System.out.println("ping()");
//	}
	
	private boolean isSameServer(NodeID node){
		return node.getIp().equalsIgnoreCase(this.IP) && node.getPort() == this.PORT;
	}
	
	@Override
	public void writeFile(RFile rFile) throws SystemException, TException {
		// TODO Auto-generated method stub
//		String owner = rFile.meta.getOwner();
		// System.out.println("Inside writeFile--------------"+rFile.getMeta().getOwner()+":"+rFile.getMeta().getFilename());
		
		String fileKey = rFile.getMeta().getOwner()+":"+rFile.getMeta().getFilename();
		// System.out.println("FileKey="+fileKey);
		NodeID node = findSucc(SHA_256(fileKey));
		// System.out.println("The Node="+node);
		if(!isSameServer(node)){
			SystemException sys =  new SystemException();
			sys.setMessage("Server does not own this file");
			throw sys;
		}
		try {
			if(fileHandlerMap.containsKey(fileKey)){
				rFile.getMeta().setVersion(fileHandlerMap.get(fileKey).getMeta().getVersion()+1);
			}
			fileHandlerMap.put(fileKey, rFile);
		} catch (Exception e) {
			// TODO: handle exception
//			e.printStackTrace();
		}
	}

	@Override
		public RFile readFile(String filename, String owner) throws SystemException  {
			// TODO Auto-generated method stub
			try{
				String fileKey = owner+":"+filename;
				NodeID node = findSucc(SHA_256(fileKey));
				SystemException sys =  new SystemException();
				if(!isSameServer(node)){
					sys.setMessage("Server does not own this file");
					throw sys;
				}
				if(fileHandlerMap.get(fileKey) == null){
					sys.setMessage("File Not Found!!");
					throw sys;
				}else{
					return fileHandlerMap.get(fileKey);
				}
			}catch(SystemException sys){
				throw sys;
			}catch(Exception e){
				System.out.println(e.getMessage());
	//			e.printStackTrace();
			}
			return null;
		}

	@Override
	public void setFingertable(List<NodeID> node_list) throws TException {
		// TODO Auto-generated method stub
//		System.out.print("length=" + node_list.size());
		// for (NodeID node : node_list) {
		// 	System.out.print(node);
		// }
		
		fingerTable = node_list;
		// System.out.print("Hellooooo"+);
	}

	@Override
	public NodeID findSucc(String key) throws SystemException, TException {
		// TODO Auto-generated method stub
		// if key is at current node directly return the node
		// System.out.println("Inside findSucc with key= "+key);
		// System.out.println("Which Server= "+curNode);
		if(fingerTable == null || fingerTable.size() == 0){
			SystemException sys =  new SystemException();
			sys.setMessage("Finger table is not set for node="+curNode);
			throw sys;
		}
		if(curNode.ip.compareTo(key) == 0){
			return curNode;
		}
		
		NodeID predesorNode = findPred(key);
		// System.out.println("predesorNode="+predesorNode);
		if(predesorNode == null){
			SystemException ex = new SystemException();
			ex.setMessage("No predecor found for key="+key);
			throw ex;
		}
		// Got the predesor node so, just get the succesor form their
		return doRPCForSuccesorAndPredesor(predesorNode, null, true);
	}
	private boolean checkZeroCrossingCond(NodeID predesorNode, String key){
		// System.out.println("Inside checkZeroCrossingCond="+predesorNode+", key="+key);
		NodeID sucessorNode = doRPCForSuccesorAndPredesor(predesorNode, null, true);
		// System.out.println("sucessorNode="+sucessorNode);
		if(sucessorNode.id.compareTo(predesorNode.id) < 0){
			//special case
			// System.out.println("Inside SpecialCase");
			if(key.compareTo(predesorNode.id) >=0 && key.compareTo(LAST_KEY)<=0)
				return true;
			if(key.compareTo("0") >=0 && key.compareTo(sucessorNode.id) <=0)
				return true;
		}else{
			// System.out.println("Inside normal case");
			int isGreaterThanCurrent =  key.compareTo(predesorNode.id);
			int isLesserThanSuccesor = key.compareTo(sucessorNode.id);
			if(isGreaterThanCurrent >=0 && isLesserThanSuccesor <=0)	return true;
		}
		// System.out.println("return checkZeroCrossingCond=false");
		return false;
	}
	
	public NodeID doRPCForSuccesorAndPredesor(NodeID node,String key ,boolean isGetSuccesorCall){
		// System.out.println("doRPCForSuccesorAndPredesor=="+node.ip+",port="+node.port+", key="+key+", isGetSuccesorCall="+isGetSuccesorCall);
		try {
			if(isSameServer(node)){
				// System.out.println("Inside LOCAL doRPCForSuccesorAndPredesor");
				return (isGetSuccesorCall) ? getNodeSucc() : findPred(key);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return null;
		}
		
		TTransport transport;
		TProtocol protocol;
		NodeID retNode = null;
		try {
			transport  = new TSocket(node.ip,node.port);
			transport.open();
			protocol = new TBinaryProtocol(transport);
			FileStore.Client client = new FileStore.Client(protocol);
			retNode = (isGetSuccesorCall) ? client.getNodeSucc() : client.findPred(key);
			transport.close();
		} catch (SystemException sy) {
			// TODO: handle exception
//			sy.printStackTrace();
		}catch (Exception e) {
			// TODO: handle exception
//			e.printStackTrace();
		}
		return retNode;
	}
	
	@Override
	public NodeID findPred(String key) throws SystemException, TException {
		// TODO Auto-generated method stub
		if(fingerTable == null || fingerTable.size() == 0){
			SystemException sys =  new SystemException();
			// sys.setMessage("Finger table is not set for node="+curNode);
			throw sys;
		}
		if(key == null) return null;
		NodeID predesorNode = curNode;
		while(!checkZeroCrossingCond(predesorNode, key)){
			predesorNode = closetPrecedingFinger(key);
			predesorNode = doRPCForSuccesorAndPredesor(predesorNode, key, false);
			// System.out.println("Found predesorNode="+predesorNode);
			break;
		}
		return predesorNode;
	}
	
	private NodeID closetPrecedingFinger(String key){
		// System.out.println("findPred="+key);
		if(key.compareTo(curNode.id) < 0){
			//special case
			for(int i=255;i>=0;i--){
				if(fingerTable.get(i).id.compareTo(curNode.id) > 0 && fingerTable.get(i).id.compareTo(LAST_KEY) <=0){
					return fingerTable.get(i);
				}
				if(fingerTable.get(i).id.compareTo("0") >= 0 && fingerTable.get(i).id.compareTo(key) <0){
					return fingerTable.get(i);
				}
			}
		}else{
			//normal case
			for(int i=255;i>=0;i--){
				if(fingerTable.get(i).id.compareTo(curNode.id) > 0 && fingerTable.get(i).id.compareTo(key) < 0){
					return fingerTable.get(i);
				}
			}
		}
		// System.out.println("return=nullllll");
		return null;
	}
	
	@Override
	public NodeID getNodeSucc() throws SystemException, TException {
		// TODO Auto-generated method stub
		if(fingerTable == null){
			SystemException sys =  new SystemException();
			// sys.setMessage("Finger table is not set for node="+curNode);
			throw sys;
		}
		// System.out.println("Inside getNodeSucc="+curNode+ ", Node="+fingerTable.get(0));
		return fingerTable.get(0);
	}
	
	public static int compareToKeys(String key1, String key2){
		return key1.compareTo(key2);
	}
	public static String SHA_256(String target) {
		String res = "";
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hash = digest.digest(target.getBytes("UTF-8"));
			StringBuffer hexString = new StringBuffer();

			for (int i = 0; i < hash.length; i++) {
				String hex = Integer.toHexString(0xff & hash[i]);
				if (hex.length() == 1)
					hexString.append('0');
				hexString.append(hex);
			}
			res =  hexString.toString();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return res;
	}

}
