/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import edu.uoc.dpcs.lsim.LSimFactory;
import lsim.worker.LSimWorker;
import recipes_service.data.Operation;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class Log implements Serializable{
	// Needed for the logging system sgeag@2017
	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations
	 * received  by a client.
	 * They are stored in a ConcurrentHashMap (a hash table),
	 * that stores a list of operations for each member of 
	 * the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log= new ConcurrentHashMap<String, List<Operation>>();  

	public Log(List<String> participants){
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * inserts an operation into the log. Operations are 
	 * inserted in order. If the last operation for 
	 * the user is not the previous operation than the one 
	 * being inserted, the insertion will fail.
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
	public synchronized boolean add(Operation op){
		
	// return generated automatically. Remove it when implementing your solution 
	
		String idHost = op.getTimestamp().getHostid();
		Timestamp ultimTS;
		List<Operation> ops = this.log.get(idHost);
		
		if (ops != null && ops.size() > 0){
			ultimTS = ops.get(ops.size()-1).getTimestamp();
		}
		else {
			ultimTS = null;
		}
		
		long comp = op.getTimestamp().compare(ultimTS);
		
		if ( (ultimTS == null && comp == 0) 
				|| (ultimTS != null && comp == 1) ){
			this.log.get(idHost).add(op);
			return true;
		}
		else return false;
		
	}
		
		/*
		List<Operation> PrincipalLog = log.get(op.getTimestamp().getHostid());
		if (PrincipalLog.size()>0){
			Operation lastOp = PrincipalLog.get(PrincipalLog.size()-1);
			if (lastOp.getTimestamp().compare(op.getTimestamp())>0){
				return false;
			}
		}
		PrincipalLog.add(op);
		log.put(op.getTimestamp().getHostid(), PrincipalLog);
		return true;
	}
	*/
	/*
	private Timestamp get(String host){
		
		List<Operation> operations = this.log.get(host);
		
		if (operations != null && operations.size() > 0){
			return operations.get(operations.size()-1).getTimestamp();
		}
		else return null;
					
	}
	*/	
	
	/**
	 * Checks the received summary (sum) and determines the operations
	 * contained in the log that have not been seen by
	 * the proprietary of the summary.
	 * Returns them in an ordered list.
	 * @param sum
	 * @return list of operations
	 */
	public synchronized List<Operation> listNewer(TimestampVector sum){
		
		List<Operation> llista = new Vector();
		
		for (String node : this.log.keySet()) {
            List<Operation> operations = this.log.get(node);
            Timestamp timestampToCompare = sum.getLast(node);

            for (Operation op : operations) {
                if (op.getTimestamp().compare(timestampToCompare) > 0) {
                    llista.add(op);
                }
            }
        }
        return llista;
    }
	
	
	/**
	 * Removes from the log the operations that have
	 * been acknowledged by all the members
	 * of the group, according to the provided
	 * ackSummary. 
	 * @param ack: ackSummary.
	 */
	public void purgeLog(TimestampMatrix ack){
	}

	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
				
		if ( obj == null  || !(obj instanceof Log) ){
            return false;
        } 
		else 
        	if (this == obj) {
        		return true;
        	}

        if (this.log == ((Log)obj).log) {
            return true;
        } 
        else 
        	if (this.log == null || ((Log)obj).log == null) {
        		return false;
        } 
        else {
            return this.log.equals(((Log)obj).log);
        }
    }

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name="";
		for(Enumeration<List<Operation>> en=log.elements();
		en.hasMoreElements(); ){
		List<Operation> sublog=en.nextElement();
		for(ListIterator<Operation> en2=sublog.listIterator(); en2.hasNext();){
			name+=en2.next().toString()+"\n";
		}
	}
		
		return name;
	}
}