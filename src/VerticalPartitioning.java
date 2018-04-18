package de.uni_koblenz.west.koral.master.graph_cover_creator.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.web.HttpSC.Code;

import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.measurement.MeasurementCollector;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;

/**
 * Declares all supported graph cover strategies.
 * 
 * @author Adnan Akhter
 *
 */

public class VerticalPartitioning extends GraphCoverCreatorBase{
	String dataSet = "/home/MuhammadSaleem/Adnan/DataSet/SWDF/cleanRDF.nt";
	File inputFile = new File(dataSet);
	String outputFolder = "/home/MuhammadSaleem/Adnan/final-results/koralResults/";
    static Map<Integer, Integer> fileSizes = new HashMap<Integer, Integer>();


	private final MessageDigest digest;
	
	public VerticalPartitioning(Logger logger, MeasurementCollector measurementCollector) {
		super(logger, measurementCollector);
		   try {
			     digest = MessageDigest.getInstance("MD5");
			    } catch (NoSuchAlgorithmException e) {
			      if (logger != null) {
			        logger.throwing(e.getStackTrace()[0].getClassName(), e.getStackTrace()[0].getMethodName(),
			                e);
			     }
			   throw new RuntimeException(e);
		 }
		// TODO Auto-generated constructor stub
	}


	public static void main (String[] args) throws IOException {
		System.out.println("Initializing ... ");
		long start = System.currentTimeMillis();
//    	String dataSet="D:/AKSW/Dataset/lsq.1_0.2017-08-14.uniprot-bio2rdf.qel.sorted.nt";
//		String dataSet="D:/AKSW/Dataset/DBpedia10.nt";
//		String dataSet = "D:/AKSW/VirtuosoDataSet/swdf/swdf.nt";
//		String type = "predicate";
		String dataSet = args[0];
		String type = args[1];
		String outputFolder = args[2];
//		String outputFolder = "c:/OutPut/newOutput/";
		
		try {
			doVerticalPartitioning(dataSet, type, 10, outputFolder);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		System.out.println(outputFolder.hashCode());
//		String outputFolder1 = "http://OutPut/ #newOutput/~";
//		System.out.println(outputFolder1.hashCode());

		
		long totalTime = System.currentTimeMillis() - start;
		BufferedWriter logFile = new BufferedWriter(new FileWriter(outputFolder+"LogFile"));
		logFile.write("Total time taken in ms: "+totalTime);
		logFile.flush();
		logFile.close();

		System.out.println("Terminated Successfully in "+totalTime +" milliseconds");
		
	}


	public static void doVerticalPartitioning(String dataSet, String type, int noOfPartitions, String outputFolder) throws IOException {
		if(type.equals("predicate")) {
			performPredicatePartitioning(dataSet, noOfPartitions, outputFolder);
		}
		
		else if(type.equals("subject")) {
			performSubjectPartitioning(dataSet, noOfPartitions, outputFolder);
		}
		
		else {
			System.err.println("Invalid type!!!");
			System.exit(0);
		}

		
	}


	public static void performSubjectPartitioning(String dataSet, int noOfPartitions, String outputFolder) throws IOException {
		
		Set<String> predicates = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader(dataSet));
		BufferedWriter bw;
	    String line;
	    
	    Map<Integer, Integer> fileSizes = new HashMap<Integer, Integer>();
	    String folder = "";
	    int index = 0;
	    while ((line = br.readLine()) != null) {
	    	index++;
	    	if(index % 100000 == 0) {
	    		System.out.println(index);
//	    		System.out.println(predicates.size()+": " + predicates);
//	    		index = 0;
	    	}
	    	
//	    	String line = "%a triple is here%";
	    	//Create an empty model
	    try {
	    	final Model model = ModelFactory.createDefaultModel();
	    	//Parse and store the RDF triple in the model
	    	RDFDataMgr.read(model, new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8)), Lang.NTRIPLES);
	    	//Get all the statements - only 1 if any
	    
	    	final StmtIterator listStatements = model.listStatements();
	    	//Got my statement
	    	
//		    	System.out.println(line);

	    		final Statement statement = listStatements.next();
	    		int code = statement.getSubject().hashCode();
//	    		predicates.add(statement.getSubject().toString());
		    	bw = new BufferedWriter(new FileWriter(outputFolder+"file"+code, true));
//		    	System.out.println(code);
		    	bw.write(line);
		    	bw.newLine();
		    	bw.flush();
		    	
		    	if(! fileSizes.keySet().contains(code)) {
		    		fileSizes.put(code, 1);
		    	}
		    	
		    	else {
		    		
		    		int size = fileSizes.get(code)+1;
		    		fileSizes.put(code, size);
		    	}
	    }catch(Exception e) {
	    	System.err.println(e.getMessage() );
	    }
	    	
	    		
	    	
	    	
	    	
	    	
	    	
//	    	bw.close();
	    }
	    
	    fileSizes = sortByValue(fileSizes);
//	    System.out.println("Sizes are: "+fileSizes);
	    
	    
	    writePartitions(fileSizes,noOfPartitions, outputFolder);
	
		
	}


	public static void performPredicatePartitioning(String dataSet, int noOfPartitions, String outputFolder) throws IOException {
			
			Set<String> predicates = new HashSet<String>();
			BufferedReader br = new BufferedReader(new FileReader(dataSet));
			BufferedWriter bw;
		    String line;
		    
		    String folder = "";
		    int index = 0;
		    while ((line = br.readLine()) != null) {
		    	index++;
		    	if(index % 100000 == 0) {
		    		System.out.println(index);
//		    		System.out.println(predicates.size()+": " + predicates);
//		    		index = 0;
		    	}
		    	
//		    	String line = "%a triple is here%";
		    	//Create an empty model
		    try {
		    	final Model model = ModelFactory.createDefaultModel();
		    	//Parse and store the RDF triple in the model
		    	RDFDataMgr.read(model, new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8)), Lang.NTRIPLES);
		    	//Get all the statements - only 1 if any
		    
		    	final StmtIterator listStatements = model.listStatements();
		    	//Got my statement
		    	
//			    	System.out.println(line);

		    		final Statement statement = listStatements.next();
		    		int code = statement.getPredicate().hashCode();
//		    		predicates.add(statement.getPredicate().toString());
			    	bw = new BufferedWriter(new FileWriter(outputFolder+"file"+code, true));
//			    	System.out.println(code);
			    	bw.write(line);
			    	bw.newLine();
			    	bw.flush();
			    	
			    	if(! fileSizes.keySet().contains(code)) {
			    		fileSizes.put(code, 1);
			    	}
			    	
			    	else {
			    		
			    		int size = fileSizes.get(code)+1;
			    		fileSizes.put(code, size);
			    	}
		    }catch(Exception e) {
		    	System.err.println(e.getMessage() );
		    }
		    }
		    
		    fileSizes = sortByValue(fileSizes);		   
		    
//		    This method is to create partitions only
//		    writePartitions(fileSizes,noOfPartitions, outputFolder);
		    
		}


	
	public void writetoChunk(Map<Integer, Integer> fileSizes, int noOfPartitions, String outputFolder, EncodedFileOutputStream[] outputs, boolean[] writtenFiles) throws IOException {
//		new File("c:/OutPut").mkdir();
		
//			for ( int folder = 0 ; folder < noOfPartitions ; folder ++ ){
//				new File(outputFolder+"Partition" +folder).mkdir(); 
//			}
			int count = 0 ;
			int partitionNo = 0;
			for (int key: fileSizes.keySet()) {
				try {
					partitionNo = count % noOfPartitions ;
					String fileIndex = "file"+key ;
					count++;
					EncodedFileInputStream input = new EncodedFileInputStream(getRequiredInputEncoding(), new File(outputFolder +fileIndex));
					for(de.uni_koblenz.west.koral.common.io.Statement statement : input) {
						System.out.println(statement);
				    	writeStatementToChunk(partitionNo, noOfPartitions, statement, outputs, writtenFiles);
					}
				}catch(Exception e) {System.out.println(e.getMessage());}
				
				System.out.println("Partition number is: " +partitionNo);
				
			}
			
//					count = 0 ;
//					for (int key: fileSizes.keySet()) {
//						int partitionNo = count % noOfPartitions ;
//						String fileIndex = "file"+key ; 
//	//					write fileIndex into PartitionNo
//						count++;
//						BufferedReader br = new BufferedReader(new FileReader(outputFolder +fileIndex));
////						BufferedWriter bw = new BufferedWriter(new FileWriter(outputFolder+"Partition"+partitionNo +"/" +fileIndex));
//						int targetChunk = partitionNo;
//						String line = "";
//						while ((line = br.readLine()) != null) {
//							
////							EncodedFileInputStream input = line;
////							System.out.println(line);
//							try {
//								System.out.println("inside try block");
//						    	final Model model = ModelFactory.createDefaultModel();
//						    	RDFDataMgr.read(model, new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8)), Lang.NTRIPLES);
//						    	final StmtIterator listStatements = model.listStatements();
//						    	final de.uni_koblenz.west.koral.common.io.Statement statement = (de.uni_koblenz.west.koral.common.io.Statement) listStatements.next();
////						    	final de.uni_koblenz.west.koral.common.io.Statement statement = listStatements.next();
//							    System.out.println(statement);
////							    statement = line;
//						    	writeStatementToChunk(targetChunk, noOfPartitions, statement, outputs, writtenFiles);
//							}catch(Exception e) {
//								System.out.println(e.getMessage());
//							}	
//						}						
//				}
//			}
		}
	
	
	
	
	
	
	public static void writePartitions(Map<Integer, Integer> fileSizes, int noOfPartitions, String outputFolder) throws IOException {
//		new File("c:/OutPut").mkdir();
		
			for ( int folder = 0 ; folder < noOfPartitions ; folder ++ ){
				new File(outputFolder+"Partition" +folder).mkdir(); 
			}
//				
//				for(int key = 0 ; key <fileSizes.keySet().size(); key=key+noOfPartitions){
//					Object[] myKey = fileSizes.keySet().toArray();
//					int fileIndex = fileSizes.get(myKey[key]) ;
//					System.out.println("mykey   "+myKey.length);
//					
				
					int count = 0 ;
					for (int key: fileSizes.keySet()) {
						int partitionNo = count % noOfPartitions ;
						String fileIndex = "file"+key ; 
	//					write fileIndex into PartitionNo
						count++;
	//					}
	//					System.out.println("sdfsdfs "+fileIndex);
		//				now write fileIndex from list of files to folder 
						BufferedReader br = new BufferedReader(new FileReader(outputFolder +fileIndex));
						BufferedWriter bw = new BufferedWriter(new FileWriter(outputFolder+"Partition"+partitionNo +"/" +fileIndex));
						String line = "";
						while ((line = br.readLine()) != null) {
							 bw.write(line);
							 bw.newLine();
							 bw.flush();
						}
	//					File f = new File("C:/OutPut/" +fileIndex);
	////					f.delete();
	//					System.out.println("fffffffffffff " +f.delete());
						
				}
//			}
		}
	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort( list, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o1.getValue()).compareTo( o2.getValue() );
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }


	@Override
	public EncodingFileFormat getRequiredInputEncoding() {
	    return EncodingFileFormat.UEE;
	}
	
	
//	@Override
//	protected void createCover(DictionaryEncoder dictionary, EncodedFileInputStream input, int numberOfGraphChunks,
//			EncodedFileOutputStream[] outputs, boolean[] writtenFiles, File workingDir) {
//		System.out.println("Predicate-based cover creator is called");
//		 try {
//			performPredicatePartitioning(dataSet, numberOfGraphChunks, outputFolder);
//			writetoChunk(fileSizes,numberOfGraphChunks, outputFolder, outputs, writtenFiles);
//		} catch (IOException e) {e.printStackTrace();}
//		 
//	}


	@Override
	protected void createCover(DictionaryEncoder dictionary, EncodedFileInputStream input, int numberOfGraphChunks,
			EncodedFileOutputStream[] outputs, boolean[] writtenFiles, File workingDir) {
		System.out.println("Vertical cover creator called");
    	
		int targetChunk = 0;	
		List<Long> codes = new ArrayList<Long>();	
		
		for (de.uni_koblenz.west.koral.common.io.Statement statement : input) {
			long code = statement.getPropertyAsLong();
			System.out.println(code + ": <<<<<<<<Property>>>>>>>>>> " +dictionary.decode(code));
			if(codes.contains(code)) {
				targetChunk = getTargetChunk(codes.indexOf(code), numberOfGraphChunks);				
				writeStatementToChunk(targetChunk, numberOfGraphChunks, statement, outputs, writtenFiles);
			}
			else {
				codes.add(code);
				targetChunk = getTargetChunk(codes.indexOf(code), numberOfGraphChunks);	
				writeStatementToChunk(targetChunk, numberOfGraphChunks, statement, outputs, writtenFiles);
				
			}
		}
		System.out.println("Size of setCode is: " +codes.size());
	}

	public int getTargetChunk(int index, int numberOfGraphChunks) {
		index = index % numberOfGraphChunks;
		if (index < 0)
			index *= -1;
		System.out.println("Target chunk is: " +index);
		return index;
	}

	
}

