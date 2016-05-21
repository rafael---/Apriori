/*
 * Trabalho de Tópicos especiais em bancos de dados
 * Acadêmico: Rafael Hengen Ribeiro
 * Descrição: Implementação do algoritmo Apriori utilizando as bibliotecas de MapReduce do Hadoop
 * 
 * Classe: Apriori
 * Função: Interface com o usuário e organização dos Mappers e Reducers
 */

package apriori;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import agrupamento.GroupMapper;
import agrupamento.GroupReducer;
import initialFilter.FilterMapper;
import initialFilter.FilterReducer;

public class Apriori {
	public static double minSup = 0.15;
	public static double minConf = 0.7;
	public static int maxNivel = 4;
	
	public static int lineNumber;
	public static HashMap <Integer, Double> conjItens;
	public static Set<Set<Integer>> conjAtual;
	public static HashMap<Set<Integer>, Double> conjTotal;
	public static int nivelAtual;
	public static int countMinSup;


    public static void main(String[] args) throws Exception {
    	lineNumber = 0;
    	conjItens = new HashMap<>();
    	conjTotal = new HashMap<>();
    	nivelAtual = 2;
    	
    	String inputFile = null;
    	String outputDir = null;
    	
    	if(args.length < 2 || args.length%2==1)	{
    		System.out.println("Use: Apriori arquivo_entrada diretorio_saida");
    		return;
    	}
    	
    	for(int i = 0; i < args.length; i+=2)	{
    		try	{
    			switch(args[i])	{
    			case "-i": case "--input-file":
    				inputFile = args[i+1];
    				break;
    			case "-o": case "--output-path":
    				outputDir = args[i+1];
    				if(outputDir.lastIndexOf('/') == outputDir.length()-1)
    					outputDir = outputDir.substring(0,outputDir.length()-1);
    				break;
    			case "-s": case "--min-sup":
    				minSup = Double.valueOf(args[i+1]);
    				break;
    			case "-c": case "--min-conf":
    				minConf = Double.valueOf(args[i+1]);
    				break;
    			case "-m": case "--max-level":
    				maxNivel = Integer.valueOf(args[i+1]);
    				break;
    			default:
    				throw new Exception("Opção inválida \""+args[i]+"\"!");
    			}
    		}
    		catch(Exception e)	{
    			System.out.println("Erro: "+e.getMessage());
    			break;
    		}
    	}
    	
    	if(inputFile.isEmpty() || outputDir.isEmpty())	{
    		System.out.println("Argumentos inválidos");
    		return;
    	}

    	Apriori apriori = new Apriori();
        boolean isCompleted = false;
        
    	System.out.println("Job 1: Filtragem inicial");
        isCompleted = apriori.filtragemInicial(inputFile, outputDir+"/sup");
        if(!isCompleted)	{
        	System.exit(0);
        }
        
    	conjAtual = produtoCartesiano(conjItens.keySet(), conjItens.keySet());

        while(nivelAtual <= maxNivel)	{
        	countMinSup = 0;
        	System.out.println("Job 2: Geração de grupos de tamanho = "+nivelAtual);
	        isCompleted = apriori.agrupamento(inputFile, outputDir+"/group"+nivelAtual);
	        if(!isCompleted)	{
	        	System.exit(0);
	        }
	        if(countMinSup == 0)
	        	break;
	        nivelAtual++;
	        conjAtual = produtoCartesiano(conjAtual, conjItens.keySet(), nivelAtual);
        }
        
        writeLog(outputDir+"/grupos.log");
        
        calcularConfianca(outputDir+"/resultado.txt");
    }
    
    public static void calcularConfianca(String nomeArq) throws IOException	{
    	FileWriter file = new FileWriter(nomeArq);
    	BufferedWriter writer = new BufferedWriter(file);
    	
    	for(Set<Integer> conjuntos : conjTotal.keySet())	{	
        	for(int n : conjuntos)	{
        		Set<Integer> temp = new HashSet<>();
        		temp.addAll(conjuntos);
        		temp.remove(n);
        		double conf = conjTotal.get(conjuntos)/conjItens.get(n);
        		if(conf > minConf)	{
        			System.out.println("["+n+"] -> "+temp+": "+conf);
        			writer.write("["+n+"] -> "+temp+": "+conf+"\n");
        		}
        	}
        	
        	for(Set<Integer> subConjuntos : conjTotal.keySet())	{
        		if(conjuntos.size() > subConjuntos.size() && conjuntos.containsAll(subConjuntos))	{
        			Set<Integer> temp = new HashSet<>();
        			temp.addAll(conjuntos);
        			temp.removeAll(subConjuntos);
        			double conf = conjTotal.get(conjuntos) / conjTotal.get(subConjuntos);
        			if(conf > minConf)	{
        				System.out.println(subConjuntos+" -> "+temp+": "+conf);
            			writer.write(subConjuntos+" -> "+temp+": "+conf+"\n");
        			}
        		}
        	}
        }
    	
    	writer.close();
    	file.close();
    }
    
    public static boolean writeLog(String nomeArq)	{
    	BufferedWriter writer = null;
    	try	{
    		writer = new BufferedWriter(new FileWriter(nomeArq));
    		
    		for(Set<Integer> conjuntos : conjTotal.keySet())	{
    			StringBuilder str = new StringBuilder();
    			for(int n : conjuntos)	{
    				str.append(n);
    				str.append(',');
    			}
    			str.deleteCharAt(str.length()-1);
    			str.append('\t');
    			str.append(conjTotal.get(conjuntos));
    			str.append('\n');
    			writer.write(str.toString());
    		}
            
    		writer.close();
    	}
    	catch(IOException e)	{
    		System.out.println("Erro ao escrever no arquivo "+nomeArq);
    		return false;
    	}
    	
    	return true;
    }
   
    public static <T> Set<Set<T>> produtoCartesiano(Set<T> l1, Set<T> l2)	{
    	Set<Set<T>> ret = new HashSet<>();
    	for(T item1 : l1)	{
    		for(T item2 : l2)	{
    			Set<T> mSet = new HashSet<>();
    			mSet.add(item1);
    			mSet.add(item2);
    			ret.add(mSet);
    		}
    	}
    	return ret;
    }
    
    public static <T> Set<Set<T>> produtoCartesiano(Set<Set<T>> l1, Set<T> l2, int tamGrupo)	{
    	Set<Set<T>> ret = new HashSet<>();
    	for(Set<T> itemSet1 : l1)	{
    		for(T item2 : l2)	{
    			Set<T> mSet = new HashSet<>();
    			mSet.addAll(itemSet1);
    			mSet.add(item2);
    			if(mSet.size() == tamGrupo)
    				ret.add(mSet);
    		}
    	}
    	return ret;
    }
    
	public boolean filtragemInicial(String in, String out) throws IOException, 
	    ClassNotFoundException, 
	    InterruptedException {
		
		Job job = new Job(new Configuration(), "Filtragem inicial");
		job.setJarByClass(Apriori.class);
		
		// input / mapper
		FileInputFormat.addInputPath(job, new Path(in));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(FilterMapper.class);
		
		// output / reducer
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(FilterReducer.class);
		
		return job.waitForCompletion(true);
	}
	
	public boolean agrupamento(String in, String out) throws IOException, 
	    ClassNotFoundException, 
	    InterruptedException {
		
		Job job = new Job(new Configuration(), "Agrupamento");
		job.setJarByClass(Apriori.class);
		
		// input / mapper
		FileInputFormat.addInputPath(job, new Path(in));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(GroupMapper.class);
		
		// output / reducer
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(GroupReducer.class);
		
		return job.waitForCompletion(true);
	}
}