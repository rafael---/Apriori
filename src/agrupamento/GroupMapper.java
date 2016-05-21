/*
 * Trabalho de Tópicos especiais em bancos de dados
 * Acadêmico: Rafael Hengen Ribeiro
 * Descrição: Implementação do algoritmo Apriori utilizando as bibliotecas de MapReduce do Hadoop
 * 
 * Classe: GroupMapper
 * Função: Mapeamento dos conjuntos de itens no arquivo de entrada
 */

package agrupamento;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import apriori.Apriori;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

public class GroupMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String line = value.toString();
        String [] tokens = line.split("[ \t]");
        
        List<Integer> valores = new ArrayList<>();
        for(String s : tokens)
        	valores.add(Integer.valueOf(s));
        
		for(Set<Integer> conj : Apriori.conjAtual)	{
        	StringBuilder strConj = new StringBuilder();
        	int k = 0;
        	for(int n : conj)	{
        		for(int valor : valores)	
        			if(n == valor)
        				k++;
        		
        		strConj.append(n);
        		strConj.append(',');
        	}
        	if(k == Apriori.nivelAtual)
        		context.write(new Text(strConj.substring(0, strConj.length()-1).toString()), new Text("1"));
        }
    }
    
}
