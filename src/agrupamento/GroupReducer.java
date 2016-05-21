/*
 * Trabalho de Tópicos especiais em bancos de dados
 * Acadêmico: Rafael Hengen Ribeiro
 * Descrição: Implementação do algoritmo Apriori utilizando as bibliotecas de MapReduce do Hadoop
 * 
 * Classe: GroupReducer
 * Função: Reducer. Verifica a frequência dos conjuntos de itens no arquivo de entrada e 
 * 	seleciona os grupos com suporte maiores ou iguais a minSup
 * Saída: Grupos com os elementos separados por vírgula e o suporte (frequência do grupo dividido
 * 	pelo número total de linhas)
 */

package agrupamento;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import apriori.Apriori;

 public class GroupReducer extends Reducer<Text, Text, Text, Text> {
	 
	 	static boolean first = true;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            
            for (Text value : values) 
                sum += Integer.valueOf(value.toString());
            

            if(first)	
            	Apriori.conjAtual.clear();
            
            double suporte = (double)sum/Apriori.lineNumber;
            
            if(suporte >= Apriori.minSup)	{
            	String [] lineTokens = key.toString().split(",");
            	
            	List<Integer> conjuntos = new ArrayList<>();
            	
            	for(String s : lineTokens)
            		conjuntos.add(Integer.valueOf(s));
            	
            	Set<Integer> conj = new HashSet<>();
            	for(Integer n : conjuntos)
            		conj.add(n);
            	
            	Apriori.conjTotal.put(conj, suporte);
            	Apriori.conjAtual.add(conj);
            	
            	Apriori.countMinSup++;
            	context.write(key, new Text(String.valueOf(suporte)));
            	
            	first = false;
            }
        }
    }