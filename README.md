# Apriori
Implementação do algoritmo Apriori utilizando as bibliotecas de MapReduce do Hadoop

## Utilização do programa
### Argumentos:
<ul>
<li>-i : arquivo de entrada</li>
<li>-o : diretório de saída</li>
<li>-s : suporte mínimo</li>
<li>-c : confiança mínima</li>
<li>-m : nível máximo de agrupamento (grupos com até m itens)</li>
</ul>
<strong>Os argumentos -i e -o são obrigatórios.</strong>

### <em>Defaults</em>:
<ul>
<li>Suporte mínimo (<em>minSup</em>): <strong>0.15</strong></li>
<li>Confiança mínima: <strong>0.7</strong></li>
<li>Nível máximo de agrupamento: <strong>4</strong></li>
</ul>

## Funcionamento.:
### Estruturas. 
<ul>
<li>HashMap &lt;Integer, Double&gt; conjItens → Conjunto com todos os itens com suporte &gt;
<em>minSup</em>. A chave (tipo <em>Integer</em>) é o item e o valor (tipo <em>Double</em>) é o suporte.</li>
<li>Set&lt;Set&lt;Integer&gt;&gt; conjAtual → Conjunto de grupos de itens. É necessário para fazer
a geração de grupos de tamanho <em>k</em>.</li>
<li>HashMap&lt;Set&lt;Integer&gt;, Double&gt; conjTotal → Conjunto com todos os itens com
tamanho <em>k</em> &gt; 2 e suporte &gt; <em>minSup</em>. A chave é o conjunto de itens (Set&lt;Integer&gt;) e o
valor é o suporte.</li>
</ul>

### Tipos:
<ul><li>O tipo <em>Integer</em> se refere ao tipo de dados dos itens contidos no arquivo.</li>
<li>As funções utilizam tipos de dados genéricos, porém o padrão utilizado é o tipo <em>Integer</em>. </li>
<li>Para utilizar outro tipo de dados é basta mudar o tipo das estruturas descritas acima</li></ul>

#### Obs.: O diretório de saída deve estar vazio
