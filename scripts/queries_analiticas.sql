-- Desafio 1: O Crítico "Ranzinza" vs. O "Bonzinho"
/*Encontrando o userId que deu a menor média de notas e o que deu a 
maior média de notas.*/
(SELECT u.user_id AS "user",
  	    AVG(a.rating) AS "media"
   FROM fato_avaliacoes a
   JOIN dim_usuario u ON a.fk_usuario = u.user_key
  GROUP BY "user"
  ORDER BY "media" ASC
  LIMIT 1)

 UNION ALL

(SELECT u.user_id AS "user",
	    AVG(a.rating) AS "media"
   FROM fato_avaliacoes a
   JOIN dim_usuario u ON a.fk_usuario = u.user_key
  GROUP BY "user"
  ORDER BY "media" DESC
  LIMIT 1)



-- Desafio 2: A "Década de Ouro" do Cinema
/*Agrupando os filmes pelo ano_lancamento e descubrindo qual ano 
teve a melhor média de notas dada pelos usuários.*/
SELECT t.ano AS "ano",
	   AVG(a.rating) AS "media_ano"
  FROM fato_avaliacoes a
  JOIN dim_tempo t ON a.fk_tempo = t.date_key
 GROUP BY "ano"
 ORDER BY "media_ano" DESC
 LIMIT 10
 


-- Desafio 3: Sazonalidade
/*As pessoas dão notas melhores no fim de semana?*/
SELECT t.dia_semana AS "dia",
	   AVG(a.rating) AS "media_dia"
  FROM fato_avaliacoes a
  JOIN dim_tempo t ON a.fk_tempo = t.date_key
 GROUP BY "dia"
 ORDER BY "media_dia" DESC



-- Desafio 4: O Filme "Ame ou Odeie"
/*Quais filmes têm a maior variância (desvio padrão) nas notas?*/
SELECT f.title AS "titulo",
	   COUNT(a.rating) AS "Qtd Votos",
	   STDDEV(a.rating) AS "desvio",
	   ROUND(AVG(a.rating), 2) AS "Média"
  FROM fato_avaliacoes a
  JOIN dim_filme f ON a.fk_filme = f.movie_key
 GROUP BY "titulo"
HAVING COUNT(a.rating) > 10
 ORDER BY "desvio" DESC 