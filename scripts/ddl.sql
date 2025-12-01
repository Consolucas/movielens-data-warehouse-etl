-- Tabelas simples e diretas
CREATE TABLE dim_filme (
    movie_key INT PRIMARY KEY,
    movie_id INT,
    title VARCHAR(255),
    ano_lancamento INT,
    genres VARCHAR(255)
);

CREATE TABLE dim_usuario (
    user_key INT PRIMARY KEY,
    user_id INT
);

CREATE TABLE dim_tempo (
    date_key INT PRIMARY KEY,
    data_completa DATE,
    ano INT,
    mes INT,
    dia_semana VARCHAR(50),
    trimestre INT
);

CREATE TABLE fato_avaliacoes (
    rating_id BIGINT PRIMARY KEY,
    fk_tempo INT,
    fk_usuario INT,
    fk_filme INT,
    rating DECIMAL(3,1),
    
    -- Amarras (Constraints) básicas
    FOREIGN KEY (fk_tempo) REFERENCES dim_tempo(date_key),
    FOREIGN KEY (fk_usuario) REFERENCES dim_usuario(user_key),
    FOREIGN KEY (fk_filme) REFERENCES dim_filme(movie_key)
);