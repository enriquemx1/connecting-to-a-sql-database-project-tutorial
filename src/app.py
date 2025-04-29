import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Paso 1: Cargar variables de entorno
load_dotenv()
DB_PATH = os.getenv("DB_PATH", "./labaseuno.db")  # Usar ruta correcta desde .env

# Paso 2: Conectar a la base de datos SQLite
engine = create_engine(f"sqlite:///{DB_PATH}")

# Verificar que la base de datos está bien conectada
print(f"Conectado a la base de datos en: {DB_PATH}")

# Paso 3: Crear tablas si no existen
with engine.connect() as connection:
    connection.execute(text("""
    CREATE TABLE IF NOT EXISTS publishers (
        publisher_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    );
    """))

    connection.execute(text("""
    CREATE TABLE IF NOT EXISTS authors (
        author_id INTEGER PRIMARY KEY,
        first_name TEXT NOT NULL,
        middle_name TEXT NULL,
        last_name TEXT NULL
    );
    """))

print("Tablas 'publishers' y 'authors' creadas correctamente.")

# Paso 4: Insertar datos asegurando que se guarden correctamente
publishers = [
    (1, 'Editorial Santillana'),
    (2, 'Grupo Planeta'),
    (3, 'Siglo XXI Editores'),
    (4, 'Alfaguara'),
    (5, 'Editorial Norma'),
    (6, 'Ediciones SM'),
    (7, 'Fondo de Cultura Económica')
]

authors = [
    (1, 'Gabriel', None, 'García Márquez'),
    (2, 'Laura', None, 'Esquivel'),
    (3, 'Mario', None, 'Vargas Llosa'),
    (4, 'Isabel', None, 'Allende'),
    (5, 'Octavio', None, 'Paz'),
    (6, 'Jorge', 'Luis', 'Borges'),
    (7, 'Julio', None, 'Cortázar'),
    (8, 'Carlos', None, 'Fuentes')
]

with engine.begin() as connection:  # `begin()` asegura que los cambios sean permanentes
    for pub in publishers:
        connection.execute(text("INSERT OR IGNORE INTO publishers (publisher_id, name) VALUES (:id, :name)"),
                           {"id": pub[0], "name": pub[1]})

    for author in authors:
        connection.execute(text("INSERT OR IGNORE INTO authors (author_id, first_name, middle_name, last_name) VALUES (:id, :fn, :mn, :ln)"),
                           {"id": author[0], "fn": author[1], "mn": author[2], "ln": author[3]})

print("Datos insertados correctamente en las tablas.")

# Paso 5: Consultar los datos con Pandas
df_publishers = pd.read_sql("SELECT * FROM publishers;", engine)
df_authors = pd.read_sql("SELECT * FROM authors;", engine)

print("\nDatos de publishers:")
print(df_publishers)

print("\nDatos de authors:")
print(df_authors)

