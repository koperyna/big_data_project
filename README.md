# big_data_project
## Виконала студентка КН-415 Пожарицька Катерина
У цьому проекті ми розглядаємо IMDB датасет та створюємо запити до розподілених даних.  
Середовищем роботи був Google Colab. 
## _Підготовка середовища_
Підключаємо середовище до Google Диску та вказуємо шлях до датасету:
```
from google.colab import drive
drive.mount('/content/drive')

#шлях до датасету
imdb_path = "/content/drive/MyDrive/imdb/"
```
Створюємо SparkSession:
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("IMDb Big Data Project").getOrCreate(
```
Імпортуємо потрбіні функції SQL:
```
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
```
## _Датафрейми_  
Створюємо схеми для кожного датафрейму по типу:
```
name_basics_schema = StructType([
        StructField("nconst", StringType(), True),
        StructField("primaryName", StringType(), True),
        StructField("birthYear", IntegerType(), True),
        StructField("deathYear", IntegerType(), True),
        StructField("primaryProfession", StringType(), True),
        StructField("knownForTitles", StringType(), True),
    ])
...
```
Створюємо самі датафрейми по кожному з файлів датасету:
```
df_people = spark.read.csv(imdb_path + "name.basics.tsv", sep="\t", header=True, schema=name_basics_schema)

df_akas = spark.read.csv(imdb_path + "title.akas.tsv", sep="\t", header=True, schema=title_akas_schema)

...
```
Показуємо основну інформацію про кожний з датафреймів:
```
df_title.show(5)
df_title.printSchema()
print("Кількість рядків:", df_title.count())
print("Кількість стовпців:", len(df_title.columns))
df_title.select("isAdult", "runtimeMinutes").describe().show(20)

df_people.show(5)
df_people.printSchema()
print("Кількість рядків:", df_people.count())
print("Кількість стовпців:", len(df_people.columns))
df_people.select("birthYear", "deathYear").describe().show(20)

...
```
## _Переходимо до бізнес-питань_
Потрібно було мати:
> не менше 3 питання де використовується filters  
> не менше 2 питання де використовується join  
> не менше 2 питання де використовується group by  
> не менше 2 питання де використовується window functions

### Бізнес-питання №1: Фільми українською  
Використано: filter()
```
#фільми українською
#FILTER
ukr_movies = df_akas.filter(col("language") == 'uk').select("title").distinct()
ukr_movies.show(20)
```
### Бізнес-питання №2: Топ 5 фільмів кожною мовою  
Використано: join(), window

```
#топ 5 фільмів кожною мовою
#WINDOW and JOIN
df_akas_with_ratings = df_akas.join(df_ratings, df_akas.titleId == df_ratings.tconst, "inner")
window = Window.partitionBy("language").orderBy(col("averageRating").desc())
top_5_lang = df_akas_with_ratings.withColumn("rank", rank().over(window)).filter(col("rank") <= 5)
top_5_lang.show(20)
```
### Бізнес-питання №3: Фільми після 2015  
Використано: filter()  
```
#фільми після 2015
#FILTER
after_2015_movies = df_title.filter(col("startYear") > '2015').select("primaryTitle", "startYear")
after_2015_movies.show(20)
```
### Бізнес-питання №4: Фільми, довші за 122 хвилин  
Використано: filter()  
```
#фільми довші за 122 хв
#FILTER
more_122_min = df_title.filter(col("runtimeMinutes").cast("int") > 122).select("primaryTitle", "runtimeMinutes")
more_122_min.show(20)
```
### Бізнес-питання №5: Кількість фільмів по жанрах  
Використано: groupBy()  
```
#кількість фільмів по жанрах
#GROUPBY
amount_movies_per_genre = df_title.groupBy("genres").count().orderBy("count", ascending=False)
amount_movies_per_genre.show(20)
```
### Бізнес-питання №6: Середня тривалість фільмів по жанрах  
Використано: groupBy()  
```
#Cередня тривалість фільмів по жанрах
#GROUPBY
avg_length_movies_per_genre = df_title.withColumn("runtime", col("runtimeMinutes").cast("int")) \
    .groupBy("genres").avg("runtime").orderBy("avg(runtime)", ascending=False)
avg_length_movies_per_genre.show(20)
```
### Бізнес-питання №7: Топ 3 фільми за рейтингом у кожному жанрі  
Використано: join(), window  
```
#топ3 фільми за рейтингом у кожному жанрі 
#WINDOW
df_joined = df_title.join(df_ratings, "tconst")
windowSpec = Window.partitionBy("genres").orderBy(col("averageRating").desc())
top_3_each_genre = df_joined.withColumn("rank", rank().over(windowSpec)).filter(col("rank") <= 3)
top_3_each_genre.select("primaryTitle", "genres", "averageRating", "rank").show(20)

```
## _Запис результатів_  
Запис результатів відбувається на Google Диск для кожного з запитів.  
```
top_3_each_genre.write.csv("/content/drive/MyDrive/imdb/output/top3_each_genre.csv", header=True, mode="overwrite")
print("Запис результатів завершено!")

ukr_movies.write.csv("/content/drive/MyDrive/imdb/output/ukr_movies.csv", header=True, mode="overwrite")
print("Запис результатів завершено!")

...
```

