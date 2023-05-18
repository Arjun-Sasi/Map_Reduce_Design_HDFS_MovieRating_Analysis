#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Mapper: 
 
###!/usr/bin/env python 
 
import sys 
 
def load_years(filename):     years = set()     with open(filename) as f:         for line in f:             years.update(line.strip().split()) 
    return years 
 
years = load_years("years.txt") 
all_years = not bool(years) 
 
for line in sys.stdin:     uid, title, genres, year, rating = line.strip().split('\t')     if all_years or year in years:         for genre in genres.split('|'): 
            print(f"{genre}\t{title}|{rating}\t1") 
#Reducer: 
 
###In this case the reducer this version of the reducer now calculates the average rating using the sum and count of ratings for each title and it still checks whether a movie has at least 15 ratings.  
             #!/usr/bin/env python 
 
import sys 
 
current_genre = None current_title = None current_sum = 0 current_count = 0 highest_avg_rating = 0 
MIN_RATING_COUNT = 15 
 
for line in sys.stdin: 
    genre, title_rating, rating_count = line.strip().split('\t') 
    title, rating = title_rating.split('|')     rating = float(rating) 
    rating_count = int(rating_count) 
 
    if current_genre == genre:         if current_title == title:             current_sum += rating             current_count += rating_count         else:             if current_count >= MIN_RATING_COUNT:                 avg_rating = current_sum / current_count                 if avg_rating > highest_avg_rating:                     highest_avg_rating = avg_rating 
                    highest_rated_title = current_title 
             
            current_title = title             current_sum = rating             current_count = rating_count     else:         if current_genre and current_title and current_count >= MIN_RATING_COUNT:             print(f"{current_genre}\t{highest_rated_title}\t{highest_avg_rating:.1f}") 
 
        current_genre = genre         current_title = title         current_sum = rating         current_count = rating_count 
        highest_avg_rating = 0 
 
if current_genre and current_title and current_count >= MIN_RATING_COUNT: 
    print(f"{current_genre}\t{current_title}\t{highest_avg_rating:.1f}") 

