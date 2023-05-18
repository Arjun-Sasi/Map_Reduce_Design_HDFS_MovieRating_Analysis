#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python 
 
import sys 
 
def load_years(filename):     years = set()     with open(filename) as f:         for line in f:             years.update(line.strip().split()) 
    return years 
 
years = load_years("years.txt") 
all_years = not bool(years) 
 
for line in sys.stdin:     uid, title, genres, year, rating = line.strip().split('\t')     if all_years or year in years:         for genre in genres.split('|'): 
            print(f"{genre}\t{title}|{rating}\t1") 
 
 
Combiner: 
 
#!/usr/bin/env python 
 
import sys 
 
current_genre = None 
current_title_ratings = {} 
 
for line in sys.stdin:     genre, title_rating, count = line.strip().split('\t')     title, rating = title_rating.split('|') 
    rating, count = float(rating), int(count) 
 
    if current_genre == genre:         if title in current_title_ratings:             current_title_ratings[title].append(rating)         else:             current_title_ratings[title] = [rating]     else:         if current_genre:             for title, ratings in current_title_ratings.items(): 
                avg_rating = sum(ratings) / len(ratings) 
                print(f"{current_genre}\t{title}|{avg_rating}\t{len(ratings)}")         current_genre = genre 
        current_title_ratings = {title: [rating]} 
 
if current_genre: 
    for title, ratings in current_title_ratings.items(): 
        avg_rating = sum(ratings) / len(ratings) 
        print(f"{current_genre}\t{title}|{avg_rating}\t{len(ratings)}") 
 
 
Reducer: 
 
#!/usr/bin/env python 
 
import sys 
 
current_genre = None current_title = None current_sum = 0 current_count = 0 
highest_avg_rating = 0 MIN_RATING_COUNT = 15 for line in sys.stdin:     genre, title_rating, rating_count = line.strip().split('\t')     title, avg_rating = title_rating.split('|')     avg_rating = float(avg_rating) 
    rating_count = int(rating_count) 
 
    if current_genre == genre:         if rating_count >= MIN_RATING_COUNT:             current_sum += avg_rating * rating_count             current_count += rating_count 
 
            if avg_rating > highest_avg_rating:                 highest_avg_rating = avg_rating 
                current_title = title     else: 
        if current_genre and current_title: 
            print(f"{current_genre}\t{current_title}\t{highest_avg_rating:.1f}") 
 
        current_genre = genre         if rating_count >= MIN_RATING_COUNT: 
            current_title = title 
            current_sum = avg_rating * rating_count             current_count = rating_count             highest_avg_rating = avg_rating         else: 
            current_title = None             current_sum = 0             current_count = 0 
            highest_avg_rating = 0 
 
if current_genre and current_title: 
    print(f"{current_genre}\t{current_title}\t{highest_avg_rating:.1f}") 

