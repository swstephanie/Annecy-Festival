# -*- encoding: utf-8 -*-
'''
@File    :   main.py   
@Contact :   
@License :   (C)Copyright 2018-2021
 
@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
4/22/22 3:28 PM   Suwei Wang      1.0         None
'''

import requests
from bs4 import BeautifulSoup
import bs4
import pandas as pd
import re
import time
import warnings
warnings.filterwarnings("ignore")
import datetime
from multiprocessing import Pool,Process
import multiprocessing


def requests_and_check(url):
    response = requests.get(url)
    if not response.status_code == 200:
        print("YES")
    try:
        results_page = BeautifulSoup(response.content, 'lxml')
        return results_page
    except:
        print("ERROR")
        return None


def get_archives_info(url='https://www.annecy.org/about/archives'):
    results_page = requests_and_check(url)

    df = pd.DataFrame(columns=['title', 'year', 'link'])
    if results_page is None:
        return df

    for block in results_page.find_all('div', class_='clearfix'):
        year_list = block.find_all('li')
        for year in year_list:
            new_row = {}
            new_row['link'] = year.find('a').get('href').strip()
            title_string = year.find('a').get('title')
            new_row['title'] = re.split(r"\s(?=[0-9])", title_string, maxsplit=1)[0].strip()
            new_row['year'] = re.split(r"\s(?=[0-9])", title_string, maxsplit=1)[1].strip()
            df = df.append(new_row, ignore_index=True)
    return df


def get_official_selection_df_year(
        year_url='https://www.annecy.org/about/archives/2021/official-selection', \
        year='2021'):
    title = 'Official selection'
    results_page = requests_and_check(year_url)
    df = pd.DataFrame(columns=['title', 'year', 'selection', 'link'])

    if results_page is None:
        return df

    for i in results_page.find('div', class_='grd-cat__list').find_all('a'):
        new_row = {}
        new_row['selection'] = i.get_text().strip()
        new_row['link'] = i.get('href').strip()
        df = df.append(new_row, ignore_index=True)
    df.title = title
    df.year = year
    return df


def get_awards_df_year(year_url='https://www.annecy.org/about/archives/2021/award-winners', \
                       year='2021'):
    title = 'Awards'
    results_page = requests_and_check(year_url)
    df = pd.DataFrame(columns=['title', 'year', 'department', 'award', 'film', 'img_link', 'film_link'])
    if results_page is None:
        return df

    if int(year) >= 2011:
        ls = []
        for i in results_page.find('div', {'id': 'palmares'}):
            if type(i) == bs4.element.Tag:
                ls.append(i)
        for i, j in zip(ls[0::2], ls[1::2]):
            department = i.get_text().strip()
            for item in j.find_all('li'):
                item_dict = {}
                item_dict['department'] = department
                item_dict['award'] = item.find('h2').get_text()
                item_dict['film'] = item.find('h4').get_text()
                item_dict['img_link'] = item.find('img').get('src')
                item_dict['film_link'] = item.find('a').get('href')

                df = df.append(item_dict, ignore_index=True)
    else:
        depts = results_page.find('div', class_='blc p_com').find_all('div', class_="palm_categ")
        dept_films = results_page.find('div', class_='blc p_com').find_all('ul')

        for dept, films in zip(depts, dept_films):
            department = dept.get_text().strip()
            for film in films.find_all('li'):
                item_dict = {}
                item_dict['department'] = department
                item_dict['award'] = film.find('h6').get_text()
                item_dict['film'] = film.find('h3').get_text()
                item_dict['img_link'] = film.find('img').get('src')
                item_dict['film_link'] = film.find('a').get('href')

                df = df.append(item_dict, ignore_index=True)

    df.year = year
    df.title = title
    return df


def get_award_df():
    df = get_archives_info()
    df = df[df.title == 'Découvrez le Palmarés']
    years = df.year.tolist()
    year_urls = df.link.tolist()
    output = pd.DataFrame(columns=['title', 'year', 'department', 'award', 'film', 'img_link', 'film_link'])
    for year, year_url in zip(years, year_urls):
        output = pd.concat([output, get_awards_df_year(year_url=year_url, year=year)], ignore_index=True)
    return output


def get_film_info(url='https://www.annecy.org/about/archives/2021/official-selection/film-index:film-20211299'):
    results_page = requests_and_check(url)
    if results_page is None:
        return pd.DataFrame()
    new_dict = {}

    results = results_page.find("div", class_='blc_identite').find_all('div', class_="sous-blc_content")

    for i in range(len(results)):
        for j in results[i].find_all('p'):
            lt = j.get_text().split(":", 1)
            ###TEST
            try:
                new_dict[lt[0].strip()] = lt[1].strip()
            except:
                continue
    try:
        new_dict['Overview'] = results_page.find('div', class_='accroche').get_text().strip()
    except:
        new_dict['Overview'] = ''
    if 'df' not in locals():
        df = pd.DataFrame(new_dict, index=[0])
    else:
        df = df.append(new_dict, ignore_index=True)
    return df

def get_dept_in_official_selection_year(args):
    assert len(args) == 2, "must be a year and url pair"
    year = args[0]
    url = args[1]
    df = pd.DataFrame(columns=['year'])
    results_page = requests_and_check(url)
    for dept in results_page.find('div', class_='grd-cat__item').find_all('a'):
        department = dept.get_text().strip()
        dept_url = dept.get('href')
        dept_page = requests_and_check(dept_url)
        if dept_page.find('ul', class_='liste_films') is None:
            continue
        for film in dept_page.find('ul', class_='liste_films').find_all('li'):
            film_url = film.find('a').get('href')
            film_df = get_film_info(film_url)
            # test
            if film_df is None:
                return "YES"
            film_df['department'] = department
            df = pd.concat([df, film_df], ignore_index=True)

    df.year = year
    return df
def get_awards_and_film_info():
    df = get_award_df()
    iter_list = df.film_link.tolist()
    pool = multiprocessing.Pool(processes=4)
    result_list = pool.map(get_film_info, iter_list)
    res = pd.concat(result_list, ignore_index=True)
    df = pd.concat([df, res], axis=1)
    df.to_csv('awards_with_film_info.csv',index= False)

def get_official_selection():
    df = get_archives_info()
    official_selection_year_list = df[df.title == 'Official selection'].year.tolist()
    official_selection_link_list = df[df.title == 'Official selection'].link.tolist()
    iter_list = [[i, j] for i, j in zip(official_selection_year_list,official_selection_link_list)]
    pool = multiprocessing.Pool(processes=4)

    result_list = pool.map(get_dept_in_official_selection_year, iter_list)
    res = pd.concat(result_list,ignore_index=True)

    res.to_csv('official_selection.csv',index = False)


if __name__ == '__main__':
    start = time.time()
    #get_official_selection()
    #get_awards_and_film_info()

    print("Success!!!!")
    print('Elapsed time', round((time.time() - start)/60,2), 'min')






