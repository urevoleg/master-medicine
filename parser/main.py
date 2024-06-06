# базовые библиотеки
import os
import re
import json
import dataclasses
import yaml

import requests
import typing as t

import pandas as pd
from tqdm.auto import tqdm

import warnings
warnings.filterwarnings('ignore')

from bs4 import BeautifulSoup

from dotenv import load_dotenv
load_dotenv()


import logging
from logging import StreamHandler


@dataclasses.dataclass
class ParseObj:
    start_number: str
    end_number: str
    url_template: str
    page_inc: int
    padding: int | None


def get_logger(logger_name="super_logger", handler_adds=None):
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    super_logger = logging.getLogger(logger_name)
    super_logger.setLevel(logging.DEBUG)
    handler = StreamHandler()
    handler.setFormatter(fmt=formatter)
    super_logger.addHandler(handler)

    if not handler_adds is None:
        handler_adds.setFormatter(fmt=formatter)
        handler_adds.setLevel(logging.DEBUG)
        super_logger.addHandler(handler_adds)
    return super_logger


def read_yml(yml_path: str = '../src/zdrav.yml'):
    # Чтение YML файла
    with open(yml_path, 'r') as file:
        data = yaml.safe_load(file)

    # Итерация по данным и печать годов и секций
    for item in data['zdrav']:
        year = item['year']
        page_inc = item["page_inc"]
        # print(f"Year: {year}")
        sections = item['sections']
        for section in sections:
            section.update({"year": year, "page_inc": page_inc})
            yield section


def extract_page_numbers_and_url_template(start_url: str, end_url: str, year: int = 2001) -> ParseObj:
    if year == 2001:
        start_number = int(re.findall(r'/i(.*)r.htm', start_url)[0]) if re.findall(r'/i(.*)r.htm', start_url) else -1
        end_number = int(re.findall(r'/i(.*)r.htm', end_url)[0]) if re.findall(r'/i(.*)r.htm', start_url) else -1
        url_template = "/".join(start_url.split("/")[:-1]) + "/i0%sr.htm"
        padding = None
        page_inc = 10
    if year == 2005:
        page_inc = 1
        if len(start_url.split("-")) == 3:
            start_number = int(re.findall(r'/\d\d-\d\d-(.*).htm', start_url)[0]) if re.findall(r'/\d\d-\d\d-(.*).htm', start_url) else -1
            end_number = int(re.findall(r'/\d\d-\d\d-(.*).htm', end_url)[0]) if re.findall(r'/\d\d-\d\d-(.*).htm', end_url) else -1
            url_template = "/".join(start_url.split("/")[:-1]) + "/" + re.sub(r'\d(?=\D*$)', '%s', start_url.split("/")[-1])
            padding = None
        if len(start_url.split("-")) == 2:
            start_number = int(re.findall(r'/\d\d-(.*).htm', start_url)[0]) if re.findall(r'/\d\d-(.*).htm', start_url) else -1
            end_number = int(re.findall(r'/\d\d-(.*).htm', end_url)[0]) if re.findall(r'/\d\d-(.*).htm', end_url) else -1
            url_template = "/".join(start_url.split("-")[:-1]) + "-%s.htm"
            padding = 2
    if year == 2007 or year == 2009 or year == 2011:
        page_inc = 1
        if len(start_url.split("/")[-1]) == 8:
            start_number = int(re.findall(r'/\d-(.*).htm', start_url)[0]) if re.findall(r'/\d-(.*).htm',
                                                                                        start_url) else -1
            end_number = int(re.findall(r'/\d-(.*).htm', end_url)[0]) if re.findall(r'/\d-(.*).htm',
                                                                                      end_url) else -1
            url_template = "/".join(start_url.split("-")[:-1]) + "-%s.htm"
            padding = 2
        else:
            if len(start_url.split("-")) == 3:
                start_number = int(re.findall(r'/\d\d-\d\d-(.*).htm', start_url)[0]) if re.findall(
                    r'/\d\d-\d\d-(.*).htm', start_url) else -1
                end_number = int(re.findall(r'/\d\d-\d\d-(.*).htm', end_url)[0]) if re.findall(r'/\d\d-\d\d-(.*).htm',
                                                                                               end_url) else -1
                url_template = "/".join(start_url.split("/")[:-1]) + "/" + re.sub(r'\d(?=\D*$)', '%s',
                                                                                  start_url.split("/")[-1])
                padding = None
            if len(start_url.split("-")) == 2:
                start_number = int(re.findall(r'/\d\d-(.*).htm', start_url)[0]) if re.findall(r'/\d\d-(.*).htm',
                                                                                              start_url) else -1
                end_number = int(re.findall(r'/\d\d-(.*).htm', end_url)[0]) if re.findall(r'/\d\d-(.*).htm',
                                                                                          end_url) else -1
                url_template = "/".join(start_url.split("-")[:-1]) + "-%s.htm"
                padding = 2

    obj = ParseObj(start_number=start_number,
                   end_number=end_number,
                   url_template=url_template,
                   page_inc=page_inc,
                   padding=padding)
    return obj


def create_url(number: str, url_template: str, padding: t.Union[int, None] = None) -> str:
    if padding:
        number = number.zfill(padding)
    url = url_template % number
    return url


def parse(start_url: str,
          end_url: str,
          year: int,
          page_inc: int,
          early_stop: int = 10,
          logger: logging.Logger = get_logger(logger_name=__file__)):

    res = []
    obj = extract_page_numbers_and_url_template(start_url=start_url,
                                                end_url=end_url,
                                                year=year)
    number = obj.start_number
    end_number = obj.end_number
    url_template = obj.url_template

    logger.debug(obj)

    if number != -1:
        cnt = 1
        while number <= end_number:

            tmp_url = create_url(number=str(number), url_template=url_template, padding=obj.padding)
            logger.info(f"Page url: {tmp_url}")
            dfs = parse_one_url(url=tmp_url,
                                logger=logger)
            res += [dfs]

            number += page_inc

            if early_stop and cnt > early_stop:
                logger.info(f"⏰ Early stop")
                break
            cnt += 1
        else:
            logger.info(f"🔚 tmp_url == end_url")

        return res

    else:
        logger.info(f"No correct page number!")
        return res


def parse_one_url(url: str,
                  logger: logging.Logger = get_logger(logger_name=__file__)) -> t.List[pd.DataFrame]:
    cfo_dfs = []
    try:
        response = requests.get(url)
        response.raise_for_status()
        html = response.content

        soup = BeautifulSoup(html, 'html')
        page_title = soup.title.text.strip().lower() if soup.title else url

        tmp = pd.read_html(html, encoding="cp1251", decimal=",", thousands=None)

        if tmp != []:
            for df in tmp:
                # headers = df.iloc[0, :]
                # data = df.iloc[1:, :]
                # data.columns = headers.to_list()
                # data.name = page_title
                # data.url = url

                df.name = page_title
                df.url = url
                cfo_dfs.append(df)
    except Exception as e:
        error_str = f"{e}\t{url}"
        logger.debug(error_str)

    return cfo_dfs


def save(list_of_dfs: t.List[pd.DataFrame],
         year: int,
         folder: str,
         logger: logging.Logger = get_logger(logger_name=__file__)):

    """

    :param list_of_dfs:
    :param year:
    :param folder: относительно папки скрипта parser/main.py
    :param logger:
    :return:
    """

    os.makedirs(folder, exist_ok=True)
    l_of_d_hash_names = []
    cnt = 0
    for df in tqdm(list_of_dfs):
        # коcтыль
        if len(df) == 0:
            pass
        elif len(df) == 1:
            df = df[0]
            try:
                path = f"{folder}/{df.name}.csv"
                df.to_csv(path, index=None)
            except OSError as e:
                import hashlib
                hash_object = hashlib.md5(df.name.encode())
                hash_str = str(hash_object.hexdigest())
                path = f"{folder}/{hash_str}.csv"
                df.to_csv(path, index=None)

                print(f"Добавили в словарь: {hash_str}, {df.name}")
                l_of_d_hash_names.append([hash_str, df.name])
                cnt += 1

                with open(f"{folder}/names_mapping_{hash_str}.csv", 'w') as f:
                    f.write("hash,table_name\n")
                    f.write(f"{hash_str},{df.name}\n")
        else:
            raise Exception(f"❌ Hi, bro, It is some datasets in page: {df[0].url}!")

    assert len(l_of_d_hash_names) == cnt, "🔴 Something wrong with the hashing!"
    rows = [dict(zip(["hash", "table_name"], row)) for row in l_of_d_hash_names]
    # pd.DataFrame(rows).to_csv(f"{folder}/names_mapping.csv")


if __name__ == "__main__":
    logger = get_logger(logger_name=__file__)

    FOLDER = "dev"

    for s in read_yml():
        # print(s)
        if s["year"] in (2013,):
            dfs = parse(start_url=s["start_url"],
                                 end_url=s["end_url"],
                                 year=s["year"],
                                 page_inc=s["page_inc"],
                                 early_stop=None,
                                 logger=logger)

            save(list_of_dfs=dfs, year=s["year"], folder=f'../{FOLDER}/{s["year"]}/{s["item_name"]}')
