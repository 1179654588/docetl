import json
import operator
import os
import re
import traceback
from abc import ABC, abstractmethod
from hashlib import md5
from typing import Optional, Any

import pymysql.cursors
import requests
from loguru import logger
from lxml import etree
from pymysql.err import InterfaceError, OperationalError, DatabaseError, InternalError
from requests import Response


class NotFoundError(Exception):
    ...


class AbstractParser(ABC):
    def __init__(self, mysql_properties: dict):
        self.download_retry = 5
        self.sink_retry = 3
        self.bask_host = 'http://10.9.8.26:31010/open/'
        self.offset_path = os.path.join(os.getcwd(), 'offset_store')
        self.mysql_properties = mysql_properties
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

    @property
    def conn(self):
        return pymysql.connect(**self.mysql_properties, autocommit=True)

    @abstractmethod
    def parse(self, *args, **kwargs):
        ...

    @abstractmethod
    def setup(self, *args, **kwargs):
        ...

    def download(self, url: str, referer: str = None) -> Response:
        times = self.download_retry
        while times > 0:
            try:
                headers = {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "Pragma": "no-cache",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
                }
                if referer:
                    headers['Referer'] = referer

                if url.startswith('ip_right_marking'):
                    url = self.bask_host + url
                response = requests.get(url, headers=headers, verify=False, timeout=(30, 90))
                if response.status_code == 200 and response.content:
                    return response
                elif response.status_code == 404:
                    raise NotFoundError(f"Not Found, HTTP CODE 404!, {url}")
                else:
                    raise Exception(f"Download Failed!, {response}")
            except NotFoundError:
                raise
            except Exception as ex:
                times -= 1
                logger.warning(f"下载附件失败，重试中：{self.download_retry - times}, {ex}")
        raise Exception(f"Download Failed!, {url}")

    def sink(self, row: dict, table: str, unique_fields: dict) -> int:
        times = self.sink_retry
        while times:
            try:
                sql = f"""INSERT INTO {table} (%s) VALUES(%s)"""
                cols = ', '.join('`{}`'.format(k) for k in row.keys())
                vals = ', '.join('%({})s'.format(k) for k in row.keys())
                return self.cursor.execute(sql % (cols, vals), row)
            except pymysql.err.IntegrityError as ex:
                if "Duplicate entry" in str(ex) and table in unique_fields.keys():
                    logger.warning(f"【{table}】数据已存在, 进行更新操作：{row}")
                    set_clause = ', '.join(
                        ['`{}` = %({})s'.format(k, k) for k in row.keys() if k not in unique_fields[table]])
                    where_clause = ' AND '.join(
                        ['`{}` = %({})s'.format(k, k) for k in row.keys() if k in unique_fields[table]])
                    return self.cursor.execute(f"""UPDATE {table} SET {set_clause} WHERE {where_clause}""", row)
                else:
                    logger.warning(f"【{table}】数据已存在, 但未配置唯一约束：{row}")
                    raise
            except (InterfaceError, OperationalError, DatabaseError, InternalError) as ex:
                times -= 1
                logger.warning(f"数据库连接错误，重试 {self.sink_retry - times}/{self.sink_retry}: {ex}")
                self.conn.ping(reconnect=True)
                self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
            except Exception as ex:
                logger.error(f"数据入库失败：{ex}\n\t|- Exception: {traceback.format_exc()}")
                times -= 1
        raise Exception(f"数据插入失败：{row}\n{traceback.format_exc()}")

    def store_offset(self, table: str, offset: str) -> bool:
        if not os.path.exists(self.offset_path):
            os.makedirs(self.offset_path, exist_ok=True)
        with open(f'{self.offset_path}/{table}.offset', 'w') as fd:
            fd.write(str(offset))
        return True

    def load_offset(self, table: str) -> Optional[str]:
        if not os.path.exists(f'{self.offset_path}/{table}.offset'):
            return None

        with open(f'{self.offset_path}/{table}.offset', 'r') as fd:
            return fd.read().strip()

    def __del__(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


class AbstractHighTechParser(AbstractParser, ABC):
    def __init__(self, mysql_properties: dict, table_unique_fields: dict):
        super().__init__(mysql_properties=mysql_properties)
        self.table_unique_fields = table_unique_fields

    def setup(self):
        for record in self.parse():
            table = record.pop('table_name')
            # 处理额外信息
            if 'ext_json' in record and record['ext_json']:
                record['ext_json'] = json.dumps(record['ext_json'], ensure_ascii=False, separators=(',', ':'))
            # 处理attachment_info字段
            if 'attachment_info' in record and record['attachment_info']:
                attach_info = dict()
                attach_info['url'] = record['attachment_info']['url']
                attach_info['filename'] = record['attachment_info']['filename']
                record['attachment_info'] = json.dumps(attach_info, ensure_ascii=False, separators=(',', ':'))
            # 处理detail_url_gov字段
            if 'detail_url' in record and record['detail_url']:
                record['detail_url_gov'] = record['detail_url'] if 'gov.cn' in record.get('detail_url', '') else None
            # 处理attachment_info_gov字段
            if 'attachment_info_gov' in record and record['detail_url_gov']:
                record['attachment_info_gov'] = record['attachment_info']
            state = self.sink(row=record, table=table, unique_fields=self.table_unique_fields)
            logger.success(f"【{table}】数据插入/更新成功：rows: {state}, {record}")