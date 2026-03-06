"""
创新型中小企业数据导入器

功能描述：
    1. 从ODS层查询创新型中小企业相关公告数据
    2. 解析附件（支持HTML、Excel、PDF、Word、RAR等格式）
    3. 提取企业名称等关键信息
    4. 生成映射规则表和原始数据表
    5. 关联企业digest并生成最终数据表

主要流程：
    Stage 1: query() - 查询ODS数据
    Stage 2: parse() - 解析附件并生成mapping_rule和raw_data
    Stage 3: process_final_data() - 生成最终数据表

Author: [yqz]
Date: 2025-11-06
"""

import hashlib
import json
import operator
import os
import sys
import re
import traceback
from datetime import datetime, timedelta
from io import BytesIO
from typing import List, Tuple, Optional
from urllib.parse import urlparse

# ==================== 第三方库导入 ====================
import pandas as pd
import pymysql.cursors
import rarfile
import requests
import unicodedata
from dateutil.relativedelta import relativedelta
from loguru import logger
from lxml import etree
from pymysql.cursors import DictCursor

# ==================== 项目内部导入 ====================
from abstract import AbstractHighTechParser
from db_config import TEST_DB_CONFIG, DWD_DB_CONFIG, ODS_DB_CONFIG
from constants import TABLE_UNIQUE_KEY_FIELDS
from models.honor_innovative_sme import (
    InnovativeSMERawDataModel, 
    InnovativeSMEMappingRuleModel
)
from utils import get_digest_by_rule_one, n_company_name, get_location
from docetl_api import extract_companies

class InnovativeSME(AbstractHighTechParser):
    """
    创新型中小企业数据处理类
    
    继承自 AbstractHighTechParser，实现创新型中小企业数据的
    查询、解析、转换和入库功能。
    
    Attributes:
        notice_type (int): 公告类型（0=公示，1=通知/公布）
        status (int): 当前状态（0=有效，1=过期失效，10=变动记录）
        ods_id_list (list): 需要处理的ODS ID列表
        raw_table (str): 原始数据表名
        mapping_table (str): 映射规则表名
        final_table (str): 最终数据表名
    """
    
    def __init__(self, mysql_properties: dict, ods_id_list: Optional[list] = None):
        """
        初始化创新型中小企业处理器
        
        Args:
            mysql_properties (dict): MySQL连接配置
            ods_id_list (list, optional): 需要处理的ODS ID列表
        """
        super().__init__(
            mysql_properties=mysql_properties, 
            table_unique_fields=TABLE_UNIQUE_KEY_FIELDS
        )
        
        # ==================== AI提示词配置 ====================
        self.prompt = """请分析以下段落文本：
        {{ input.text }}
        
        你的任务是识别并提取文本中隐含的表格数据。请提取出所有提及的企业信息，并以列表嵌套json的形式返回。
        对于每一条提取到的记录，必须包含以下字段：
        - 序号: 文本中对应的序号
        - 企业名称: 企业的全称
        - 企业地址: 企业的具体地址
        
        如果当前段落中没有包含符合条件的企业表格信息，请返回一个空列表 []。"""
        
        # ==================== 业务配置 ====================
        self.notice_type = 1   # 公告类型（0=公示，1=通知/公布）
        self.status = None     # 当前状态（0=有效，1=过期失效，10=变动记录）
        self.is_review = 0      # 是否是复核通过记录（0否，1是）
        self.ods_id_list = ods_id_list

        # ==================== 表名配置 ====================
        self.raw_table = "dwd_honor_innovative_smes_raw_data"
        self.mapping_table = "dwd_honor_innovative_smes_mapping_rule"
        self.final_table = "dwd_honor_innovative_smes"

        # ==================== 统计信息配置 ====================
        self.success_attachments = []  # 成功解析的附件列表 [{ods_id, filename, url}]
        self.failed_attachments = []   # 失败解析的附件列表 [{ods_id, filename, url, error}]
        self.ods_parse_status = {}     # 记录每个ods_id的解析状态

    # ==================== 数据查询模块 ====================
    
    def query(self):
        """
        从ODS层查询创新型中小企业公告数据
        
        查询逻辑：
            1. 根据ods_id_list查询ods_technology_notice表
            2. 关联ods_technology_notice_parse_result获取附件信息
            3. 只查询use_flag=0的有效数据
        
        Returns:
            list[dict]: 查询结果列表，每条记录包含公告基本信息和附件信息
        """
        dwd_mysql = pymysql.connect(**ODS_DB_CONFIG)
        cursor = dwd_mysql.cursor(pymysql.cursors.DictCursor)
        
        # 检查ods_id_list是否为空
        if not self.ods_id_list:
            logger.warning("ods_id_list 为空，没有数据需要查询")
            return []
        
        # 构建 IN 子句的占位符
        placeholders = ','.join(['%s'] * len(self.ods_id_list))
        
        # SQL查询语句
        sql = f"""
            select t1.*, t2.attachment_download_info, t2.download_flag 
            from (
                select * from bdp_sti.ods_technology_notice 
                where id in ({placeholders})
                and use_flag = 0
            ) as t1
            inner join (
                select id, attachment_info as attachment_download_info, download_flag 
                from bdp_sti.ods_technology_notice_parse_result
            ) as t2 
            on t1.id = t2.id 
            order by t1.id
        """
        
        # 使用参数化查询（防止SQL注入）
        cursor.execute(sql, self.ods_id_list)
        
        data = cursor.fetchall()
        cursor.close()
        dwd_mysql.close()
        return data

    # ==================== 附件解析模块 ====================
    
    def parse_attachment(self, attach: dict, referer: str, **kwargs) -> list[dict]:
        """
        根据附件类型选择相应的解析方法
        
        支持的附件类型：
            - HTML/HTM/SHTML: 解析网页表格
            - XLS/XLSX/ET: 解析Excel文件
        
        Args:
            attach (dict): 附件信息字典
            referer (str): 请求来源URL
            **kwargs: 其他参数（如record等）
        
        Returns:
            list[dict]: 解析后的数据列表
        """
        # 获取附件类型
        doc_type = attach.get(
            'attachment_type', 
            os.path.splitext(urlparse(attach.get('url', '')).path)[-1].strip('.')
        ).lower()
        
        # 从文件名提取类型
        if not doc_type:
            if suffix := re.match(
                r'.*?\.(xls|xlsx|docx|doc|pdf|wps|ed|et|html|htm|shtml).*?', 
                attach['filename'], 
                18
            ):
                doc_type = suffix.group(1).lower()
        
        # 从HTTP响应头提取类型
        if not doc_type:
            if filename := re.match(
                r"attachment;.*?filename[*]?=.*?\.(xls|xlsx|docx|doc|pdf|wps|ed|et|html|htm|shtml).*?",
                requests.head(attach['url'], timeout=30).headers.get('Content-Disposition', ''),
                18
            ):
                doc_type = filename.group(1)
        try:
            url = attach.get('store_path', attach.get('url'))
            if url.startswith('ip_right_marking'):
                url = 'http://10.9.8.26:31010/open/' + url
            input_data = [{"url": url}]
            # 根据文件类型选择解析方法
            if doc_type in ('html', 'shtml', 'htm') and not operator.contains(attach['url'], 'mp.weixin.qq.com'):
                _, output = extract_companies(
                    input_data=input_data,
                    file_type="html",
                    parsing_config={
                        "ocr_for_images": "True",
                        "lang": "ch",
                        "lines_per_chunk": "100",
                        "overlap_lines": "5"
                    },
                    prompt=self.prompt
                )
                return self.get_json_data(output)
            elif doc_type in ('pdf'):
                _, output = extract_companies(
                    input_data=input_data,
                    file_type="pdf",
                    parsing_config={
                        "ocr_enabled": "True",
                        "doc_per_page": "True",
                        "lang": "ch"
                    },
                    prompt=self.prompt
                )
                return self.get_json_data(output)
            elif doc_type in ('xls', 'xlsx', 'excel', 'et'):
                _, output = extract_companies(
                    input_data=input_data,
                    file_type="excel",
                    parsing_config={
                        "orientation": "row",
                        "col_order": "企业名称,序号",
                        "doc_per_sheet": "True",
                        "max_rows_per_str": "50"
                    },
                    prompt=self.prompt
                )
                return self.get_json_data(output)
            elif doc_type in ('doc', 'docx', 'wps', 'word'):
                _, output = extract_companies(
                    input_data=input_data,
                    file_type="docx",
                    parsing_config={
                        "lines_per_chunk": "100",
                        "overlap_lines": "5"
                    },
                    prompt=self.prompt
                )
                return self.get_json_data(output)

        except Exception as ex:
            logger.error(f"解析附件失败，附件信息：{attach}，错误信息：{str(ex)}")

    # ==================== 主解析流程 ====================
    
    def parse(self):
        """
        主解析流程：遍历查询结果并解析每个附件
        
        处理流程：
            1. 查询ODS数据
            2. 获取附件信息（优先使用attachment_download_info）
            3. 遍历每个附件进行解析
            4. 特殊处理RAR压缩包
            5. 生成mapping_rule和raw_data
    
        Yields:
            dict: 解析后的数据记录（包含table_name标识）
        """
        for record in self.query():
            logger.info(f"开始处理ODS记录ID：{record['id']}")
            ods_id = record['id']
            
            # 初始化该ods_id的统计信息
            if ods_id not in self.ods_parse_status:
                self.ods_parse_status[ods_id] = {
                    'total': 0,
                    'success': 0,
                    'failed': 0
                }
            
            # 获取附件信息
            if record['attachment_download_info']:
                attachment_info = json.loads(record['attachment_download_info'])
            else:
                attachment_info = json.loads(record['attachment_info'] or '[]')
            
            # 如果没有附件，添加默认的HTML类型
            if not attachment_info:
                attachment_info.append({
                    'url': record['url'],
                    'filename': record['url'],
                    'attachment_type': 'html'
                })
            
            # 标记该ods_id是否解析失败（用于跳过整个ods_id）
            ods_parse_failed = False
            
            # 遍历附件列表
            for idx, attach in enumerate(attachment_info):
                logger.info(f"开始解析附件：{attach}")
                sink_mapping_rule = True
                
                self.ods_parse_status[ods_id]['total'] += 1
                
                try:
                    # # 特殊处理RAR压缩包
                    # if attach['url'].endswith('.rar'):
                    #     for ix, (fn, json_data) in enumerate(self.extract_all_rar_files(self.download(attach['url'], record['url']).content, attach['filename'], record=record)):
                    #         attach['filename'] = fn
                    #         yield from self._parse(json_data=json_data, record=record, idx=ix, attach=attach, sink_mapping_rule=sink_mapping_rule)
                    #     # 记录成功
                    #     self.success_attachments.append({
                    #         'ods_id': ods_id,
                    #         'filename': attach.get('filename', ''),
                    #         'url': attach.get('url', '')
                    #     })
                    #     self.ods_parse_status[ods_id]['success'] += 1
                    #     continue

                    json_data = self.parse_attachment(attach, referer=record['url'], record=record)
                    yield from self._parse(json_data=json_data, record=record, idx=idx, attach=attach, sink_mapping_rule=sink_mapping_rule)
                    
                    # 记录成功
                    self.success_attachments.append({
                        'ods_id': ods_id,
                        'filename': attach.get('filename', ''),
                        'url': attach.get('url', '')
                    })
                    self.ods_parse_status[ods_id]['success'] += 1
                    
                except Exception as ex:
                    # 记录失败
                    error_msg = f"{type(ex).__name__}: {str(ex)}"
                    self.failed_attachments.append({
                        'ods_id': ods_id,
                        'filename': attach.get('filename', ''),
                        'url': attach.get('url', ''),
                        'error': error_msg
                    })
                    self.ods_parse_status[ods_id]['failed'] += 1
                    
                    logger.error(
                        f"附件解析失败：ods_id: {ods_id}, "
                        f"filename: {attach.get('filename')}, "
                        f"url: {attach.get('url')}\n"
                        f"错误信息: {error_msg}\n"
                        f"{traceback.format_exc()}"
                    )
                    
                    # 标记该ods_id解析失败，跳出附件循环
                    ods_parse_failed = True
                    continue
            
            # 如果该ods_id解析失败，记录日志
            if ods_parse_failed:
                logger.warning(f"ods_id {ods_id} 存在解析失败的附件，建议检查失败的附件列表以获取详细信息")
                continue

    def _parse(self, json_data: list[dict], record: dict, idx: int, attach: dict, sink_mapping_rule: bool):
        """
        核心解析逻辑：将JSON数据转换为mapping_rule和raw_data
        
        处理步骤：
            1. 解析mapping_rule（批次、年份等信息）
            2. 解析有效期日期
            3. 遍历每条数据记录
            4. 生成list_id（批次分组）
            5. 关联企业digest
            6. 生成最终数据记录
        
        Args:
            json_data (list[dict]): 解析后的JSON数据
            record (dict): ODS原始记录
            idx (int): 附件索引
            attach (dict): 附件信息
            sink_mapping_rule (bool): 是否需要生成mapping_rule
        
        Yields:
            dict: mapping_rule或raw_data记录
        """
        try:
            # 解析映射规则（批次、年份、企业名称字段等）
            mapping_rule, serial_no = self.parse_mapping_rule(
                record=record, 
                row_keys=json_data[0]['row'].keys()
            )
            
            batch_date = None
            batch_no = -1
            batch_name = None
            first = True
            list_id = f"{record['id']}_{idx}"
            record_count = 0
            
            # 解析发布日期和有效期
            start_date, end_date, issue_date = self.parse_publish_date(record)

            # 如果是复核名单，时间取 发文年份
            if '复核' in attach.get('filename', ''):
                mapping_rule['year'] = str(self.extract_year(record['publish_time']))

            # 如果是复核通过名单，标记is_review
            if '复核通过' in attach.get('filename', ''):
                self.is_review = 1

            # 如果是撤销公告，status置为2 ， start_date=end_date=成文日期
            if '撤销' in attach.get('filename', ''):
                self.status = 2
                if issue_date: # 如果有成文日期
                    start_date = end_date = issue_date
                else: # 否则取发布日期
                    start_date = end_date = issue_date = record['publish_time']

            # 跳过更名类附件
            if '更名' in attach.get('filename', ''):
                logger.info(f"跳过附件名：{attach.get('filename')}")
                return None
            
            # 遍历数据记录
            for item in json_data:
                # 提取日期信息
                if 'start_date' in item['row']:
                    start_date = item['row'].pop('start_date')
                if 'end_date' in item['row']:
                    end_date = item['row'].pop('end_date')
                _batch_name = item['row'].pop('批次') if '批次' in item['row'] else None
            
                # Step 1: 生成映射表
                if (
                    (serial_no and serial_no in item['row'] and ((start_date != batch_date and item['row'][serial_no] == '1') 
                                    or item['row'][serial_no] == '1'))
                    or (start_date != batch_date and not serial_no) 
                    or batch_name != _batch_name
                ) or first:
                    batch_date = start_date
                    batch_name = _batch_name
                    batch_no += 1
                    list_id = f"{record['id']}_{batch_no + idx}"
                    
                    yield InnovativeSMEMappingRuleModel(**mapping_rule | {
                        'list_id': list_id,
                        'batch': batch_name if batch_name else mapping_rule.get('batch', None), # 首先从单个表格中取批次；若单个表格中没有，则取mapping_rule中的批次
                        'issue_time': issue_date,
                        'start_date': start_date,
                        'end_date': end_date,
                        'status': self.status if self.status is not None else int(datetime.now().date() > end_date),
                        'notice_type': 0 if re.search(r'公示|拟', record['detail_title']) else self.notice_type,
                        'attachment_info': attach if attach.get('attachment_type') != 'html' else None,
                    }).model_dump() | {'table_name': self.mapping_table}
                    
                    sink_mapping_rule = True
                    first = False

                # 关联企业digest（用于企业匹配）
                digest, province, credit_no = get_digest_by_rule_one(
                    company_name=item['row'][mapping_rule['company_name']],
                    event_date=record['publish_time']
                )

                # 如果是复核名单，根据company_name_digest查询该企业上一条入库记录，将其end_date置为复核通过日期的前一天
                # 这里的数据库根据配置文件选择
                if self.is_review == 1:
                    last_record = self.get_last_record_by_digest(digest, database=DWD_DB_CONFIG)
                    if last_record:
                        last_record['end_date'] = (record['publish_time'] - timedelta(days=1)).date()
                        self.update_record(last_record, database=DWD_DB_CONFIG)

                # 清理空值
                item['row'].pop(None, None)
                if not item['row'].get(mapping_rule['company_name'], None):
                    continue

                # Step 2: 生成raw_data表
                company_name = item['row'].pop(mapping_rule['company_name'], None)
                
                yield InnovativeSMERawDataModel(**{
                    'ods_id': record['id'],
                    'list_id': list_id,
                    'serial_no': item['row'].pop(serial_no, None) if serial_no and serial_no in item['row'] else None,
                    'company_name': n_company_name(company_name) if company_name else None,
                    'ext_json': item['row'] or None,
                    'company_name_digest': digest,
                    'remark': None,
                    'company_id': hashlib.md5(digest.encode('UTF-8')).hexdigest() if digest else None,
                }).model_dump() | {'table_name': self.raw_table}

                record_count += 1
                
        except Exception as ex:
            # 记录解析错误并抛出异常（让上层 parse() 方法处理）
            error_msg = f"{type(ex).__name__}: {str(ex)}"
            logger.error(
                f"_parse 解析失败：ods_id: {record['id']}, URL: {attach}\n"
                f"\t|- Exception: {error_msg}\n\t\t|- {traceback.format_exc()}"
            )
            # 重新抛出异常，让 parse() 方法的 except 块捕获
            raise

    # ==================== 规则解析模块 ====================
    
    def parse_mapping_rule(self, record: dict, row_keys: list) -> Tuple[dict, str]:
        """
        解析映射规则：提取批次、年份、企业名称字段等信息
        
        提取策略：
            - 批次：从detail_title或title中正则匹配"第X批"
            - 年份：从detail_title或title中正则匹配"XXXX年"，失败则从publish_time提取
            - 企业名称字段：从row_keys中匹配包含"企业名称"、"公司名称"等关键词的字段
            - 产品信息字段：匹配包含"产品信息"、"产品名称"等关键词的字段
            - 序号字段：匹配包含"序号"的字段
        
        Args:
            record (dict): ODS原始记录
            row_keys (list): 数据行的字段列表
        
        Returns:
            tuple: (mapping_rule字典, serial_no字段名)
        """
        # 提取省份信息
        rating_scope = None
        area_info = get_location(record["publish_unit"]) if record["publish_unit"] else None
        if not area_info:
            area_info = get_location(record["detail_title"]) if record["detail_title"] else None
        if area_info and "province" in area_info:
            rating_scope = area_info["province"]
        
        # 提取批次信息
        batch = re.match(r'.*?(第.+?批).*?', record['detail_title'])
        if not batch:
            batch = re.match(r'.*?(第.+?批).*?', record['title'])
        
        # 提取年份信息
        year = re.match(r'.*?(\d{4})年.*?', record['detail_title'])
        if not year:
            year = re.match(r'.*?(\d{4})年.*?', record['title'])
        
        # 初始化字段名
        company_name, serial_no = None, None
        
        # 遍历字段名，匹配关键字段
        for key in row_keys:
            # 匹配企业名称字段
            if not company_name and key and (
                operator.contains(key, '企业名称') or operator.contains(key, '名单') or
                operator.contains(key, '企业全称') or operator.contains(key, '单位名称') or
                operator.contains(key, '公司名称') or operator.contains(key, '项目单位') or
                operator.contains(key, '集群名称')
            ):
                company_name = key
            
            # 匹配序号字段
            if not serial_no and key and operator.contains(key, '序号'):
                serial_no = key

       
        # 构建mapping_rule字典
        return {
            'ods_id': record['id'],
            'honor_name': '创新型中小企业',
            'rating_scope': rating_scope if rating_scope else None,
            'company_name': company_name,
            'level': 1,  # 1=省级
            'batch': batch and batch.group(1) or None,
            'year': year.group(1) if year else str(self.extract_year(record['publish_time'])),
            'title': record['detail_title'],
            'publish_unit': record['publish_unit'],
            'publish_date': record['publish_time'],
            'detail_url': record['url'],
            'verification_status': 0,
        }, serial_no

    def parse_publish_date(
        self, 
        record: dict
    ) -> Tuple[Optional[datetime], Optional[datetime], Optional[datetime]]:
        """
        解析发布日期和有效期
        
        解析策略：
            1. start_date: 默认使用publish_time，优先从body文本中正则匹配
            2. end_date: 默认为start_date + 3年 - 1天，优先从body文本中正则匹配
            3. issue_date: 从body文本中匹配发文日期（落款日期）
        
        正则匹配规则：
            - 有效期格式1: "有效期为XXXX年X月X日至XXXX年X月X日"
            - 有效期格式2: "有效期三年...为XXXX年X月X日-XXXX年X月X日"
            - 发文日期: "发布单位...XXXX年X月X日"
        
        Args:
            record (dict): ODS原始记录
        
        Returns:
            tuple: (start_date, end_date, issue_date)
        """
        # 默认start_date为发布时间
        start_date = record['publish_time'].date() if isinstance(
            record['publish_time'], datetime
        ) else record['publish_time']
        
        # 默认end_date为3年后
        end_date = start_date + relativedelta(years=3) - timedelta(days=1) if start_date else None
        issue_date = None
        
        try:
            # 标准化body文本（去除空白字符）
            text = unicodedata.normalize(
                'NFKC', 
                re.sub('[\s+\xa0]', '', ''.join(etree.HTML(record['body']).xpath('//text()')))
            )
            
            # 提取发文日期（落款日期）
            try:
                if res := re.search(
                    record["publish_unit"] + r'.*?(\d{4}年\d{1,2}月\d{1,2}日).*?', 
                    text, 
                    18
                ):
                    issue_date = datetime.strptime(res.group(1), '%Y年%m月%d日').date()
            except Exception as ex:
                logger.warning(f"解析落款日期失败：{ex}")

            # 提取有效期（格式1）
            res = re.search(
                r'.*?(?:创新型中小企业)?有效期为(\d{4}年\d{1,2}月\d{1,2}日)至(\d{4}年\d{1,2}月\d{1,2}日).*?', 
                text, 
                18
            )
            
            # 提取有效期（格式2）
            if not res:
                res = re.search(
                    r'.*?有效期三年.*?为(\d{4}年\d{1,2}月\d{1,2}日)-(\d{4}年\d{1,2}月\d{1,2}日).*?', 
                    text, 
                    18
                )
            
            # 解析有效期日期
            if res:
                start_date = datetime.strptime(res.group(1), '%Y年%m月%d日').date()
                end_date = datetime.strptime(res.group(2), '%Y年%m月%d日').date()
                
        except Exception as ex:
            logger.warning(f"解析有效期时间失败：{ex}")
        
        return start_date, end_date, issue_date

    # ==================== 获取解析结果 ====================
    def get_json_data(self, output):
        output = json.loads(output) if output else None
        json_data = []
        if output and len(output) > 0 and isinstance(output[0], dict) and 'companies' in output[0]:
            companies = output[0]['companies']
            for company in companies:
                json_data.append({'row': company})
        print(f"json_data解析结果：{json_data}")
        return json_data

    # ==================== RAR解压模块 ====================
    # def extract_all_rar_files(self, rar_object: bytes, attach: dict, **kwargs):
    #     """
    #     递归解压RAR文件中的所有文件
        
    #     支持的文件类型：
    #         - Excel (xls, xlsx, et)
    #         - PDF
    #         - Word (doc, docx, wps)
        
    #     Args:
    #         rar_object (bytes): RAR文件的二进制数据
    #         attach (dict): 附件信息
    #         **kwargs: 其他参数（如record等）
        
    #     Yields:
    #         tuple: (filename, parsed_data) - 文件名和解析后的数据
        
    #     Raises:
    #         Exception: 不支持的文件格式或目录结构
    #     """
    #     with rarfile.RarFile(BytesIO(rar_object)) as rf:
    #         for member in rf.infolist():
    #             if not member.isdir():
    #                 filename = member.filename
    #                 doc_type = os.path.splitext(filename)[-1].strip('.').lower()
                    
    #                 # 从文件名提取类型
    #                 if not doc_type:
    #                     if suffix := re.match(
    #                         r'.*?\.(xls|xlsx|docx|doc|pdf|wps|ed|et|html|htm|shtml).*?', 
    #                         filename, 
    #                         18
    #                     ):
    #                         doc_type = suffix.group(1).lower()

    #                 # 处理Excel文件
    #                 if doc_type in ('xls', 'xlsx', 'excel', 'et'):
    #                     response = requests.Response()
    #                     response._content = rf.open(member.filename).read()
    #                     tab_list = self.parse_excel(
    #                         response=response, 
    #                         attach=attach, 
    #                         record=kwargs['record']
    #                     )
    #                     list(map(lambda x: x['row'].update({"附件名": filename}), tab_list))
    #                     yield filename, tab_list
                    
    #                 # 处理PDF文件
    #                 elif doc_type == 'pdf':
    #                     tab_list = PDFParser(
    #                         file_path=rf.open(member.filename).read(),
    #                         line_scale=40,
    #                         is_page_probability=0.2,
    #                         is_title_probability=0.7,
    #                         header_match_threshold=0.5,
    #                         sub_title_match_threshold=0.7,
    #                     ).get_tables_as_json()
    #                     list(map(lambda x: x['row'].update({"附件名": filename}), tab_list))
    #                     yield filename, tab_list
                    
    #                 # 处理Word文件
    #                 elif doc_type in ('doc', 'docx', 'wps', 'word'):
    #                     tab_list = DOCParser(
    #                         column_parser={"序号": "序号", "企业名称": "企业名称"},
    #                         target_dir='./',
    #                         logger=logger,
    #                     ).parse(
    #                         file_path=rf.open(member.filename).read(),
    #                         file_name=hashlib.md5(
    #                             os.path.basename(
    #                                 attach.get('store_path', attach.get('url'))
    #                             ).encode()
    #                         ).hexdigest(),
    #                     )
    #                     list(map(lambda x: x['row'].update({"附件名": filename}), tab_list))
    #                     yield filename, tab_list
    #             else:
    #                 raise Exception(f"不支持的附件格式：{attach.get('store_path', attach.get('url'))}")

    #             # 递归处理嵌套的RAR文件
    #             if member.filename.lower().endswith('.rar'):
    #                 self.extract_all_rar_files(
    #                     rf.open(member.filename).read(), 
    #                     attach, 
    #                     **kwargs
    #                 )

    # ==================== 工具方法模块 ====================
    
    def get_last_record_by_digest(self, digest: str, database) -> Optional[dict]:
        """
        根据企业digest查询该企业的最新一条有效记录
        
        查询条件：
            - company_name_digest = digest
            - use_flag = 0 (有效记录)
            - 按create_time降序排序，取第一条
        
        Args:
            digest (str): 企业名称的digest值
        
        Returns:
            dict: 最新记录，未找到返回None
        """
        if not digest:
            logger.warning("digest为空，无法查询记录")
            return None
        
        try:
            conn = self.get_connection(database)
            if not conn:
                logger.error("数据库连接失败")
                return None
            
            with conn.cursor(DictCursor) as cursor:
                # 查询该企业最新的一条有效记录
                sql = f"""
                    SELECT * FROM {self.final_table}
                    WHERE company_name_digest = %s 
                    AND use_flag = 0
                    ORDER BY year DESC
                    LIMIT 1
                """
                cursor.execute(sql, (digest,))
                record = cursor.fetchone()
                
            conn.close()
            
            if record:
                logger.info(f"复核名单：找到企业[{record.get('company_name')}]的最新记录，id={record.get('id')}")
            else:
                logger.info(f"复核名单：未找到digest={digest}的记录")

            return record
            
        except Exception as e:
            logger.error(f"复核名单：查询最新记录失败: {e}\n{traceback.format_exc()}")
            return None

    def update_record(self, record: dict, database) -> bool:
        """
        更新数据库记录的end_date字段
        
        Args:
            record (dict): 包含id和end_date的记录字典
        
        Returns:
            bool: 更新成功返回True，失败返回False
        """
        if not record:
            logger.warning("复核名单：未查找到记录，无法更新")
            return False
        
        try:
            conn = self.get_connection(database)
            if not conn:
                logger.error("数据库连接失败")
                return False
            
            with conn.cursor() as cursor:
                # 更新end_date字段
                sql = f"""
                    UPDATE {self.final_table}
                    SET end_date = %s, update_time = %s
                    WHERE id = %s
                """
                cursor.execute(sql, (
                    record.get('end_date'),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    record.get('id')
                ))
                conn.commit()
                
                logger.info(
                    f"更新记录成功: id={record.get('id')}, "
                    f"company_name={record.get('company_name')}, "
                    f"new_end_date={record.get('end_date')}"
                )
                
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"更新记录失败: {e}\n{traceback.format_exc()}")
            if conn:
                conn.rollback()
                conn.close()
            return False

    def get_connection(self, config):
        """
        通用数据库连接方法
        
        Args:
            config (dict): 数据库配置字典
        
        Returns:
            pymysql.Connection: 数据库连接对象，失败返回None
        """
        try:
            return pymysql.connect(
                host=config['host'],
                port=config['port'],
                user=config['user'],
                password=config['password'],
                database=config['database'],
            )
        except Exception as e:
            print(f"数据库连接失败: {e}")
            return None
    
    def extract_year(self, date_input) -> Optional[int]:
        """
        从datetime格式的数据中提取年份
        
        支持的输入类型：
            - datetime对象
            - 字符串（多种日期格式）
        
        Args:
            date_input: datetime对象、字符串或其他日期格式
        
        Returns:
            int: 年份，提取失败返回None
        """
        if not date_input:
            return None
        
        try:
            # 如果已经是datetime对象
            if isinstance(date_input, datetime):
                return date_input.year
            
            # 如果是字符串，尝试解析
            if isinstance(date_input, str):
                # 尝试常见的日期格式
                date_formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d',
                    '%Y/%m/%d %H:%M:%S',
                    '%Y/%m/%d',
                ]
                for fmt in date_formats:
                    try:
                        dt = datetime.strptime(date_input, fmt)
                        return dt.year
                    except ValueError:
                        continue
            
            return None
        except Exception as e:
            print(f"提取年份失败: {e}")
            return None

    def insert_data(self, data: dict, table_name: str, connection) -> bool:
        """
        插入单条数据到数据库
        
        Args:
            data (dict): 数据字典
            table_name (str): 目标表名
            connection: 数据库连接对象
        
        Returns:
            bool: 插入成功返回True，失败返回False
        """
        if not data or not isinstance(data, dict):
            print("没有有效数据需要插入")
            return False
        
        try:
            with connection.cursor() as cursor:
                # 构建INSERT语句
                columns = list(data.keys())
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                
                # 执行插入
                values = tuple(data[col] for col in columns)
                cursor.execute(insert_sql, values)
                connection.commit()
                return True
                
        except Exception as e:
            print(f"插入数据失败: {e}")
            connection.rollback()
            return False

    def getmd5(self, text):
        return hashlib.md5(text.encode('utf-8')).hexdigest()


# ==================== 主程序入口 ====================

if __name__ == '__main__':

    # ods_id_list = [25716034, 25733184, 25780274, 25803252, 25845477]
    ods_id_list = [27169947]
    
     # 初始化InnovativeSME对象
    sme = InnovativeSME(
        mysql_properties=DWD_DB_CONFIG | {'database': 'dwd_ticl'}, 
        ods_id_list=ods_id_list
    )

    print("\n" + "="*80)
    print("开始解析创新型中小企业数据...")
    print("="*80 + "\n")

# ==================== 阶段1：附件解析信息 ====================
    # results = list(sme.parse())
    
    # print(f"\n{'='*80}")
    # print(f"解析完成，共获取 {len(results)} 条结果")
    # print(f"{'='*80}\n")
    
    # mapping_rule_count = 0
    # raw_data_count = 0
    
    # for idx, result in enumerate(results, 1):
    #     table_name = result.get('table_name')
        
    #     if table_name == 'dwd_honor_innovative_smes_mapping_rule':
    #         mapping_rule_count += 1
    #     elif table_name == 'dwd_honor_innovative_smes_raw_data':
    #         raw_data_count += 1
        
    #     print(f"\n--- 结果 #{idx} ---")
    #     print(f"表名: {table_name}")
        
    #     # 根据表类型打印关键字段
    #     if table_name == 'dwd_honor_innovative_smes_mapping_rule':
    #         print(f"  ├─ list_id: {result.get('list_id')}")
    #         print(f"  ├─ 荣誉名称: {result.get('honor_name')}")
    #         print(f"  ├─ 批次: {result.get('batch')}")
    #         print(f"  ├─ 年份: {result.get('year')}")
    #         print(f"  └─ 标题: {result.get('title')}")
    #         print(f"  └─ 附件信息: {result.get('attachment_info')}")

            
    #     elif table_name == 'dwd_honor_innovative_smes_raw_data':
    #         print(f"  ├─ list_id: {result.get('list_id')}")
    #         print(f"  ├─ 序号: {result.get('serial_no')}")
    #         print(f"  ├─ 企业名称: {result.get('company_name')}")
    #         print(f"  └─ 扩展字段: {result.get('ext_json')}")
    
    # # ==================== 打印统计信息 ====================
    # print(f"\n{'='*80}")
    # print(f"统计信息:")
    # print(f"  - mapping_rule记录: {mapping_rule_count} 条")
    # print(f"  - raw_data记录: {raw_data_count} 条")
    # print(f"{'='*80}\n")
    
# ==================== 阶段2：入mapping_rule和raw_data库 ====================
    print(f"\n{'='*80}")
    print("开始入库...")
    sme.setup()
    print("入库完成！")
    
    # ==================== 打印附件解析统计 ====================
    print(f"\n{'='*80}")
    print("附件解析统计:")
    print(f"  - 成功解析的附件: {len(sme.success_attachments)} 个")
    for attach in sme.success_attachments:
        print(f"    ├─ ods_id: {attach.get('ods_id')}, filename: {attach.get('filename')}, url: {attach.get('url')}")
    print(f"  - 失败解析的附件: {len(sme.failed_attachments)} 个")
    for attach in sme.failed_attachments:
        print(f"    ├─ ods_id: {attach.get('ods_id')}, filename: {attach.get('filename')}, url: {attach.get('url')}, error: {attach.get('error')}")
    print(f"{'='*80}\n")
    print("解析状态详情:")
    for ods_id, status in sme.ods_parse_status.items():
        print(f"  - ods_id: {ods_id}, status: {status}")

# ==================== 阶段3：生成dwd最终数据 ====================
    # print("开始生成dwd最终数据...")
    # sme.process_final_data()
    # print("dwd最终数据生成完成！")