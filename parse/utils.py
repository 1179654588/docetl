import json, os
from datetime import datetime, date
from typing import Optional, Tuple
from urllib.parse import urlparse
from bs4 import BeautifulSoup as bs

import copy
import mysql.connector
from retrying import retry
import re
import queue
from typing import Iterable

PUNCTUATION_PATTERN = re.compile(
    r"(^[\s\t\r\n＂＃＄％＆＇｀〃〝〞‘’‛“”„‟﹑\"#$%&'_`@＠＊＋，－／：；＜＝＞＼＾＿｛｜｝～〜〟〰〾〿–—…‧﹏﹔·！？｡。￥!()*+,-./:;<=>?{|}~]+$)")
N_STATUS = ['正常', '吊销', '注销', '撤销', '迁出', '迁入', '清算', '停业', '已终止营业地点', '休止活动', '其他',
            '其它', 'null', None]
ELEMENT_SCORE = ["province_short", "legal_person", "establish_date", "company_major_type"]

# 工商实例
GSXT_DB = {
    #"host": "gsxt.mysql.ob.pinganhub.com",
    "host": "10.9.16.100",
    "port": 3306,
    "user": "bdp_gsxt_t_ro",
    "password": ")$5(TS!r-6E",
    "database": "bdp_ic_gsxt",
    "connection_timeout": 90
}

def translate_web_code(code):
    """HTML实体解析，注意：HTML实体名称对大小写敏感"""
    res = bs(code, "html5lib")
    return res.text

f_str_map = {"(": "（",
             ")": "）",
             "・": "·",
             "•": "·",
             "—": "-",
             ":": "：",
             "－": "-",
             "": "",  # 不是空格，是一个方框
             # "&nbsp;": "",
             # "&#8226;": "·",
             # "&mdash;": "-",
             "&NBSP;": "",
             "&MDASH;": "-"}


def DBC2SBC(ustring):
    """全角转半角"""
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 0x3000:
            inside_code = 0x0020
        else:
            inside_code -= 0xfee0
        if not (0x0021 <= inside_code <= 0x7e):
            rstring += uchar
            continue
        rstring += chr(inside_code)
    return rstring


def strip_special_char(name):
    """去除特殊字符与多余的空格"""
    name = name.strip(" 。“*”`+#·＊?/&;、-!@※%^◎×<=△$").lstrip(".")
    if re.match("^[\\u4e00-\\u9fa5\\s]+(\\.|[(（]|[(（][?？][)）])$", name, re.I):
        sub_str = re.match("^[\\u4e00-\\u9fa5\\s]+(\\.|[(（]|[(（][?？][)）])$", name, re.I).group(1)
        name = name.replace(sub_str, '')
    name = re.sub("\\s", " ", name)  # 统一其它类型的空格
    name = re.sub("(?<![a-zA-Z]) *(?<![a-zA-Z])", "", name)
    return name


def strip_content_of_english_in_the_end(name):
    """严格要求名称模式为：中文+左括号+英文或空格+右括号"""
    if re.search("[\\u4e00-\\u9fa5\\s]+[(（][a-zA-Z\\s.,·]+[）)]$", name):
        name_cleaned = re.sub("[(（][a-zA-Z\\s.,·]+[）)]$", "", name)
        if name_cleaned:
            return name_cleaned
    return name

def n_company_name(name):
    """
    统一处理企业名称中的特殊字符
    1：半角小括号转全角小括号
    2：全角数字字母转半角数据字母
    3：去除中文名称中的空格
    4：所有字母转大写
    5：统一名称中的点号
    警告：修改统一处理规则前要慎重，不要捡了芝麻丢了西瓜
    :param name:
    :return: name cleaned
    >>> n_company_name("ＴＣＬ空调器　(武汉)・·・．.有限公司＃＃##＃＃")
    'TCL空调器（武汉）·····有限公司'
    >>> n_company_name("戴 珊")
    '戴珊'
    >>> n_company_name("Alibaba.Com China Limited")
    'ALIBABA·COM CHINA LIMITED'
    >>> n_company_name("一汽-大众汽车有限公司")
    '一汽-大众汽车有限公司'
    >>> n_company_name("一汽—大众汽车有限公司")
    '一汽-大众汽车有限公司'
    >>> n_company_name(";赵申玲")
    '赵申玲'
    >>> n_company_name("郑武涛　曹树梅")
    '郑武涛曹树梅'
    >>> n_company_name("&nbsp;荆林全")
    '荆林全'
    >>> n_company_name("&#38064;海健康产业发展集团有限公司")
    '钰海健康产业发展集团有限公司'
    >>> n_company_name("&#23723;&#23721;&#20559;&#23725;")
    '岫岩偏岭'
    >>> n_company_name("х╝ачее")
    'Х╝АЧЕЕ'
    >>> n_company_name("李＆＃１７９７３；彪")
    '李䘵彪'
    >>> n_company_name("山东乾有实业有限公司（委派代表: 贾宁宁）")
    '山东乾有实业有限公司（委派代表：贾宁宁）'
    >>> n_company_name("刘冬梅、王顺华、")
    '刘冬梅、王顺华'
    """
    if not name:
        return name
    if name.encode("UTF-8").isalpha():  # 全为英文字母的保留原样，如：YongCang
        return name
    if not isinstance(name, str):
        return name
    name = translate_web_code(name)  # HTML实体区分大小写
    name = DBC2SBC(name)
    for k, v in f_str_map.items():
        name = name.replace(k, v)
    name = strip_special_char(name)
    name = name.upper()
    if "&#" in name:  # 出现全角的html实体
        name = translate_web_code(name)
    if name.endswith(")") or name.endswith("）"):
        name = strip_content_of_english_in_the_end(name)
    return name

def str2datetime(string):
    if isinstance(string, (datetime.datetime, datetime.date)):
        return string
    if not (isinstance(string, str) or isinstance(string, bytes)):
        return None
    REX_DT = re.compile(r"([129]\d{3})[-/年\.]{0,1}(\d{1,2})[-/月\.]{0,1}(\d{1,2})[日]{0,1}([T ]{0,1}(\d{2}):{0,1}(\d{2}):{0,1}(\d{2}))*.*")
    m = REX_DT.match(string)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        if m.group(4):
            hour = int(m.group(5))
            minute = int(m.group(6))
            second = int(m.group(7))
        else:
            hour = 0
            minute = 0
            second = 0
        try:
            return datetime.datetime(year, month, day, hour, minute, second)
        except:
            return None
    else:
        try:
            return datetime.datetime.strptime(string, '%a %b %d %H:%M:%S CST %Y')
        except:
            return None


def transform_date(value):
    import datetime
    if not value:
        return
    if isinstance(value, (datetime.date, datetime.datetime)):
        return datetime.date(value.year, value.month, value.day)
    value = str2datetime(value)
    return value.date() if value else None


class ParameterError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class NotFound(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class SearchModel:
    fields = {
        "company_name",
        "n_company_name",
        "company_code",
        "credit_no",
        'establish_date',
        'legal_person',
        'capital',
        'company_status',
        'company_type',
        'operation_startdate',
        'operation_enddate',
        'issue_date',
        'authority',
        "company_address",
        'business_scope',
        'n_company_status',
        'company_major_type',
        'company_minor_type',
        'cancel_date',
        'cancel_reason',
        'revoke_date',
        'revoke_reason',
        'province',
        'province_short',
        'area_code',
        'city',
        'city_code',
        'district',
        'district_code',
        'baseinfo_update_time',
        "company_name_digest",
        'org_code',
        'use_flag',
    }

    def __init__(self):
        """
        初始化数据库连接
        """
        self.conf = GSXT_DB
        self.db = mysql.connector.connect(**self.conf)
        self.cursor = self.db.cursor(dictionary=True)
        self.info_sql = "select {} from tb_company_base".format(",".join(self.fields))

    @retry(stop_max_attempt_number=3)
    def _exec_sql(self, sql, params):
        """
        执行sql查询数据
        :param sql:
        :param params:
        :return:
        """
        try:
            self.cursor.execute(sql, params)
            return self.cursor.fetchall()
        except:
            self.db.close()
            self.db = mysql.connector.connect(**self.conf)
            self.cursor = self.db.cursor(dictionary=True)
            raise

    def _get_history_name(self, digest):
        return self._exec_sql(
            "select history_name,start_date,end_date from bdp_ent.dwd_ent_history_name where use_flag = 0 and company_name_digest = %(digest)s",
            {"digest": digest}
        )

    def _search_valid_data(self, company_info: dict, del_digests: list = []) -> dict:
        """
        获取use_flag=0的数据
        如果company_info的use_flag是0，则直接返回，否则通过dwd_ent_del_digest_map递归查询有效的数据
        :param company_info:
        :param del_digests:
        :return:
        """
        if company_info.get("use_flag") != 0:
            if company_info.get("company_name_digest") not in del_digests:
                new_company_name_digest = self._exec_sql(
                    "select company_name_digest from dwd_ent_del_digest_map where del_company_name_digest = %(company_name_digest)s",
                    {"company_name_digest": company_info.get("company_name_digest")})
                if new_company_name_digest:
                    del_digests.append(company_info.get("company_name_digest"))

                    new_company_info = self._exec_sql(
                        self.info_sql + " where company_name_digest = %(company_name_digest)s",
                        {"company_name_digest": new_company_name_digest[0].get("company_name_digest")})
                    if new_company_info:
                        return self._search_valid_data(new_company_info[0], del_digests)

            raise NotFound("digest已被废弃")

        return company_info

    def _filter_by_other_info(self, matched_data, **kwargs):
        """
        通过其他辅助信息过滤数据
        :param matched_data: 命中的数据
        :param kwargs: 辅助字段
            * establish_date
            * legal_person
            * company_major_type
            * province_short
        :return:
        """
        delete_index = []
        for i in range(len(matched_data)):
            data = matched_data[i]
            for element in ELEMENT_SCORE:
                if kwargs.get(element):
                    if isinstance(kwargs.get(element), list) and data.get(element) \
                            and data.get(element) not in kwargs.get(element):
                        delete_index.append(i)
                    elif data.get(element) and data.get(element) != kwargs.get(element):
                        delete_index.append(i)

        return [matched_data[i] for i in range(len(matched_data)) if i not in delete_index]

    def _filter_by_event_date(self, matched_data, event_date, use_other_date=False):
        """
        通过事件日期过滤数据
        :param matched_data: 命中的数据
        :param event_date: 事件日期
        :param use_other_date: 是否使用其他时间过滤，暂时支持吊、注销时间
        """
        if event_date:
            delete_idx = []
            for i in range(len(matched_data)):
                row = matched_data[i]
                if "start_date" in row:  # 有start_date这个字段代表是历史名称匹配，此时不能用establish_date判断
                    if (row.get("start_date") and row.get("start_date") > event_date) \
                            or (row.get("end_date") and row.get("end_date") < event_date):
                        delete_idx.append(i)
                    # 非正常企业，有吊注销时间，那事件应发生在主体吊注销之前
                    elif use_other_date and row.get("n_company_status") != "正常" \
                            and (row.get("cancel_date") or row.get('revoke_date')):
                        if (row.get("cancel_date") or row.get('revoke_date')) < event_date:
                            delete_idx.append(i)
                else:
                    if row.get("establish_date") and event_date < row.get("establish_date"):
                        delete_idx.append(i)
                    # 非正常企业，有吊注销时间，那事件应发生在主体吊注销之前
                    elif use_other_date and row.get("n_company_status") != "正常" \
                            and (row.get("cancel_date") or row.get('revoke_date')):
                        if (row.get("cancel_date") or row.get('revoke_date')) < event_date:
                            delete_idx.append(i)

            return [matched_data[j] for j in range(len(matched_data)) if j not in delete_idx]

        return matched_data

    def _filter_by_status(self, matched_data):
        if len(matched_data) == 1:
            return matched_data
        else:
            normal_data = [d for d in matched_data if d.get("n_company_status") == "正常"]
            return normal_data if normal_data else matched_data

    def _filter_by_mainland(self, matched_data):
        if len(matched_data) == 1:
            return matched_data
        else:
            normal_data = [d for d in matched_data if d.get("company_major_type")
                           and int(d.get("company_major_type")) < 10]
            return normal_data if normal_data else matched_data

    def filter_result(self, matched_data, **kwargs):
        """
        过滤匹配数据
        :param matched_data:
        :return:
        """
        if len(matched_data) == 1:
            return [self._search_valid_data(matched_data[0])]

        matched_data = self._filter_by_event_date(matched_data, kwargs.get("event_date"), kwargs.get("use_other_date"))
        matched_data = self._filter_by_other_info(matched_data, **kwargs)
        if kwargs.get("prefer_mainland"):
            matched_data = self._filter_by_mainland(matched_data)
        if kwargs.get("prefer_normal"):
            matched_data = self._filter_by_status(matched_data)

        if not matched_data:
            raise NotFound("查询不到digest")
        elif len(matched_data) == 1:
            return [self._search_valid_data(matched_data[0])]
        else:
            valid_result = list(filter(lambda row: row.get("use_flag") == 0, matched_data))
            if not valid_result:
                raise NotFound("多条数据均已废弃")
            else:
                return valid_result

    def search_by_company_id(self, company_id, **kwargs):
        """
        根据company_id查询company_name_digest
        :param company_id:
        :return:
        """
        result = self._exec_sql("select company_name_digest from tb_unique_key_map where company_id = %(company_id)s",
                                {"company_id": company_id})
        if result:
            company_info = self._exec_sql(self.info_sql + " where company_name_digest = %(company_name_digest)s",
                                          {"company_name_digest": result[0].get("company_name_digest")})
            return company_info
        return []

    def search_by_credit_no(self, credit_no, **kwargs):
        """
        根据credit_no查询company_name_digest
        :param credit_no:
        :return:
        """
        return self._exec_sql(self.info_sql + " where credit_no = %(credit_no)s", {"credit_no": credit_no})

    def search_by_company_code(self, company_code, **kwargs):
        """
        根据company_code查询company_name_digest，company_code长度小于9位的数据要使用名称过滤
        :param company_code:
        :return:
        """
        result = self._exec_sql(self.info_sql + " where company_code = %(company_code)s",
                                {"company_code": company_code})
        if kwargs.get("only_one") and result and len(company_code) < 9 and kwargs.get("n_company_name"):
            new_result = []
            for row in result:
                if row.get("n_company_name") == kwargs.get("n_company_name"):
                    new_result.append(row)
                else:
                    history_name = self._get_history_name(row.get("company_name_digest"))
                    for his_name in history_name:
                        if his_name == kwargs.get("n_company_name"):
                            new_result.append(row)
            return new_result
        return result

    def search_by_company_name(self, params, **kwargs):
        """
        根据company_name查询company_name_digest
        :param params: 过滤参数
        :return:
        """
        condition = ["{0}=%({0})s".format(k) for k in params]
        sql = self.info_sql + " where " + " and ".join(condition)
        current_data = self._exec_sql(sql, params)
        history_params = {"history_name": params["n_company_name"]}
        if "establish_date" in params:
            history_params["establish_date"] = params["establish_date"]

        history_data = self.search_by_history_name(history_params, **kwargs)

        return current_data + history_data

    def search_by_history_name(self, params, **kwargs):
        """
        通过历史名称查询
        :param params:
        :return:
        """
        condition = ["{0}=%({0})s".format(k) for k in params]
        sql = """select {0} 
            from tb_company_base a
            join bdp_ent.dwd_ent_history_name b on a.company_name_digest = b.company_name_digest
             and b.use_flag = 0
            where b.{1}""".format(
            "a." + ",a.".join(self.fields) + ",b.start_date,b.end_date,b.history_name",
            " and ".join(condition)
        )
        return self._exec_sql(sql, params)

    def search(self,
               only_one,
               prefer_normal,
               prefer_mainland,
               prefer_current,
               **kwargs):
        """
        根据条件查询company_name_digest/主体基本信息
        :param only_one: 是否只返回准确的一条
        :param prefer_normal: 过滤完有多条数据时，是否优先选择正常数据，该过滤执行在优选大陆之后
        :param prefer_mainland: 过滤完有多条数据时，是否优先选择大陆企业
        :param prefer_current: 过滤完有多条数据时，是否优先选择当前名称
        :param kwargs: 参数字典，可以接受以下key
            * company_id：对外输出表的主体唯一键
            * company_name：主体名称
            * credit_no：统一信用代码
            * company_code：注册号
            * legal_person：法人名称，辅助判断字段，不通过该字段查询digest
            * establish_date：成立时间，辅助判断字段，不通过该字段查询digest
            * province_short：省份缩写，需要是标准的省份搜鞋，辅助判断字段，不通过该字段查询digest
            * company_major_type：主体类型，辅助判断字段，不通过该字段查询digest，可以是数字字符串或可迭代对象
            * event_date：发生某件事件的日期，辅助判断字段，不通过该字段查询digest
            * continue_match：优先级最高的参数未命中数据时，是否有用次优先级的参数继续匹配，默认True
            * use_other_date：是否使用其他时间配合事件时间过滤，暂时支持吊、注销时间
        :return:
        """
        if not kwargs:
            raise ParameterError("The kwargs cannot be empty！")

        company_id = kwargs.get('company_id')
        company_name = n_company_name(kwargs.get('company_name'))
        credit_no = kwargs.get('credit_no')
        company_code = kwargs.get('company_code')
        establish_date = transform_date(kwargs.get('establish_date'))
        company_major_type = kwargs.get('company_major_type')
        if isinstance(company_major_type, (str, int)):
            company_major_type = [int(company_major_type)]
        elif isinstance(company_major_type, Iterable):
            company_major_type = [int(i) for i in company_major_type]
        else:
            company_major_type = None
        event_date = transform_date(kwargs.get('event_date'))
        continue_match = kwargs.get('continue_match', True)

        if not any([company_id, company_name, credit_no, company_code]):
            raise ParameterError("The parameter is incorrect, necessary fields are missing!")

        priority_pq = queue.Queue()
        if company_id and len(company_id) == 32 and company_id.isalnum():
            priority_pq.put(('company_id', company_id))

        if credit_no and len(credit_no) == 18 and credit_no.isalnum():  # 长度是18位，且只包含数字字母
            priority_pq.put(('credit_no', credit_no))

        if company_code and len(company_code) >= 5 and not PUNCTUATION_PATTERN.match(company_code):  # 长度大于等于5
            priority_pq.put(('company_code', company_code))

        if company_name and len(company_name) > 3:  # search_name的长度不能小于等于3
            _ = {"n_company_name": company_name}
            if establish_date:
                _["establish_date"] = establish_date.strftime("%Y-%m-%d")
            priority_pq.put(('company_name', _))

        other_params = {"n_company_name": n_company_name, "only_one": only_one, "prefer_normal": prefer_normal,
                        "prefer_mainland": prefer_mainland}
        if kwargs.get("province_short"):
            other_params["province_short"] = kwargs.get("province_short")
        if establish_date:
            other_params["establish_date"] = establish_date
        if company_major_type:
            other_params["company_major_type"] = company_major_type
        if event_date:
            other_params["event_date"] = event_date
        if kwargs.get("legal_person"):
            other_params["legal_person"] = kwargs.get("legal_person")
        if kwargs.get("use_other_date"):
            other_params["use_other_date"] = kwargs.get("use_other_date")

        if priority_pq.qsize() == 0:
            raise ParameterError('The parameters are not formatted correctly！')

        while not priority_pq.empty():
            param_k, param_v = priority_pq.get()
            search_func = getattr(self, 'search_by_%s' % param_k)
            try:
                matched = search_func(param_v, **other_params)
                matched = self.filter_result(matched, **other_params)

                if matched:
                    for m in matched:
                        m["history_names"] = self._get_history_name(m.get("company_name_digest"))

                    if only_one:
                        if len(matched) == 1:
                            return matched[0]
                        else:
                            if prefer_current:
                                _ = [m for m in matched if "start_date" not in m]
                                return _[0] if len(_) == 1 else None
                            else:
                                return None
                    else:
                        return matched
            except NotFound:
                if not continue_match:
                    raise


sm = SearchModel()


def get_digest(only_one=True,
               prefer_normal=False,
               prefer_mainland=False,
               prefer_current=False,
               **kwargs):
    """
    根据条件查询company_name_digest/主体基本信息
    :param only_one: 是否只返回准确的一条
    :param prefer_normal: 过滤完有多条数据时，是否优先选择正常数据，默认为否，因为正常不代表一定准确。该条件执行在优选大陆之后
    :param prefer_mainland: 过滤完有多条数据时，是否优先选择大陆企业，默认为否.
    :param prefer_current: 过滤完有多条数据时，是否优先选择当前名称
    :param kwargs: 参数字典，可以接受以下key
        * company_id：对外输出表的主体唯一键
        * n_company_name：主体名称，只接受格式化后的主体名称
        * credit_no：统一信用代码
        * company_code：注册号
        * legal_person：法人名称，辅助判断字段，不通过该字段查询digest
        * establish_date：成立时间，辅助判断字段，不通过该字段查询digest
        * company_major_type：主体类型，辅助判断字段，不通过该字段查询digest
        * event_date：发生某件事件的日期，辅助判断字段，不通过该字段查询digest
        * continue_match：优先级最高的参数未命中数据时，是否有次优先级的参数继续匹配，默认True
    :return:
    """
    try:
        data = sm.search(only_one, prefer_normal, prefer_mainland, prefer_current, **kwargs)
        return {"status_code": 200, "data": data} if data else {"status_code": 404, "reason": "查询不到digest"}
    except (NotFound, ParameterError) as e:
        return {"status_code": 404, "reason": e.message}

def get_digest_by_rule_one(company_name: str, event_date=None, start_date: datetime | date = None) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """按规则：
    1. 当前名称中：工商主体 > 状态正常 > 成立日期
    2. 历史名称中：工商主体 > 状态正常 > 成立日期
    """

    def _filter_by_mainland(matched_data):
        if len(matched_data) == 1:
            return matched_data
        else:
            normal_data = [d for d in matched_data if d.get("company_major_type")
                           and int(d.get("company_major_type")) == 3]
            return normal_data if normal_data else matched_data

    def _filter_by_status(matched_data):
        if len(matched_data) == 1:
            return matched_data
        else:
            normal_data = [d for d in matched_data if d.get("n_company_status") == "正常"]
            return normal_data if normal_data else matched_data

    def _filter_by_establish_date(matched_data):
        if len(matched_data) == 1:
            return matched_data[0]
        else:
            normal_data = list(filter(lambda x: x.get("establish_date"), matched_data))
            if len(normal_data) == 1:
                return sorted(normal_data, key=lambda x: x.get("establish_date"), reverse=True)[0]
            else:
                return {}

    def filter_digest(data: list):
        """工商主体 > 状态正常 > 成立日期"""
        matched_data = _filter_by_mainland(data)
        matched_data = _filter_by_status(matched_data)
        result = _filter_by_establish_date(matched_data)
        return result.get("company_name_digest"), result.get("province"), result.get("credit_no")

    if not company_name:
        return None, None, None
    response = get_digest(only_one=False, **{"company_name": company_name, "event_date": event_date})
    if response['status_code'] == 200:
        response['data'] = list(filter(lambda x: x.get("company_major_type") and int(x.get("company_major_type")) < 10, response['data']))
        if start_date:
            start_date = start_date.date() if isinstance(start_date, datetime) else start_date
            response['data'] = list(filter(lambda x: x.get("establish_date") and x.get("establish_date") < start_date, response['data']))
        if not response['data']:
            return None, None, None
        if len(response['data']) == 1:
            return response['data'][0]['company_name_digest'],response['data'][0]['province'],response['data'][0]['credit_no']

        history = list(filter(lambda x: x.get('start_date'), response['data']))
        current = list(filter(lambda x: not x.get('start_date'), response['data']))
        digest = None

        if current:
            digest = filter_digest(current)

        if not digest and history:
            digest = filter_digest(history)

        return digest
    return None, None, None


# 构建节点
class Node:
    
    def __init__(self):
        self.map = {}
        
    def contain(self, key):
        return self.map.__contains__(key)
    
    def __getitem__(self, key):
        return self.map[key]
    
    def __setitem__(self, key, value):
        self.map[key] = value
   
   
class Item:
    
    def __init__(self, key):
        self.key = key
        self.subNum = 0
        self.subNode = Node()
        self.output = None
        
    def add(self, key, item):
        self.subNum += 1
        self.subNode[key] = item


# 判断字典是否为汉字,英文(针对换行符进行断词)
def parse_dict(input_str):
    if not isinstance(input_str, str):
        input_str = input_str.decode()
    buf = []
    words = []
    for word in input_str:
        if word >= u'\u4e00' and word <= u'\u9fa5':
            words = words+[word]
        elif word >= u'\u0021' and word <= u'\u007e':
            words = words+[word]
        else:
            buf.append(words)
            words = []
    return buf

# 判断读入的是否合规
def parse_input(input_str):
    if not isinstance(input_str, str):
        input_str = input_str.decode()
    buf = []
    for word in input_str:
        if word >= '\u4e00' and word <= '\u9fa5':
            buf = buf+[word]
        elif word >= u'\u0021' and word <= u'\u007e':
            buf = buf+[word]
        else:
            buf = buf+[word]
    return buf

#建树
def build_tree(input_str):
    sock = open(input_str,"r")
    buf = sock.read()
    buf = parse_dict(buf) # 保留有效字符
    tree = Item(" ")
    for words in buf:
        current = tree
        for word in words:
            for x in word:
                if current.subNode.contain(x):
                    current = current.subNode[x]
                else:
                    item = Item(x)
                    current.add(x,item)
                    current = item
        current.output = "".join(words)
    return tree

#建树
def build_tree_from_list(input_list):
    tree = Item(" ")
    for words in input_list:
        current = tree
        for word in words:
            for x in word:
                if current.subNode.contain(x):
                    current = current.subNode[x]
                else:
                    item = Item(x)
                    current.add(x,item)
                    current = item
        current.output = "".join(words)
    return tree

# 判断读入的是否合规
def parse_reverse_input(input_str):
    if not isinstance(input_str, str):
        input_str = input_str.decode()
    # 反转字符串
    input_str = input_str[::-1]
    buf = []
    for word in input_str:
        if word >= u'\u4e00' and word <= u'\u9fa5':
            buf = buf+[word]
        elif word >= u'\u0021' and word <= u'\u007e':
            buf = buf+[word]
        else:
            buf = buf+[word]
    return buf

#建树
def build_reverse_tree(input_str):
    tree = Item(" ")
    for line in open(input_str, encoding='utf-8'):
        line = line.strip()
        lineArray = line.split('\t')
        if len(lineArray) >= 1:
            key = lineArray[0]
            words = parse_reverse_input(key)
            current = tree
            for word in words:
                for x in word:
                    if current.subNode.contain(x):
                        current = current.subNode[x]
                    else:
                        item = Item(x)
                        current.add(x,item)
                        current = item
            current.output = "".join(words)
    return tree

# 查询一次(在单一位置)
def search_tree_by_one_pos(buf,tree):
    havefind = []
    tmpfind = ""
    current = tree
    num = 0
    # 依次送入单字
    while num <len(buf):
        word = buf[num]
        isWord = True
        # 依次送入单字字节
        for x in word:
            # 单字节匹配
            if current.subNode.contain(x):
                current = current.subNode[x]     
            else:
                current = tree
                isWord = False
                break
        if isWord == True:
            if current.subNum == 0:
                # 存入结果队列
                havefind.append(tmpfind + "".join(word))
                return havefind
            # 非叶子节点并且有值
            elif current.output != None:
                # 存入结果队列
                tmpfind = tmpfind + "".join(word)
                havefind.append(tmpfind)
            else:
                tmpfind = tmpfind + "".join(word)
            num = num + 1
        else:
            return havefind
    return havefind

# 查询字典树:输出分词结果
def search_tree_to_list(buf,tree):
    buf = parse_input(buf) # 控制输入字符
    havefind = []
    num = 0
    # 依次送入单字
    while num <len(buf):
        word_list = search_tree_by_one_pos(buf[num:],tree) # 查询一次
        word = ''
        if len(word_list)>0:
            word = word_list[0]
            for item in word_list:
                if len(item) > len(word):
                    word = item
            havefind.append(word)
            num = num + len(word)
        else:
            num = num + 1
    return havefind

# 查询字典树:输出有序特征集
def search_tree_to_set(buf,tree):
    buf = parse_input(buf) # 控制输入字符
    havefind = []
    num = 0
    # 依次送入单字
    while num <len(buf):
        word_list = search_tree_by_one_pos(buf[num:],tree) # 查询一次
        for word in word_list:
            if word not in havefind:
                havefind.append(word)
        num = num + 1
    # 对havefind进行排序
    havefind.sort()
    return havefind

# 查询全部(匹配所有,有详细的区分)
def search_detail(buf,tree):
    buf = parse_input(buf) # 控制输入字符
    havefind = []
    num = 0
    pre_num = 0
    isNotLinked = True
    # 依次送入单字
    while num <len(buf):
        word_list = search_tree_by_one_pos(buf[num:],tree) # 查询一次
        word = ''
        if len(word_list)>0:
            if num != 0 and num == pre_num:
                isNotLinked = False
            word = word_list[0]
            # 取最大长度
            for item in word_list:
                if len(item) > len(word):
                    word = item
            # 判断是否连接
            if isNotLinked == False:
                havefind.append('-')
            # 送入最大长度到结果list
            havefind.append(word)
            # 按照unicode进行移动
            num = num + len(word)
            # 上一次匹配末尾
            pre_num = num
        else:
            num = num + 1
            isNotLinked = True
        
    return havefind

# 建树(带标签的树)
def build_kv_tree(input_str):
    tree = Item(" ")
    for line in open(input_str, encoding='utf-8'):
        line = line.strip()
        lineArray = line.split('\t')
        if len(lineArray) == 2:
            key = lineArray[0]
            value = lineArray[1]
            words = parse_input(key)
            values = parse_input(value)
            current = tree
            for word in words:
                for x in word:
                    if current.subNode.contain(x):
                        current = current.subNode[x]
                    else:
                        item = Item(x)
                        current.add(x,item)
                        current = item
            # 判断是否有值
            if current.output == None:
                current.output = "".join(words) + '\t' + "".join(values)
            else:
                current.output = current.output + ',' + "".join(values)
    return tree

# 建树(带标签的树)
def build_kv_tree_from_dict(input_dict):
    tree = Item(" ")
    for key in input_dict:
        value = input_dict[key]
        words = parse_input(key)
        values = parse_input(value)
        current = tree
        for word in words:
            for x in word:
                if current.subNode.contain(x):
                    current = current.subNode[x]
                else:
                    item = Item(x)
                    current.add(x,item)
                    current = item
        # 判断是否有值
        if current.output == None:
            current.output = "".join(words) + '\t' + "".join(values)
        else:
            current.output = current.output + ',' + "".join(values)
    return tree

# 查询一次(在单一位置)
def search_kv_tree_by_one_pos(buf,tree):
    havefind = []
    current = tree
    num = 0
    # 依次送入单字
    while num <len(buf):
        word = buf[num]
        isWord = True
        # 依次送入单字字节
        for x in word:
            # 单字节匹配
            if current.subNode.contain(x):
                current = current.subNode[x]     
            else:
                current = tree
                isWord = False
                break
        if isWord == True:
            if current.subNum == 0:
                # 存入结果队列
                value_word = current.output
                havefind.append(value_word.replace('\t','|'))
                return havefind
            # 非叶子节点并且有值
            elif current.output != None:
                # 存入结果队列
                value_word = current.output
                havefind.append(value_word.replace('\t','|'))
            num = num + 1
        else:
            return havefind
    return havefind

# 查询全部(匹配所有)
def search_kv_tree(buf,tree):
    buf = parse_input(buf) # 控制输入字符
    havefind = []
    num = 0
    # 依次送入单字
    while num <len(buf):
        word_list = search_kv_tree_by_one_pos(buf[num:],tree) # 查询一次
        final = ''
        final_list = []
        in_num = 0
        for item in word_list:
            if in_num == 0:
                final = item
            else:
                if len(final.split('|')[0]) < len(item.split('|')[0]):
                    final = item
            in_num = in_num + 1
        if final != '':
            final_list.append(final)
            havefind.append(final_list)
            num = num + len(final.split('|')[0])
        else:
            num = num + 1
    # list去重
    havefind_uniq = []
    for tmp in havefind:
        if tmp not in havefind_uniq:
            havefind_uniq.append(tmp)
    return havefind_uniq

# 查询全部(匹配所有)
def search_kv_tree_full(buf,tree):
    buf = parse_input(buf) # 控制输入字符
    havefind = []
    num = 0
    # 依次送入单字
    while num <len(buf):
        word_list = search_kv_tree_by_one_pos(buf[num:],tree) # 查询一次
        final = ''
        final_list = []
        in_num = 0
        for item in word_list:
            if in_num == 0:
                final = item
            else:
                if len(final.split('|')[0]) < len(item.split('|')[0]):
                    final = item
            in_num = in_num + 1
        if final != '':
            final_list.append(final)
            havefind.append(final_list)
            num = num + len(final.split('|')[0])
        else:
            havefind.append(buf[num])
            num = num + 1
    return havefind

def parse_dict_all(input_str):
    if not isinstance(input_str, str):
        input_str = input_str.decode()
    buf = []
    words = []
    for word in input_str:
        if word >= '\u4e00' and word <= '\u9fa5':
            words = words+[word]
        elif word >= '\u0021' and word <= '\u007e':
            words = words+[word]
        elif word == '\n' or word == '\r':
            buf.append(words)
            words = []
        else:
            words = words+[word]
    return buf

#建树
def build_tree_all(input_str):
    sock = open(input_str,"r")
    buf = sock.read()
    buf = parse_dict_all(buf) # 保留有效字符
    tree = Item(" ")
    for words in buf:
        current = tree
        for word in words:
            for x in word:
                if current.subNode.contain(x):
                    current = current.subNode[x]
                else:
                    item = Item(x)
                    current.add(x,item)
                    current = item
        current.output = "".join(words)
    return tree

area_words = set()
area_words_norm = set()
area_tree_name_code = {}
area_tree_code_name = {}
word_tree = None
noise_word = ["市场监督管理所", "市场", "市场监管所"]
NO_CITY_TAG = "{}(?:路|家园|小区|中路|南路|东路|西路|北路|街|市场|街道|广场|学府|镇|乡)"
file_dir = os.path.dirname(__file__)

def load_lexicon():
    global area_words, area_tree_name_code, area_tree_code_name, word_tree
    if area_words and area_words_norm and area_tree_name_code and area_tree_code_name:
        return
    for line in open(os.path.join(file_dir, "administrative_division.txt"), encoding="utf-8"):
        line = line.strip('\n')
        areas = line.split('\t')
        if len(areas) != 6:
            raise Exception("fields length not equal to 6!")
        province_code, province_name, city_code, city_name, district_code, district_name = areas
        for code, name in [(province_code, province_name), (city_code, city_name), (district_code, district_name)]:
            code = code.strip()
            name = name.strip()
            if (not code) and not name:
                continue
            names = name.split(',')
            area_words_norm.add(names[0])
            area_words.update(set(names))
            if code not in area_tree_code_name:
                area_tree_code_name[code] = []
            for n in names:
                if n not in area_tree_name_code:
                    area_tree_name_code[n] = set()
                area_tree_name_code[n].add(code)
                if n not in area_tree_code_name[code]:
                    area_tree_code_name[code].append(n)
    word_tree = build_tree_from_list(list(area_words) + noise_word)

load_lexicon()


def words_2_area_info(words):
    area_info = {}
    find_province = ""
    find_city = ""
    for word in words:
        area_code = ""
        codes = area_tree_name_code.get(word)
        if not codes:
            continue
        if len(codes) == 1:
            area_code = list(codes)[0]
        else:
            for code in codes:
                if find_city and find_city == code[:4]:
                    area_code = code
                    break
                elif find_province and not find_city and find_province == code[:2]:
                    area_code = code
                    break
        if not area_code:
            break
        if area_code.endswith("0000"):
            if find_province and find_province != area_code[:2]:
                break
            find_province = area_code[:2]
            area_info["province_code"] = area_code
            area_info["province"] = area_tree_code_name[area_code][0]
        elif area_code.endswith("00"):
            if find_province and find_province != area_code[:2]:
                break
            elif find_city and find_city != area_code[:4]:
                break
            find_city = area_code[:4]
            find_province = area_code[:2]
            area_info["city_code"] = area_code
            area_info["city"] = area_tree_code_name[area_code][0]
        else:
            if find_province and find_province != area_code[:2]:
                break
            if find_city and find_city != area_code[:4]:
                break
            if area_info.get("district"):
                break
            area_info["district_code"] = area_code
            area_info["district"] = area_tree_code_name[area_code][0]
            find_city = area_code[:4]
            find_province = area_code[:2]
    if area_info.get("district_code") and not area_info.get("city_code"):
        district_code = area_info["district_code"]
        city_code = "{}00".format(district_code[:4])
        province_code = "{}0000".format(district_code[:2])
        if (not area_tree_code_name.get(city_code)) and not area_info.get("province_code"):
            area_info["province_code"] = province_code
            area_info["province"] = area_tree_code_name[province_code][0]
        if area_tree_code_name.get(city_code):
            area_info["city_code"] = city_code
            area_info["city"] = area_tree_code_name[city_code][0]
    if area_info.get("city_code") and not area_info.get("province_code"):
        city_code = area_info["city_code"]
        province_code = "{}0000".format(city_code[:2])
        area_info["province_code"] = province_code
        area_info["province"] = area_tree_code_name[province_code][0]
    return area_info

def get_location(word):
    search_words = search_detail(word, word_tree)
    new_address_list = []
    left_content = word
    for word in search_words:
        if word == "-":
            continue
        elif word in noise_word:
            continue
        elif not word.endswith(("省", "市", "区", "县")) and left_content[left_content.index(word)+len(word):].startswith(NO_CITY_TAG):
            continue
        idx = left_content.index(word)
        if idx != 0:
            new_address_list.append(left_content[:idx])
        new_address_list.append([word])
        left_content = left_content[idx + len(word):]
    if left_content:
        new_address_list.append(left_content)
    if not new_address_list:
        return {}
    # elif isinstance(new_address_list[0], str) and re.search("[\\u4e00-\\u9fa5]", new_address_list[0]):
    #     return {}
    else:
        words = [w[0] for w in new_address_list if isinstance(w, list)]
        if not words:
            return {}
        area_info = words_2_area_info(words)
        return area_info


def filter_gov_fields_value(doc):
    if doc['url'] and 'gov.cn' in urlparse(doc['url']).hostname:
        detail_url_gov = doc['url']
        if doc['attachment_info']:
            attachment_info = json.loads(doc['attachment_info'])
            if isinstance(attachment_info, dict):
                attachment_info = [attachment_info]
            attachment_info_gov = json.dumps(
                [{k: v for k, v in it.items() if k in ['filename', 'url']} for it in attachment_info],
                ensure_ascii=False)
        else:
            attachment_info_gov = None
    else:
        detail_url_gov = None
        attachment_info_gov = None
    return attachment_info_gov, detail_url_gov


def dumps_attachment_info(attachment_info: str):
    if not attachment_info:
        return None
    if isinstance(attachment_info, str):
        attachment_info = json.loads(attachment_info)
    if isinstance(attachment_info, dict):
        attachment_info = [attachment_info]
    return json.dumps(attachment_info, ensure_ascii=False)