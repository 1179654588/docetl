"""
创新型中小企业数据模型

定义三个核心数据表的结构：
1. InnovativeSMERawDataModel - 原始数据表
2. InnovativeSMEMappingRuleModel - 映射规则表
3. InnovativeSMEFinalModel - 最终数据表（可选）
"""

from typing import Any, Dict, Literal, Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field


class InnovativeSMERawDataModel(BaseModel):
    """创新型中小企业原始数据模型"""
    
    ods_id: Union[str, int]  # 公告id
    list_id: str  # 表格标识
    serial_no: Optional[Union[str, int]] = None  # 序号
    company_name: str = None  # 企业名称
    ext_json: Optional[Dict[str, Any]] = None  # 扩展字段
    company_name_digest: Optional[str] = None  # 企业digest
    company_id: Optional[str] = None  # 主体ID
    use_flag: Literal[0, 10] = 0  # 废弃标识（0有效数据，10废弃数据）
    verification_status: Literal[0, 1] = 0  # 核验状态（1核验完成，0未核验）
    verified_at: Optional[datetime] = None  # 核验时间
    verifier: Optional[str] = None  # 核验人
    modification_details: Optional[Dict[str, Any]] = None  # 核验修改详情


class InnovativeSMEMappingRuleModel(BaseModel):
    """创新型中小企业映射规则模型"""
    
    ods_id: Union[str, int]  # 公告id
    list_id: Optional[str] = None  # 名单id（ods_id+后缀）
    rating_scope: Optional[str] = None  # 评定范围（统计全国各省级单位该资质分布情况）
    notice_type: Optional[Literal[0, 1]] = None  # 公告类型（0公示，1通知）
    honor_name: Optional[str] = None  # 荣誉名称
    level: Optional[Literal[0, 1, 2]] = None  # 等级（0国家级，1省级，2市级）
    batch: Optional[str] = None  # 批次
    year: Optional[str] = None  # 认定报备年份
    start_date: Optional[datetime] = None  # 有效时间起（发证时间）
    end_date: Optional[datetime] = None  # 有效时间止
    status: Optional[Literal[0, 1, 2]] = None  # 当前状态（0有效，1过期失效，2撤销失效）
    title: Optional[str] = None  # 公告标题
    publish_unit: Optional[str] = None  # 发布单位
    publish_date: Optional[datetime] = None  # 发布时间
    detail_url: Optional[str] = None  # 详情链接
    attachment_info: Optional[Dict[str, Any]] = None  # 附件信息
    detail_url_gov: Optional[str] = None  # 官方详情页URL
    attachment_info_gov: Optional[Dict[str, Any]] = None  # 官方附件信息
    verification_status: Literal[0, 1] = 0  # 核验状态（1核验完成，0未核验）
    verifier: Optional[str] = None  # 核验人
    verified_at: Optional[datetime] = None  # 核验时间
    use_flag: Literal[0, 10] = 0  # 废弃标识（0有效数据，10废弃数据）
